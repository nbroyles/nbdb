package compaction

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path"

	"github.com/nbroyles/nbdb/internal/manifest"
	"github.com/nbroyles/nbdb/internal/sstable"
	"github.com/nbroyles/nbdb/internal/storage"
	log "github.com/sirupsen/logrus"
)

// TODO: make this an interface or add the ability to provide compaction strategies to enable
// different compaction behavior
type Compactor struct {
	manifest *manifest.Manifest
	dataDir  string
	dbName   string
	codec    *storage.Codec
}

func New(manifest *manifest.Manifest, dataDir string, dbName string) *Compactor {
	return &Compactor{manifest: manifest, dataDir: dataDir, dbName: dbName, codec: &storage.Codec{}}
}

func (c *Compactor) Compact() error {
	// Look at other levels to see if files need to be merged
	for i := 0; i < c.manifest.Levels(); i++ {
		if compact, err := c.shouldCompact(i); err != nil {
			return fmt.Errorf("could not determine if should compact: %w", err)
		} else if compact {
			ssts := c.identifyMergeCandidates(i)
			// TODO: need to provide these metadata to the merger in the correct order of most recent to least recent
			newSsts, err := c.merge(i, ssts)
			if err != nil {
				return fmt.Errorf("failed performing compaction for %v: %w", ssts, err)
			}

			if err = c.updateManifest(ssts, newSsts); err != nil {
				return fmt.Errorf("failed to update manifest with new sstables: %w", err)
			}
		}
	}

	return nil
}

func (c *Compactor) shouldCompact(level int) (bool, error) {
	if level == 0 {
		return len(c.manifest.MetadataForLevel(level)) >= 4, nil
	}

	above, err := c.aboveCompactionThreshold(level)
	if err != nil {
		return false, fmt.Errorf("failed checking if level %d is above compaction threshold: %w", level, err)
	}

	return above, nil
}

func (c *Compactor) aboveCompactionThreshold(level int) (bool, error) {
	lvlSz := int64(0)
	for _, meta := range c.manifest.MetadataForLevel(level) {
		info, err := os.Stat(path.Join(c.dataDir, c.dbName, meta.Filename))
		if err != nil {
			return false, fmt.Errorf("failed calculating level %d size: %w", level, err)
		}

		lvlSz += info.Size()
	}

	// level size > 10^L * 1 MB
	return lvlSz > int64(math.Pow(10, float64(level)))*1_000_000, nil
}

func (c *Compactor) identifyMergeCandidates(level int) []*sstable.Metadata {
	var candidates []*sstable.Metadata
	lvlMeta := c.manifest.MetadataForLevel(level)
	if len(lvlMeta) == 0 {
		log.Panicf("should not have no metadata for level (%d) we're attempting compaction on", level)
	}

	if level == 0 {
		candidates = append(candidates, lvlMeta...)
	} else {
		candidates = append(candidates, lvlMeta[0])
	}

	// Find min key and max key from all potential candidates
	// This allows us to find all files in the next level that overlap
	var startKey []byte
	var endKey []byte
	for _, c := range candidates {
		if startKey == nil {
			startKey = c.StartKey
			endKey = c.EndKey
			continue
		}

		if bytes.Compare(c.StartKey, startKey) < 0 {
			startKey = c.StartKey
		}

		if bytes.Compare(c.EndKey, endKey) > 0 {
			endKey = c.EndKey
		}
	}

	totalRange := sstable.Metadata{
		StartKey: startKey,
		EndKey:   endKey,
	}
	for _, m := range c.manifest.MetadataForLevel(level + 1) {
		if totalRange.ContainsKey(m.StartKey) || totalRange.ContainsKey(m.EndKey) {
			candidates = append(candidates, m)
		}
	}

	return candidates
}

func (c *Compactor) merge(level int, meta []*sstable.Metadata) ([]*sstable.Metadata, error) {
	return sstable.NewMerger(level, level+1, meta, c.dataDir, c.dbName).Merge()
}

func (c *Compactor) updateManifest(oldSsts []*sstable.Metadata, newSsts []*sstable.Metadata) error {
	for _, m := range oldSsts {
		err := c.manifest.AddEntry(manifest.NewEntry(m, true))
		if err != nil {
			return fmt.Errorf("failed marking merged sstables as deleted in manifest: %w", err)
		}
	}

	for _, m := range newSsts {
		err := c.manifest.AddEntry(manifest.NewEntry(m, false))
		if err != nil {
			return fmt.Errorf("failed adding new sstables to manifest: %w", err)
		}
	}

	return nil
}
