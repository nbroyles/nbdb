package manifest

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/nbroyles/nbdb/internal/sstable"
	"github.com/nbroyles/nbdb/internal/util"
	log "github.com/sirupsen/logrus"
)

type Manifest struct {
	entries []*Entry
	levels  map[int][]*sstable.Metadata
	writer  io.Writer
	codec   Codec
}

type Entry struct {
	metadata *sstable.Metadata
	deleted  bool
}

const (
	manifestPrefix = "manifest"
	uint32size     = 4
)

func NewManifest(writer io.Writer) *Manifest {
	return &Manifest{writer: writer, levels: make(map[int][]*sstable.Metadata)}
}

func NewEntry(metadata *sstable.Metadata, deleted bool) *Entry {
	return &Entry{metadata: metadata, deleted: deleted}
}

func CreateManifestFile(dbName string, dataDir string) (*os.File, error) {
	return util.CreateFile(fmt.Sprintf("%s_%s_%d", manifestPrefix, dbName, time.Now().UnixNano()/1_000_000_000),
		dbName, dataDir)
}

func LoadLatest(dbName string, dataDir string) (bool, *Manifest, error) {
	search := path.Join(dataDir, dbName, fmt.Sprintf("%s_%s_*", manifestPrefix, dbName))
	matches, err := filepath.Glob(search)
	if err != nil {
		return false, nil, fmt.Errorf("error loading manifest file: %w", err)
	} else if len(matches) == 0 {
		return false, nil, nil
	}

	sort.Strings(matches)
	latest := matches[len(matches)-1]

	file, err := os.Open(latest)
	if err != nil {
		return false, nil, fmt.Errorf("could not open latest manifest file: %w", err)
	}

	m := NewManifest(file)

	for {
		data := make([]byte, uint32size)
		if n, err := file.Read(data); err == io.EOF {
			break
		} else if n != len(data) {
			return false, nil, fmt.Errorf("failed to read expected amount of data from manifest."+
				" read=%d, expected=%d", n, len(data))
		} else if err != nil {
			return false, nil, fmt.Errorf("failed to read record: %w", err)
		}

		eLen := binary.BigEndian.Uint32(data)

		entryBytes := make([]byte, eLen)
		if n, err := file.Read(entryBytes); uint32(n) != eLen {
			return false, nil, fmt.Errorf("failed to read expected amount of entry data from manifest."+
				" read=%d, expected=%d", n, eLen)
		} else if err != nil {
			return false, nil, fmt.Errorf("failed to read record: %w", err)
		}

		entry, err := m.codec.DecodeEntry(entryBytes)
		if err != nil {
			return false, nil, fmt.Errorf("failure decoding manifest entry: %w", err)
		}

		m.addToLevel(entry)
	}

	return true, m, nil
}

func (m *Manifest) AddEntry(entry *Entry) error {
	bytes, err := m.codec.EncodeEntry(entry)
	if err != nil {
		return fmt.Errorf("failed encoding manifest entry %v: %w", entry, err)
	}

	if written, err := m.writer.Write(bytes); written < len(bytes) {
		// TODO: Manifest state corrupted at this point?
		return fmt.Errorf("failed writing to manifest. wrote %d bytes, expected %d bytes", written, len(bytes))
	} else if err != nil {
		return fmt.Errorf("failed writing to manifest: %w", err)
	}

	m.addToLevel(entry)

	return nil
}

// MetadataForLevel returns metadata for all active sstables at the specified level
func (m *Manifest) MetadataForLevel(level int) []*sstable.Metadata {
	return m.levels[level]
}

func (m *Manifest) Levels() int {
	return len(m.levels)
}

func (m *Manifest) addToLevel(entry *Entry) {
	m.entries = append(m.entries, entry)
	if !entry.deleted {
		m.levels[int(entry.metadata.Level)] = append(m.levels[int(entry.metadata.Level)], entry.metadata)
	} else {
		// Find entry and remove from in memory structure tracking metadata for each level
		entries := m.levels[int(entry.metadata.Level)]
		loc := -1
		for idx, m := range entries {
			if m.Filename == entry.metadata.Filename {
				loc = idx
				break
			}
		}
		if loc == -1 {
			log.Panicf("missing metadata entry %v in manifest", entry.metadata)
		}
		m.levels[int(entry.metadata.Level)] = append(entries[:loc], entries[loc+1:]...)
	}
}
