package sstable

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/nbroyles/nbdb/internal/storage"
	log "github.com/sirupsen/logrus"
)

type Merger struct {
	level          int
	nextLevel      int
	srcMetadata    []*Metadata
	dataDir        string
	dbName         string
	codec          storage.Codec
	done           bool
	mergedMetadata []*Metadata
}

const (
	maxFileSize = 2_000_000
)

// Merger expects to receive srcMetadata in order of most recently created to least recently created in order
// to ensure duplicate updates are properly handled
func NewMerger(level int, nextLevel int, srcMetadata []*Metadata, dataDir string, dbName string) *Merger {
	return &Merger{
		level:          level,
		nextLevel:      nextLevel,
		srcMetadata:    srcMetadata,
		dataDir:        dataDir,
		dbName:         dbName,
		codec:          storage.Codec{},
		done:           false,
		mergedMetadata: nil,
	}
}

func (m *Merger) Merge() ([]*Metadata, error) {
	if m.done {
		log.Infof("already ran merger. skipping. level=%d, nextLevel=%d", m.level, m.nextLevel)
		return m.mergedMetadata, nil
	}

	// open all files for reading
	var files []io.ReadSeeker
	for _, me := range m.srcMetadata {
		handle, err := os.Open(path.Join(m.dataDir, m.dbName, me.Filename))
		if err != nil {
			return nil, fmt.Errorf("could not open file %s for compaction: %w", me.Filename, err)
		}

		files = append(files, handle)
	}

	// pointers to current key in each file
	current := make([]*storage.Record, len(files))
	stopByte := make([]uint32, len(files))

	for i, handle := range files {
		_, err := handle.Seek(-footerLen, io.SeekEnd)
		if err != nil {
			return nil, fmt.Errorf("could not seek to footer in sstable: %w", err)
		}

		footer, err := m.codec.DecodeFooter(handle)
		if err != nil {
			return nil, fmt.Errorf("failed to decode footer from sstable. %w", err)
		}

		stopByte[i] = footer.IndexStartByte

		_, err = handle.Seek(0, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("could not seek to beginning in sstable: %w", err)
		}

		current[i], err = m.readNext(handle, stopByte[i])
		if err != nil {
			return nil, fmt.Errorf("failed attempting to read next record in sstable: %w", err)
		}
	}

	// Merge into files at the new level until data exhausted
	for {
		meta, finished, err := m.mergeToFile(files, current, stopByte)
		if err != nil {
			return nil, fmt.Errorf("failed attempting to merge files: %v %w", m, err)
		}

		log.Debugf("results of mergeToFile: meta=%v meta.startKey=%s meta.endKey=%s finished=%v",
			meta, string(meta.StartKey), string(meta.EndKey), finished)

		m.mergedMetadata = append(m.mergedMetadata, meta)

		if finished {
			break
		}
	}

	m.done = true

	return m.mergedMetadata, nil
}

// mergeToFile takes the source data and merges as much data as it can until it's either exhausted the source
// material or hit a limit on output size. Return values are the metadata for the file created, a boolean indicating if
// there's more merge work to be done, and an error value. Method should be called until boolean indicating more work is false
func (m *Merger) mergeToFile(files []io.ReadSeeker, current []*storage.Record, stopByte []uint32) (*Metadata, bool, error) {
	out, err := CreateFile(m.dbName, m.dataDir)
	if err != nil {
		return nil, false, fmt.Errorf("failed attempt to create new sstable file: %w", err)
	}
	defer out.Close()

	bytesWritten := 0
	recWritten := 0

	indices := make(map[string]*storage.RecordPointer)
	var order []string

	var startKey []byte
	var endKey []byte
	var prevKey []byte
	for {
		// select the next key to right from current head of each sstable
		var currRecord *storage.Record
		var currIdx int
		for i, rec := range current {
			// skip this key because we've accounted for a newer version of it
			for ; rec != nil && bytes.Equal(rec.Key, prevKey); rec = current[i] {
				log.Debugf("skipping key=%s since newer update found", string(rec.Key))
				current[i], err = m.readNext(files[i], stopByte[i])
				if err != nil {
					return nil, false, fmt.Errorf("failed attempting to read next record in sstable: %w", err)
				}
			}

			if rec == nil {
				continue
			} else if currRecord == nil {
				currRecord = rec
				currIdx = i
			} else if bytes.Compare(rec.Key, currRecord.Key) < 0 {
				currRecord = rec
				currIdx = i
			}
		}

		if shouldStop(current) {
			if recWritten > 0 {
				if err = m.writeFooter(out, bytesWritten, indices, order); err != nil {
					return nil, false, fmt.Errorf("failed attempting to write footer information for sstable: %w", err)
				}
			}
			break
		}

		log.Debugf("next record to be written from current[%d]: key=%s value=%s", currIdx, string(currRecord.Key),
			string(currRecord.Value))

		// We should always have a key if we're not done, so panic
		if currRecord == nil {
			log.Panicf("next record to write is nil when it shouldn't be: current=%v", current)
		}

		if startKey == nil {
			startKey = currRecord.Key
		}

		// Write out current next value to be written
		data, err := m.codec.Encode(currRecord)
		if err != nil {
			return nil, false, fmt.Errorf("could not encode record: %w", err)
		}

		if err := write(out, data); err != nil {
			return nil, false, fmt.Errorf("failure writing next entry into sstable: %w", err)
		}

		// Create index entry if reached threshold for number of written records
		if recWritten%indexCount == 0 {
			indices[string(currRecord.Key)] = &storage.RecordPointer{
				Key:       currRecord.Key,
				StartByte: uint32(bytesWritten),
				Length:    uint32(len(data)),
			}
			order = append(order, string(currRecord.Key))
		}

		bytesWritten += len(data)
		recWritten++

		prevKey = currRecord.Key
		endKey = currRecord.Key

		// Advance to the next record
		current[currIdx], err = m.readNext(files[currIdx], stopByte[currIdx])

		if current[currIdx] == nil {
			log.Debugf("updating current pointers. current[%d]=nil", currIdx)
		} else {
			log.Debugf("updating current pointers. current[%d]=%s", currIdx, string(current[currIdx].Key))
		}

		if err != nil {
			return nil, false, fmt.Errorf("failed attempting to read next record in sstable: %w", err)
		}

		// This file has reached its max size. Let's write out the index and footer information and create a new one
		if bytesWritten > maxFileSize {
			if err = m.writeFooter(out, bytesWritten, indices, order); err != nil {
				return nil, false, fmt.Errorf("failed attempting to write footer information for sstable: %w", err)
			}
			break
		}
	}

	newMeta := Metadata{
		Level:    uint8(m.nextLevel),
		Filename: filepath.Base(out.Name()),
		StartKey: startKey,
		EndKey:   endKey,
	}

	return &newMeta, shouldStop(current), nil
}

func (m *Merger) writeFooter(out io.Writer, bytesWritten int, indices map[string]*storage.RecordPointer, order []string) error {
	indexStart := bytesWritten
	firstLen := 0
	// Write index blocks in correct order
	for _, key := range order {
		ptr := indices[key]
		data, err := m.codec.EncodePointer(ptr)
		if err != nil {
			return fmt.Errorf("could not encode index pointer record: %w", err)
		}

		if n, err := out.Write(data); n != len(data) {
			return fmt.Errorf("failed to write all bytes to disk. n=%d, expected=%d", n, len(data))
		} else if err != nil {
			return fmt.Errorf("failure writing sstable: %w", err)
		}

		// Keep length of first index block written for use in footer pointer
		if firstLen == 0 {
			firstLen += len(data)
		}
	}

	// Write footer
	data, err := m.codec.EncodeFooter(&storage.Footer{
		IndexStartByte: uint32(indexStart),
		Length:         uint32(firstLen),
		IndexEntries:   uint32(len(indices)),
	})
	if err != nil {
		return fmt.Errorf("could not encode footer pointer record: %w", err)
	}

	if err = write(out, data); err != nil {
		return fmt.Errorf("failed attempting to write to sstable: %w", err)
	}

	return nil
}

// readNext reads next entry from the handle provided. Returns the record for the entry or nil if no next record
func (m *Merger) readNext(handle io.ReadSeeker, stopByte uint32) (*storage.Record, error) {
	curPos, err := handle.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failure getting current position in file: %w", err)
	} else if curPos == int64(stopByte) {
		return nil, nil
	}

	record, err := m.codec.DecodeFromReader(handle)
	if err != nil {
		return nil, fmt.Errorf("could not decode record in sstable for merge process: %w", err)
	}

	return record, nil
}

func shouldStop(current []*storage.Record) bool {
	for _, val := range current {
		if val != nil {
			return false
		}
	}
	return true
}
