package sstable

import (
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/nbroyles/nbdb/internal/memtable/interfaces"
	"github.com/nbroyles/nbdb/internal/storage"
	log "github.com/sirupsen/logrus"
)

// Builder is a structure that can take an iterator from a memtable data structure and use
// that to create an SSTable
type Builder struct {
	iter           interfaces.InternalIterator
	codec          *storage.Codec
	writer         io.Writer
	indexPerRecord int
}

const (
	// TODO: Make this configurable as option?
	indexCount = 1000
	pointerLen = 8 // bytes. Two uint32s
)

func NewBuilder(iter interfaces.InternalIterator, writer io.Writer) *Builder {
	return newBuilder(iter, writer, indexCount)
}

func newBuilder(iter interfaces.InternalIterator, writer io.Writer, indexPerRecord int) *Builder {
	return &Builder{iter: iter, codec: &storage.Codec{}, writer: writer, indexPerRecord: indexPerRecord}
}

// CreateFile creates a level0 sstable file that can be used by the Builder
func CreateFile(dbName string, dataDir string) *os.File {
	name := fmt.Sprintf("level0_%s_%d", dbName, time.Now().UnixNano()/1_000_000_000)

	tablePath := path.Join(dataDir, dbName, name)
	if _, err := os.Stat(tablePath); !os.IsNotExist(err) {
		if err != nil {
			log.Panicf("failure checking for level 0 sstable existence: %v", err)
		} else {
			log.Panicf("attempting to create new level 0 sstable %s but already exists", tablePath)
		}
	}

	file, err := os.Create(tablePath)
	if err != nil {
		log.Panicf("could not create level 0 sstable file: %v", err)
	}

	return file
}

// TODO: crashing while writing -- what to do?
// WriteLevel0Table writes data from memtable iterator to an sstable file.
func (s *Builder) WriteLevel0Table() {
	recWritten := 0
	bytesWritten := uint32(0)

	indices := make(map[string]storage.RecordPointer)
	var order []string

	// Write actual key-values to disk
	for ; s.iter.HasNext(); recWritten++ {
		rec := s.iter.Next()
		bytes, err := s.codec.Encode(rec)
		if err != nil {
			log.Panicf("could not encode record: %v", err)
		}

		s.write(bytes)

		// Create index entry if reached threshold for number of written records
		if recWritten%s.indexPerRecord == 0 {
			indices[string(rec.Key)] = storage.RecordPointer{Key: rec.Key, StartByte: bytesWritten, Length: uint32(len(bytes))}
			order = append(order, string(rec.Key))
		}

		bytesWritten += uint32(len(bytes))
	}

	indexStart := bytesWritten
	firstLen := 0
	// Write index blocks in correct order
	for _, key := range order {
		ptr := indices[key]
		bytes, err := s.codec.EncodePointer(&ptr)
		if err != nil {
			log.Panicf("could not encode index pointer record: %v", err)
		}

		s.write(bytes)

		// Keep length of first index block written for use in footer pointer
		if firstLen == 0 {
			firstLen += len(bytes)
		}
	}

	// Write footer
	bytes, err := s.codec.EncodeFooter(&storage.Footer{
		IndexStartByte: indexStart,
		Length:         uint32(firstLen),
	})
	if err != nil {
		log.Panicf("could not encode footer pointer record: %v", err)
	}
	s.write(bytes)

	// TODO: write to manifest
}

func (s *Builder) write(bytes []byte) {
	if n, err := s.writer.Write(bytes); n != len(bytes) {
		log.Panicf("failed to write all bytes to disk. n=%d, expected=%d", n, len(bytes))
	} else if err != nil {
		log.Panicf("failure writing level 0 sstable: %v", err)
	}
}
