package sstable

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/nbroyles/nbdb/internal/memtable/interfaces"
	"github.com/nbroyles/nbdb/internal/storage"
	"github.com/nbroyles/nbdb/internal/util"
)

// Builder is a structure that can take an iterator from a memtable data structure and use
// that to create an SSTable
type Builder struct {
	name           string
	iter           interfaces.InternalIterator
	codec          *storage.Codec
	writer         io.Writer
	indexPerRecord int
}

const (
	// TODO: Make this configurable as option?
	indexCount = 1000
	pointerLen = 8 // bytes. Two uint32s
	sstPrefix  = "sstable"
)

func CreateFile(dbName string, dataDir string) (*os.File, error) {
	return util.CreateFile(fmt.Sprintf("%s_%s_%d", sstPrefix, dbName, time.Now().UnixNano()/1_000_000_000),
		dbName, dataDir)
}

func NewBuilder(name string, iter interfaces.InternalIterator, writer io.Writer) *Builder {
	return newBuilder(name, iter, writer, indexCount)
}

func newBuilder(name string, iter interfaces.InternalIterator, writer io.Writer, indexPerRecord int) *Builder {
	return &Builder{name: name, iter: iter, codec: &storage.Codec{}, writer: writer, indexPerRecord: indexPerRecord}
}

// TODO: crashing while writing -- what to do?
// WriteLevel0Table writes data from memtable iterator to an sstable file.
func (s *Builder) WriteLevel0Table() (*Metadata, error) {
	recWritten := 0
	bytesWritten := uint32(0)

	indices := make(map[string]storage.RecordPointer)
	var order []string

	// Write actual key-values to disk
	for ; s.iter.HasNext(); recWritten++ {
		rec := s.iter.Next()
		bytes, err := s.codec.Encode(rec)
		if err != nil {
			return nil, fmt.Errorf("could not encode record: %w", err)
		}

		if err = s.write(bytes); err != nil {
			return nil, fmt.Errorf("failed attempting to write to level 0 sstable: %w", err)
		}

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
			return nil, fmt.Errorf("could not encode index pointer record: %w", err)
		}

		if err = s.write(bytes); err != nil {
			return nil, fmt.Errorf("failed attempting to write to level 0 sstable: %w", err)
		}

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
		return nil, fmt.Errorf("could not encode footer pointer record: %w", err)
	}

	if err = s.write(bytes); err != nil {
		return nil, fmt.Errorf("failed attempting to write to level 0 sstable: %w", err)
	}

	return &Metadata{
		Level:    0,
		Filename: s.name,
	}, nil
}

func (s *Builder) write(bytes []byte) error {
	if n, err := s.writer.Write(bytes); n != len(bytes) {
		return fmt.Errorf("failed to write all bytes to disk. n=%d, expected=%d", n, len(bytes))
	} else if err != nil {
		return fmt.Errorf("failure writing level 0 sstable: %w", err)
	}

	return nil
}
