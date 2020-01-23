package manifest

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/nbroyles/nbdb/internal/sstable"
)

type Codec struct{}

func (c *Codec) EncodeEntry(entry *Entry) ([]byte, error) {
	buf := bytes.Buffer{}

	// TODO: update this codec when sstable.Metadata is ready
	totalLen := 5
	if err := binary.Write(&buf, binary.BigEndian, uint32(totalLen)); err != nil {
		return nil, fmt.Errorf("failed to encode total entry length: %w", err)
	}

	if err := binary.Write(&buf, binary.BigEndian, entry.deleted); err != nil {
		return nil, fmt.Errorf("failed to encode index start byte for footer: %w", err)
	}

	return buf.Bytes(), nil
}

func (c *Codec) DecodeEntry(data []byte) (*Entry, error) {
	reader := bytes.NewReader(data)

	var totalLen uint32
	if err := binary.Read(reader, binary.BigEndian, &totalLen); err != nil {
		return nil, fmt.Errorf("failed to read entry length: %w", err)
	}

	var deleted bool
	if err := binary.Read(reader, binary.BigEndian, &deleted); err != nil {
		return nil, fmt.Errorf("failed to decode deletion status of entry: %w", err)
	}

	return &Entry{
		metadata: &sstable.Metadata{},
		deleted:  deleted,
	}, nil
}
