package manifest

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Codec struct{}

func (c *Codec) EncodeEntry(entry *Entry) ([]byte, error) {
	buf := bytes.Buffer{}

	// TODO: update this codec when sstable.Metadata is ready
	// For right now, just deleted byte
	totalLen := 1
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

	var deleted bool
	if err := binary.Read(reader, binary.BigEndian, &deleted); err != nil {
		return nil, fmt.Errorf("failed to decode deletion status of entry: %w", err)
	}

	return &Entry{
		deleted: deleted,
	}, nil
}
