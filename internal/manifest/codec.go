package manifest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/nbroyles/nbdb/internal/sstable"
)

type Codec struct{}

func (c *Codec) EncodeEntry(entry *Entry) ([]byte, error) {
	buf := bytes.Buffer{}

	// 1 deleted byte + 1 level byte + 1 byte for filename length + len(filename) bytes
	totalLen := 3 + len(entry.metadata.Filename)
	if err := binary.Write(&buf, binary.BigEndian, uint32(totalLen)); err != nil {
		return nil, fmt.Errorf("failed to encode total entry length: %w", err)
	}

	if err := binary.Write(&buf, binary.BigEndian, entry.metadata.Level); err != nil {
		return nil, fmt.Errorf("failed to encode level for entry: %w", err)
	}

	fileNameLen := len(entry.metadata.Filename)
	if err := binary.Write(&buf, binary.BigEndian, uint8(fileNameLen)); err != nil {
		return nil, fmt.Errorf("failed to encode filename length for entry: %w", err)
	}

	if n, err := buf.Write([]byte(entry.metadata.Filename)); n != fileNameLen {
		return nil, fmt.Errorf("failed to write full filename to buffer. wrote=%d, len=%d", n, fileNameLen)
	} else if err != nil {
		return nil, fmt.Errorf("failed to encode filename for entry: %w", err)
	}

	if err := binary.Write(&buf, binary.BigEndian, entry.deleted); err != nil {
		return nil, fmt.Errorf("failed to encode deleted status for entry: %w", err)
	}

	return buf.Bytes(), nil
}

func (c *Codec) DecodeEntry(data []byte) (*Entry, error) {
	reader := bytes.NewReader(data)

	var level uint8
	if err := binary.Read(reader, binary.BigEndian, &level); err != nil {
		return nil, fmt.Errorf("failed to decode level of entry: %w", err)
	}

	var fileNameLen uint8
	if err := binary.Read(reader, binary.BigEndian, &fileNameLen); err != nil {
		return nil, fmt.Errorf("failed to decode filename length of entry: %w", err)
	}

	fileName := make([]byte, fileNameLen)
	if _, err := io.ReadFull(reader, fileName); err != nil {
		return nil, fmt.Errorf("failed to read filename: %w", err)
	}

	var deleted bool
	if err := binary.Read(reader, binary.BigEndian, &deleted); err != nil {
		return nil, fmt.Errorf("failed to decode deletion status of entry: %w", err)
	}

	return &Entry{
		metadata: &sstable.Metadata{
			Level:    level,
			Filename: string(fileName),
		},
		deleted: deleted,
	}, nil
}
