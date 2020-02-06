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
	// + 4 bytes for start key len + len(start_key) bytes
	// + 4 bytes for end key len + len(end_key) bytes
	totalLen := 3 + len(entry.metadata.Filename) + 4 + len(entry.metadata.StartKey) + 4 + len(entry.metadata.EndKey)
	if err := binary.Write(&buf, binary.BigEndian, uint32(totalLen)); err != nil {
		return nil, fmt.Errorf("failed to encode total entry length: %w", err)
	}

	if err := binary.Write(&buf, binary.BigEndian, entry.metadata.Level); err != nil {
		return nil, fmt.Errorf("failed to encode level for entry: %w", err)
	}

	if err := encodeVarLengthField(&buf, []byte(entry.metadata.Filename), 1); err != nil {
		return nil, fmt.Errorf("failed to encode filename for entry: %w", err)
	}

	if err := encodeVarLengthField(&buf, entry.metadata.StartKey, 4); err != nil {
		return nil, fmt.Errorf("failed to encode startKey for entry: %w", err)
	}

	if err := encodeVarLengthField(&buf, entry.metadata.EndKey, 4); err != nil {
		return nil, fmt.Errorf("failed to encode endKey for entry: %w", err)
	}

	if err := binary.Write(&buf, binary.BigEndian, entry.deleted); err != nil {
		return nil, fmt.Errorf("failed to encode deleted status for entry: %w", err)
	}

	return buf.Bytes(), nil
}

func encodeVarLengthField(buf io.Writer, data []byte, lenBytes int) error {
	var readLen int
	// TODO: there has to be a better way
	if lenBytes == 1 {
		dataLen := uint8(len(data))
		if err := binary.Write(buf, binary.BigEndian, dataLen); err != nil {
			return fmt.Errorf("failed to encode field length for entry: %w", err)
		}
		readLen = int(dataLen)
	} else {
		dataLen := uint32(len(data))
		if err := binary.Write(buf, binary.BigEndian, dataLen); err != nil {
			return fmt.Errorf("failed to encode field length for entry: %w", err)
		}
		readLen = int(dataLen)
	}

	if n, err := buf.Write(data); n != readLen {
		return fmt.Errorf("failed to write full field to buffer. wrote=%d, len=%d", n, readLen)
	} else if err != nil {
		return fmt.Errorf("failed to encode field for entry: %w", err)
	}

	return nil
}

// TODO: convert to using a io.reader like other Decode methods?
func (c *Codec) DecodeEntry(data []byte) (*Entry, error) {
	reader := bytes.NewReader(data)

	var level uint8
	if err := binary.Read(reader, binary.BigEndian, &level); err != nil {
		return nil, fmt.Errorf("failed to decode level of entry: %w", err)
	}

	fileName, err := decodeVarLengthField(reader, 1)
	if err != nil {
		return nil, fmt.Errorf("failed decoding filename field: %w", err)
	}

	startKey, err := decodeVarLengthField(reader, 4)
	if err != nil {
		return nil, fmt.Errorf("failed decoding startKey field: %w", err)
	}

	endKey, err := decodeVarLengthField(reader, 4)
	if err != nil {
		return nil, fmt.Errorf("failed decoding endKey field: %w", err)
	}

	var deleted bool
	if err := binary.Read(reader, binary.BigEndian, &deleted); err != nil {
		return nil, fmt.Errorf("failed to decode deletion status of entry: %w", err)
	}

	return &Entry{
		metadata: &sstable.Metadata{
			Level:    level,
			Filename: string(fileName),
			StartKey: startKey,
			EndKey:   endKey,
		},
		deleted: deleted,
	}, nil
}

func decodeVarLengthField(reader *bytes.Reader, lenBytes int) ([]byte, error) {
	var readLen int
	// TODO: there has to be a better way x 2
	if lenBytes == 1 {
		var dataLen uint8
		if err := binary.Read(reader, binary.BigEndian, &dataLen); err != nil {
			return nil, fmt.Errorf("failed to decode field length of entry: %w", err)
		}
		readLen = int(dataLen)
	} else {
		var dataLen uint32
		if err := binary.Read(reader, binary.BigEndian, &dataLen); err != nil {
			return nil, fmt.Errorf("failed to decode field length of entry: %w", err)
		}
		readLen = int(dataLen)
	}

	data := make([]byte, readLen)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, fmt.Errorf("failed to read field: %w", err)
	}

	return data, nil
}
