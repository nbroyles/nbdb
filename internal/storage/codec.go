package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	log "github.com/sirupsen/logrus"
)

// Responsible for encoding and decoding data sent to and retrieved
// from disk
type Codec struct{}

// Encoding record format:
// - total record length
// - record type (put, delete)
// - key length (uint32 == 4 bytes)
// - key
// { if update }
//   - val length (uint32 == 4 bytes)
//	 - val
// { /if }
// - checksum

// Encodes provided key, value and record type and returns a byte array
// ready to be written to the WAL
func (c *Codec) Encode(record *Record) ([]byte, error) {
	key := record.Key
	value := record.Value

	// record type byte + key length bytes + variable key bytes + checksum bytes
	// + (conditionally) value length bytes + (conditionally) variable value bytes
	totalLength := 1 + 4 + crc32.Size + len(key)
	if record.Type == RecordUpdate {
		totalLength += 4 + len(value)
	}

	buf := bytes.Buffer{}
	if err := binary.Write(&buf, binary.BigEndian, uint32(totalLength)); err != nil {
		return nil, fmt.Errorf("failed to encode total record length: %w", err)
	}

	if err := binary.Write(&buf, binary.BigEndian, uint32(len(key))); err != nil {
		return nil, fmt.Errorf("failed to encode key length: %w", err)
	}

	if n, err := buf.Write(key); n != len(key) {
		return nil, fmt.Errorf("failed to write full key to buffer. wrote=%d, len=%d", n, len(key))
	} else if err != nil {
		return nil, fmt.Errorf("failed to encode key: %w", err)
	}

	if err := binary.Write(&buf, binary.BigEndian, int8(record.Type)); err != nil {
		return nil, fmt.Errorf("failed to encode record type: %w", err)
	}

	if record.Type == RecordUpdate {
		if err := binary.Write(&buf, binary.BigEndian, int32(len(value))); err != nil {
			return nil, fmt.Errorf("failed to encode value length: %w", err)
		}

		if n, err := buf.Write(value); n != len(value) {
			return nil, fmt.Errorf("failed to write full value to buffer. wrote=%d, len=%d", n, len(value))
		} else if err != nil {
			return nil, fmt.Errorf("failed to encode value: %w", err)
		}
	}

	checksumData := buf.Bytes()[4:] // Ignore initial 4 bytes containing totalLen
	if err := binary.Write(&buf, binary.BigEndian, crc32.ChecksumIEEE(checksumData)); err != nil {
		return nil, fmt.Errorf("failed to encode checksum: %w", err)
	}

	return buf.Bytes(), nil
}

// Decode takes a record from the WAL and decodes it into a key, value
// and record type
func (c *Codec) Decode(record []byte) (*Record, error) {
	reader := bytes.NewReader(record)

	var totalLen uint32
	if err := binary.Read(reader, binary.BigEndian, &totalLen); err != nil {
		return nil, fmt.Errorf("failed to read record length: %w", err)
	}

	data := make([]byte, totalLen)
	if n, err := io.ReadFull(reader, data); n != len(data) {
		return nil, fmt.Errorf("failed to read expected amount of data from log."+
			" read=%d, expected=%d", n, len(data))
	} else if err != nil {
		return nil, fmt.Errorf("failed to read record: %w", err)
	}

	actualRecord := data[0:(totalLen - 4)] // minus checksum len
	expectedChecksum := binary.BigEndian.Uint32(data[(totalLen - 4):])

	actualChecksum := crc32.ChecksumIEEE(actualRecord)
	if actualChecksum != expectedChecksum {
		log.Panicf("expected checksum of WAL record does not match! expected=%d, "+
			"actual=%d", expectedChecksum, actualChecksum)
	}

	dataReader := bytes.NewReader(actualRecord)

	var keyLen uint32
	if err := binary.Read(dataReader, binary.BigEndian, &keyLen); err != nil {
		return nil, fmt.Errorf("failed to read key length: %w", err)
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(dataReader, key); err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	var rawType uint8
	if err := binary.Read(dataReader, binary.BigEndian, &rawType); err != nil {
		return nil, fmt.Errorf("failed to read record type: %w", err)
	}
	rType := RecordType(rawType)

	var value []byte
	if rType == RecordUpdate {
		var valueLen uint32
		if err := binary.Read(dataReader, binary.BigEndian, &valueLen); err != nil {
			return nil, fmt.Errorf("failed to read value length: %w", err)
		}

		value = make([]byte, valueLen)
		if _, err := io.ReadFull(dataReader, value); err != nil {
			return nil, fmt.Errorf("failed to read value: %w", err)
		}
	}

	return &Record{
		Key:   key,
		Value: value,
		Type:  rType,
	}, nil
}

func (c *Codec) EncodePointer(pointer *RecordPointer) ([]byte, error) {
	buf := bytes.Buffer{}

	if err := binary.Write(&buf, binary.BigEndian, uint32(len(pointer.Key))); err != nil {
		return nil, fmt.Errorf("failed to encode key length: %w", err)
	}

	if n, err := buf.Write(pointer.Key); n != len(pointer.Key) {
		return nil, fmt.Errorf("failed to write full key to buffer. wrote=%d, len=%d", n, len(pointer.Key))
	} else if err != nil {
		return nil, fmt.Errorf("failed to encode key: %w", err)
	}

	if err := binary.Write(&buf, binary.BigEndian, pointer.StartByte); err != nil {
		return nil, fmt.Errorf("failed to encode start byte of pointer: %w", err)
	}

	if err := binary.Write(&buf, binary.BigEndian, pointer.Length); err != nil {
		return nil, fmt.Errorf("failed to encode lenght of record being pointer to: %w", err)
	}

	return buf.Bytes(), nil
}

func (c *Codec) DecodePointer(data []byte) (*RecordPointer, error) {
	reader := bytes.NewReader(data)

	var keyLen uint32
	if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
		return nil, fmt.Errorf("failed to read key length: %w", err)
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	var startByte uint32
	if err := binary.Read(reader, binary.BigEndian, &startByte); err != nil {
		return nil, fmt.Errorf("failed to decode start byte for pointer record: %w", err)
	}

	var length uint32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("failed to decode length for pointer record: %w", err)
	}

	return &RecordPointer{
		Key:       key,
		StartByte: startByte,
		Length:    length,
	}, nil
}

func (c *Codec) EncodeFooter(footer *Footer) ([]byte, error) {
	buf := bytes.Buffer{}
	if err := binary.Write(&buf, binary.BigEndian, footer.IndexStartByte); err != nil {
		return nil, fmt.Errorf("failed to encode index start byte for footer: %w", err)
	}

	if err := binary.Write(&buf, binary.BigEndian, footer.Length); err != nil {
		return nil, fmt.Errorf("failed to encode length for footer: %w", err)
	}

	return buf.Bytes(), nil
}

func (c *Codec) DecodeFooter(data []byte) (*Footer, error) {
	reader := bytes.NewReader(data)

	var startByte uint32
	if err := binary.Read(reader, binary.BigEndian, &startByte); err != nil {
		return nil, fmt.Errorf("failed to decode index start byte for footer: %w", err)
	}

	var length uint32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("failed to decode length for footer: %w", err)
	}

	return &Footer{
		IndexStartByte: startByte,
		Length:         length,
	}, nil
}
