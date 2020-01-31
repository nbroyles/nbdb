package storage

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodec_RoundTripUpdate(t *testing.T) {
	codec := Codec{}
	data, err := codec.Encode(&Record{
		Key:   []byte("foo"),
		Value: []byte("bar"),
		Type:  RecordUpdate,
	})
	assert.NoError(t, err)

	totalLen := binary.BigEndian.Uint32(data[0:4])
	assert.Equal(t, totalLen, uint32(len(data)-4)) // length of data minus preceding bytes tracking total record len

	record, err := codec.Decode(data[4:])
	assert.NoError(t, err)

	assert.Equal(t, Record{
		Key:   []byte("foo"),
		Value: []byte("bar"),
		Type:  RecordUpdate,
	}, *record)
}

func TestCodec_RoundTripDelete(t *testing.T) {
	codec := Codec{}
	data, err := codec.Encode(&Record{
		Key:  []byte("foo"),
		Type: RecordDelete,
	})
	assert.NoError(t, err)

	totalLen := binary.BigEndian.Uint32(data[0:4])
	assert.Equal(t, totalLen, uint32(len(data)-4))

	record, err := codec.Decode(data[4:])
	assert.NoError(t, err)

	assert.Equal(t, Record{
		Key:  []byte("foo"),
		Type: RecordDelete,
	}, *record)
}

func TestCodec_ChecksumFail(t *testing.T) {
	codec := Codec{}
	data, err := codec.Encode(&Record{
		Key:   []byte("foo"),
		Value: []byte("bar"),
		Type:  RecordUpdate,
	})
	assert.NoError(t, err)

	csBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(csBytes, uint32(12))

	// Rewrite checksum bytes with wrong value
	csStart := len(data) - 4
	for i := 0; i < 4; i++ {
		data[csStart+i] = csBytes[i]
	}

	totalLen := binary.BigEndian.Uint32(data[0:4])
	assert.Equal(t, totalLen, uint32(len(data)-4))

	_, err = codec.Decode(data[4:])
	assert.EqualError(t, err, "expected checksum of WAL record does not match! expected=12, actual=538011314")
}
