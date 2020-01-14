package wal

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodec_RoundTripUpdate(t *testing.T) {
	codec := Codec{}
	data, err := codec.Encode(&Record{
		key:   []byte("foo"),
		value: []byte("bar"),
		rType: recordUpdate,
	})
	assert.NoError(t, err)

	record, err := codec.Decode(data)
	assert.NoError(t, err)

	assert.Equal(t, Record{
		key:   []byte("foo"),
		value: []byte("bar"),
		rType: recordUpdate,
	}, *record)
}

func TestCodec_RoundTripDelete(t *testing.T) {
	codec := Codec{}
	data, err := codec.Encode(&Record{
		key:   []byte("foo"),
		rType: recordDelete,
	})
	assert.NoError(t, err)

	record, err := codec.Decode(data)
	assert.NoError(t, err)

	assert.Equal(t, Record{
		key:   []byte("foo"),
		rType: recordDelete,
	}, *record)
}

func TestCodec_ChecksumFail(t *testing.T) {
	codec := Codec{}
	data, err := codec.Encode(&Record{
		key:   []byte("foo"),
		value: []byte("bar"),
		rType: recordUpdate,
	})
	assert.NoError(t, err)

	csBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(csBytes, uint32(12))

	// Rewrite checksum bytes with wrong value
	csStart := len(data) - 4
	for i := 0; i < 4; i++ {
		data[csStart+i] = csBytes[i]
	}

	assert.PanicsWithValue(t, "expected checksum of WAL record does not match! expected=12, actual=2211583973", func() {
		_, _ = codec.Decode(data)
	})
}
