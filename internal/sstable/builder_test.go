package sstable

import (
	"bytes"
	"testing"

	"github.com/nbroyles/nbdb/internal/memtable"
	"github.com/nbroyles/nbdb/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestSSTableBuilder_WriteLevel0Table(t *testing.T) {
	buf := bytes.Buffer{}

	mem := memtable.New()
	mem.Put([]byte("foo"), []byte("bar"))
	mem.Put([]byte("baz"), []byte("bax"))

	builder := newBuilder(mem.InternalIterator(), &buf, 1)

	builder.WriteLevel0Table()

	// Expect buf to now have:
	// - 2 record entries aka 2 records
	// - 2 index entries aka 2 record pointers
	// - 1 footer aka 1 record pointer

	data := buf.Bytes()
	codec := builder.codec

	idx1Ptr := decodePointer(t, codec, data, uint32(len(data)-pointerLen), uint32(len(data)))
	rec1Ptr := decodePointer(t, codec, data, idx1Ptr.StartByte, idx1Ptr.StartByte+pointerLen)
	rec2Ptr := decodePointer(t, codec, data, idx1Ptr.StartByte+pointerLen, idx1Ptr.StartByte+(pointerLen*2))

	rec1 := decodeRecord(t, codec, data, rec1Ptr.StartByte, rec1Ptr.StartByte+rec1Ptr.Length)
	assert.Equal(t, rec1, storage.NewRecord([]byte("baz"), []byte("bax"), false))

	rec2 := decodeRecord(t, codec, data, rec2Ptr.StartByte, rec2Ptr.StartByte+rec2Ptr.Length)
	assert.Equal(t, rec2, storage.NewRecord([]byte("foo"), []byte("bar"), false))
}

func decodePointer(t *testing.T, codec *storage.Codec, bytes []byte, start uint32, stop uint32) *storage.RecordPointer {
	ptr, err := codec.DecodePointer(bytes[start:stop])
	assert.NoError(t, err)

	return ptr
}

func decodeRecord(t *testing.T, codec *storage.Codec, bytes []byte, start uint32, stop uint32) *storage.Record {
	rec, err := codec.Decode(bytes[start:stop])
	assert.NoError(t, err)

	return rec
}
