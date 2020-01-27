package sstable

import (
	"bytes"
	"encoding/binary"
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

	footer := decodeFooter(t, codec, data, uint32(len(data)-pointerLen), uint32(len(data)))
	idx1 := decodePointer(t, codec, data, footer.IndexStartByte, footer.IndexStartByte+footer.Length)

	// Get the key length since we don't have a pointer w/ byte length to the first index in the list.
	// Now need to calculate how big next index is so that we can read it
	idx2Start := footer.IndexStartByte + footer.Length
	var keyLen uint32
	err := binary.Read(bytes.NewReader(data[idx2Start:idx2Start+4]), binary.BigEndian, &keyLen)
	assert.NoError(t, err)
	idx2Size := 4 + keyLen + 4 + 4 // len(key) + key + recordStartByte + recordLen

	idx2 := decodePointer(t, codec, data, idx2Start, idx2Start+idx2Size)

	rec1 := decodeRecord(t, codec, data, idx1.StartByte, idx1.StartByte+idx1.Length)
	assert.Equal(t, rec1, storage.NewRecord([]byte("baz"), []byte("bax"), false))

	rec2 := decodeRecord(t, codec, data, idx2.StartByte, idx2.StartByte+idx2.Length)
	assert.Equal(t, rec2, storage.NewRecord([]byte("foo"), []byte("bar"), false))
}

func decodePointer(t *testing.T, codec *storage.Codec, bytes []byte, start uint32, stop uint32) *storage.RecordPointer {
	ptr, err := codec.DecodePointer(bytes[start:stop])
	assert.NoError(t, err)

	return ptr
}

func decodeRecord(t *testing.T, codec *storage.Codec, bytes []byte, start uint32, stop uint32) *storage.Record {
	// +4 to ignore totalLen uint32
	rec, err := codec.Decode(bytes[start+4 : stop])
	assert.NoError(t, err)

	return rec
}

func decodeFooter(t *testing.T, codec *storage.Codec, bytes []byte, start uint32, stop uint32) *storage.Footer {
	ptr, err := codec.DecodeFooter(bytes[start:stop])
	assert.NoError(t, err)

	return ptr
}
