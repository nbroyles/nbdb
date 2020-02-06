package sstable

import (
	"bytes"
	"testing"

	"github.com/nbroyles/nbdb/internal/memtable"
	"github.com/stretchr/testify/assert"
)

// TODO: Test w/ multiple index entries once indexPerRecord is configurable and stored in Metadata
func TestSearch(t *testing.T) {
	// Build memtable and flush to disk
	mem := memtable.New()
	mem.Put([]byte("foo"), []byte("bar"))
	mem.Put([]byte("howdy"), []byte("time"))
	mem.Put([]byte("sick"), []byte("dude"))

	buf := bytes.Buffer{}
	builder := newBuilder("test", mem.InternalIterator(), &buf, 1)

	meta, err := builder.WriteLevel0Table()
	assert.NoError(t, err)
	assert.Equal(t, &Metadata{Level: 0, Filename: "test", StartKey: []byte("foo"), EndKey: []byte("sick")}, meta)

	// Search for keys
	val, err := Search([]byte("howdy"), bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	assert.Equal(t, []byte("time"), val)

	val, err = Search([]byte("foo"), bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	assert.Equal(t, []byte("bar"), val)

	val, err = Search([]byte("sick"), bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	assert.Equal(t, []byte("dude"), val)

	val, err = Search([]byte("goo"), bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	assert.Nil(t, val)
}
