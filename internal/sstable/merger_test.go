package sstable

import (
	"path"
	"path/filepath"
	"testing"

	"github.com/nbroyles/nbdb/internal/test"

	"github.com/nbroyles/nbdb/internal/memtable"
	"github.com/nbroyles/nbdb/internal/util"
	"github.com/stretchr/testify/assert"
)

// TODO: refactor merger so we don't have to actually write to disk but to byte buffers instead
func TestMerger_Merge(t *testing.T) {
	dataDir, dbName := test.ConfigureDataDir(t, "foo")
	defer test.Cleanup(t, path.Join(dataDir, dbName))

	// create a bunch of level 0 files
	mem1 := memtable.New()
	mem1.Put([]byte("foo"), []byte("bar"))
	mem1.Put([]byte("baz"), []byte("bax"))
	md01 := writeMemTable(t, "sst01", dbName, dataDir, mem1)

	mem2 := memtable.New()
	mem2.Put([]byte("aaa"), []byte("blarg"))
	mem2.Put([]byte("foo"), []byte("butt"))
	md02 := writeMemTable(t, "sst02", dbName, dataDir, mem2)

	/*
		TODO: test this once bug in memtable flushing logic fixed
		mem3 := memtable.New()
		mem3.Delete([]byte("aaa"))
		mem3.Put([]byte("howdy"), []byte("time"))
		md03 := writeMemTable(t, "sst03", dbName, dataDir, mem3)
	*/

	mem3 := memtable.New()
	mem3.Put([]byte("yerrr"), []byte("ayyy"))
	mem3.Put([]byte("howdy"), []byte("time"))
	md03 := writeMemTable(t, "sst03", dbName, dataDir, mem3)

	mem4 := memtable.New()
	mem4.Put([]byte("ohhh"), []byte("brother"))
	mem4.Put([]byte("whoomp"), []byte("there it is"))
	md04 := writeMemTable(t, "sst04", dbName, dataDir, mem4)

	mrg := NewMerger(0, 1, []*Metadata{md04, md03, md02, md01}, dataDir, dbName)

	res, err := mrg.Merge()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))

	mergeMeta := res[0]

	assert.Equal(t, &Metadata{
		Level:    1,
		Filename: mergeMeta.Filename,
		StartKey: []byte("aaa"),
		EndKey:   []byte("yerrr"),
	}, mergeMeta)

	test.AssertTable(t, map[string]string{
		"aaa":    "blarg",
		"baz":    "bax",
		"foo":    "butt",
		"howdy":  "time",
		"ohhh":   "brother",
		"whoomp": "there it is",
		"yerrr":  "ayyy",
	}, mergeMeta.Filename, path.Join(dataDir, dbName))
}

func writeMemTable(t *testing.T, filename string, dbName string, dataDir string, mem *memtable.MemTable) *Metadata {
	sst01, err := util.CreateFile(filename, dbName, dataDir)
	assert.NoError(t, err)

	builder := NewBuilder(filepath.Base(sst01.Name()), mem.InternalIterator(), 0, sst01)
	md01, err := builder.WriteTable()
	assert.NoError(t, err)

	return md01
}
