package wal

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/nbroyles/nbdb/internal/storage"
	"github.com/nbroyles/nbdb/test"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "wal_test"
	dbPath := path.Join(dir, dbName)

	test.MakeDB(t, dbPath)
	defer test.CleanupDB(dbPath)

	w := New(dbName, dir)
	assert.True(t, test.FileExists(t, path.Join(dbPath, w.name)))

	assert.Equal(t, dbName, w.dbName)
}

func TestWAL_Write(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "wal_test"
	dbPath := path.Join(dir, dbName)

	test.MakeDB(t, dbPath)
	defer test.CleanupDB(dbPath)

	w := New(dbName, dir)

	records := []*storage.Record{
		storage.NewRecord([]byte("foo"), []byte("bar"), false),
		storage.NewRecord([]byte("foo"), nil, true),
		storage.NewRecord([]byte("foo"), []byte("baz"), false),
		storage.NewRecord([]byte("oooooh"), []byte("wweeee"), false),
	}
	for _, record := range records {
		w.Write(record)
	}

	logPath := path.Join(dbPath, w.name)
	data, err := ioutil.ReadFile(logPath)
	assert.NoError(t, err)

	for i, j := 0, 0; i < len(data); j++ {
		reader := bytes.NewReader(data[i:])

		var totalLen uint32
		err = binary.Read(reader, binary.BigEndian, &totalLen)
		assert.NoError(t, err)

		recordBytes := data[i:(i + int(totalLen) + 4)]
		actualRecord, err := w.codec.Decode(recordBytes)
		assert.NoError(t, err)

		assert.Equal(t, records[j], actualRecord)

		i += int(totalLen + 4)
	}
}

func TestWAL_Size(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "wal_test"
	dbPath := path.Join(dir, dbName)

	test.MakeDB(t, dbPath)
	defer test.CleanupDB(dbPath)

	w := New(dbName, dir)

	sz := uint32(0)
	sz += writeRecord(t, w, storage.NewRecord([]byte("foo"), []byte("bar"), false))
	assert.Equal(t, sz, w.Size())

	sz += writeRecord(t, w, storage.NewRecord([]byte("foo2"), []byte("bar2"), false))
	assert.Equal(t, sz, w.Size())
}

func writeRecord(t *testing.T, w *WAL, rec *storage.Record) uint32 {
	data, err := w.codec.Encode(rec)
	assert.NoError(t, err)

	w.Write(rec)

	return uint32(len(data))
}
