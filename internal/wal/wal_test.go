package wal

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "wal_test"
	dbPath := path.Join(dir, dbName)

	makeDatabase(t, dbPath)
	defer cleanup(dbPath)

	w := New(dbName, dir)
	assert.True(t, logExists(t, path.Join(dbPath, w.name)))

	assert.Equal(t, dbName, w.dbName)
}

func TestWAL_Write(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "wal_test"
	dbPath := path.Join(dir, dbName)

	makeDatabase(t, dbPath)
	defer cleanup(dbPath)

	w := New(dbName, dir)

	records := []*Record{
		NewRecord([]byte("foo"), []byte("bar"), false),
		NewRecord([]byte("foo"), nil, true),
		NewRecord([]byte("foo"), []byte("baz"), false),
		NewRecord([]byte("oooooh"), []byte("wweeee"), false),
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

	makeDatabase(t, dbPath)
	defer cleanup(dbPath)

	w := New(dbName, dir)

	sz := uint32(0)
	sz += writeRecord(t, w, NewRecord([]byte("foo"), []byte("bar"), false))
	assert.Equal(t, sz, w.Size())

	sz += writeRecord(t, w, NewRecord([]byte("foo2"), []byte("bar2"), false))
	assert.Equal(t, sz, w.Size())
}

func writeRecord(t *testing.T, w *WAL, rec *Record) uint32 {
	data, err := w.codec.Encode(rec)
	assert.NoError(t, err)

	w.Write(rec)

	return uint32(len(data))
}

func logExists(t *testing.T, logPath string) bool {
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		return false
	} else if err == nil {
		return true
	}

	assert.FailNow(t, "could not check if WAL exists")

	return false
}

func makeDatabase(t *testing.T, dbFilePath string) {
	err := os.MkdirAll(dbFilePath, 0755)
	assert.NoError(t, err)
}

func cleanup(dbPath string) {
	os.RemoveAll(dbPath)
}
