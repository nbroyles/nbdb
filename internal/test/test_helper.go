package test

import (
	"os"
	"path"
	"sort"
	"testing"

	"github.com/nbroyles/nbdb/internal/memtable/interfaces"
	"github.com/nbroyles/nbdb/internal/storage"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func ConfigureDataDir(t *testing.T, dbName string) (string, string) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbPath := path.Join(dir, dbName)
	err = os.MkdirAll(dbPath, 0755)
	assert.NoError(t, err)

	return dir, dbName
}

func Cleanup(t *testing.T, dbPath string) {
	assert.NoError(t, os.RemoveAll(dbPath))
}

func AssertTable(t *testing.T, entries map[string]string, filename, tablePath string) {
	num := len(entries)

	// Gotta grab and sort keys because map iteration order is not guaranteed
	var keys []string
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	reader, err := os.Open(path.Join(tablePath, filename))
	assert.NoError(t, err)

	codec := storage.Codec{}
	for i := 0; i < num; i++ {
		rec, err := codec.DecodeFromReader(reader)
		assert.NoError(t, err)

		assert.Equal(t, []byte(keys[i]), rec.Key)
		assert.Equal(t, []byte(entries[keys[i]]), rec.Value)
	}
}

type StaticIterator struct {
	entries map[string]string
	keys    []string
	pointer int
}

func NewStaticIterator(entries map[string]string) interfaces.InternalIterator {
	var keys []string
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return &StaticIterator{entries: entries, keys: keys, pointer: 0}
}

func (s *StaticIterator) HasNext() bool {
	return s.pointer < len(s.keys)
}

func (s *StaticIterator) Next() *storage.Record {
	if !s.HasNext() {
		log.Panic("iterator has no next element")
	}

	key := s.keys[s.pointer]
	var val []byte
	deleted := false
	if s.entries[key] == "" {
		val = nil
		deleted = true
	} else {
		val = []byte(s.entries[key])
	}

	s.pointer += 1

	return storage.NewRecord([]byte(key), val, deleted)
}

var _ interfaces.InternalIterator = &StaticIterator{}
