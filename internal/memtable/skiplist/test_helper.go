package skiplist

import (
	"testing"

	"github.com/nbroyles/nbdb/internal/memtable/interfaces"

	"github.com/nbroyles/nbdb/internal/storage"
	"github.com/stretchr/testify/assert"
)

func put(list *SkipList, key string, value string) {
	list.Put([]byte(key), []byte(value))
}

func assertNextRecordEquals(t *testing.T, i interfaces.InternalIterator, key string, value string, delete bool) {
	assert.Equal(t, storage.NewRecord([]byte(key), []byte(value), delete), i.Next())
}
