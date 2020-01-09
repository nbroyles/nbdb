package memtable

import (
	"time"

	"github.com/nbroyles/nbdb/internal/skiplist"
	"github.com/nbroyles/nbdb/internal/storage"
)

type MemTable struct {
	memStore storage.InMemoryStore
}

func New() *MemTable {
	return &MemTable{memStore: skiplist.New(time.Now().UnixNano())}
}

func (m *MemTable) Get(key []byte) []byte {
	if found, val := m.memStore.Get(key); found {
		return val
	} else {
		return nil
	}
}

func (m *MemTable) Put(key []byte, value []byte) {
	m.memStore.Put(key, value)
}

func (m *MemTable) Delete(key []byte) {
	m.memStore.Delete(key)
}
