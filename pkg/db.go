package pkg

import (
	"github.com/nbroyles/nbdb/internal/memtable"
)

type DB struct {
	memTable *memtable.MemTable
}

func New() *DB {
	return &DB{memTable: memtable.New()}
}

// Get returns the value associated with the key. If key is not found then
// the value returned is nil
func (d *DB) Get(key []byte) []byte {
	return d.memTable.Get(key)
}

// Put inserts or updates the value if the key already exists
func (d *DB) Put(key []byte, value []byte) {
	d.memTable.Put(key, value)
}

// Deletes the specified key from the data store
func (d *DB) Delete(key []byte) {
	d.memTable.Delete(key)
}
