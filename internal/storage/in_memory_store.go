package storage

// InMemoryStore is to be implemented by any data structure that's to be used as the
// in memory store for the MemTable.
type InMemoryStore interface {
	// Get returns a boolean indicating whether the specified key
	// was found in the list. If true, the value is returned as well
	Get(key []byte) (bool, []byte)

	// Put inserts or updates the value if the key already exists
	Put(key []byte, value []byte)

	// Removes the specified key from the skip list. Returns true if
	// key was removed and false if key was not present
	Remove(key []byte) bool
}
