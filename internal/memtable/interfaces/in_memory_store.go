package interfaces

// InMemoryStore is to be implemented by any data structure that's to be used as the
// in memory store for the MemTable.
type InMemoryStore interface {
	// Get returns a boolean indicating whether the specified key
	// was found in the list. If true, the value is returned as well
	Get(key []byte) (bool, []byte)

	// Put inserts or updates the value if the key already exists
	Put(key []byte, value []byte)

	// Deletes the specified key from the skip list. Returns true if
	// key was removed and false if key was not present
	Delete(key []byte) bool

	// InternalIterator returns an iterator that can be used to iterate over each element
	// in the store. Primarily useful when flushing structure to an sstable on disk
	InternalIterator() InternalIterator

	// Size returns the approximate size of the underlying structure
	Size() uint32
}
