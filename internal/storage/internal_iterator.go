package storage

// InternalIterator is an interface that allows us to iterate over every element in the
// memtable. Useful for flushing memtable to disk. Not threadsafe so make use of while
// memtable is locked
type InternalIterator interface {
	// Returns true if there's another record available in the iterator
	HasNext() bool

	// Returns the next record in the iterator
	Next() *Record
}
