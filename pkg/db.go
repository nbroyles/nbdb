package pkg

import (
	"fmt"
	"os"
	"path"

	"github.com/nbroyles/nbdb/internal/storage"

	"github.com/nbroyles/nbdb/internal/memtable"
	"github.com/nbroyles/nbdb/internal/wal"
)

// TODO: copy keys and values passed as arguments
// TODO: check key and value size and fail if > threshold
// TODO: think about concurrency at this level (e.g. concurrent calls to put/get/delete can be made
// TODO: add close method that cleans up (e.g. closes WAL (deletes?))
// TODO: Lock DB when opened for access

type DB struct {
	memTable *memtable.MemTable
	walog    *wal.WAL
	name     string
}

const (
	// Makes sense on Mac OS X, may not elsewhere
	datadir = "/usr/local/var/nbdb"
)

// New creates a new database based on the name provided.
// New fails if the database already exists
func New(name string) (*DB, error) {
	return newDB(name, datadir)
}

func newDB(name string, datadir string) (*DB, error) {
	if err := os.MkdirAll(datadir, 0755); err != nil {
		return nil, fmt.Errorf("could not create data dir %s: %w", datadir, err)
	}

	dbPath := path.Join(datadir, name)

	if exists, err := exists(name, datadir); !exists {
		if err := os.Mkdir(dbPath, 0755); err != nil {
			return nil, fmt.Errorf("failed creating data directory for database %s: %w", name, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("could not create new database: %w", err)
	} else {
		return nil, fmt.Errorf("database %s already exists. use DB#Open instead", name)
	}

	return openDB(name, datadir)
}

// Open opens a database of the name provided. Open fails
// if the database does not exist
func Open(name string) (*DB, error) {
	return openDB(name, datadir)
}

func openDB(name string, datadir string) (*DB, error) {
	if exists, err := exists(name, datadir); !exists {
		if err == nil {
			return nil, fmt.Errorf("failed opening database %s. does not exist", name)
		} else {
			return nil, fmt.Errorf("failed opening database %s: %v", name, err)
		}
	}

	return &DB{memTable: memtable.New(), walog: wal.New(name, datadir), name: name}, nil
}

// Exists checks if database name already exists or not
func Exists(name string) (bool, error) {
	return exists(name, datadir)
}

func exists(name string, datadir string) (bool, error) {
	dbPath := path.Join(datadir, name)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failure checking to see if database already exists: %w", err)
	} else {
		return true, nil
	}
}

// OpenOrNew opens the DB if it exists or creates it if it doesn't
func OpenOrNew(name string) (*DB, error) {
	return openOrNew(name, datadir)
}

func openOrNew(name string, datadir string) (*DB, error) {
	dbExists, err := exists(name, datadir)
	if err != nil {
		return nil, fmt.Errorf("failed checking if database %s already exists: %v", name, err)
	}

	if dbExists {
		return openDB(name, datadir)
	} else {
		return newDB(name, datadir)
	}
}

// Get returns the value associated with the key. If key is not found then
// the value returned is nil
func (d *DB) Get(key []byte) []byte {
	return d.memTable.Get(key)
}

// Put inserts or updates the value if the key already exists
func (d *DB) Put(key []byte, value []byte) {
	d.walog.Write(storage.NewRecord(key, value, false))
	d.memTable.Put(key, value)
}

// Deletes the specified key from the data store
func (d *DB) Delete(key []byte) {
	d.walog.Write(storage.NewRecord(key, nil, true))
	d.memTable.Delete(key)
}
