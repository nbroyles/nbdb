package pkg

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nbroyles/nbdb/internal/memtable"
	"github.com/stretchr/testify/assert"
)

func TestDB_RoundTrip(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	db, err := New(dbName, DBOpts{dataDir: dir})
	defer cleanup(dbName, dir)
	assert.NoError(t, err)

	err = db.Put([]byte("foo"), []byte("bar"))
	assert.NoError(t, err)

	val, err := db.Get([]byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("bar"), val)
}

func TestDB_GetFromCompactingMemtable(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	db, err := New(dbName, DBOpts{dataDir: dir})
	defer cleanup(dbName, dir)
	assert.NoError(t, err)

	val, err := db.Get([]byte("foo"))
	assert.NoError(t, err)
	assert.Nil(t, val)

	mt := memtable.New()
	mt.Put([]byte("foo"), []byte("bar"))
	db.compactingMemTable = mt

	val, err = db.Get([]byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("bar"), val)
}

func TestDB_GetFromSSTable(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	db, err := New(dbName, DBOpts{dataDir: dir})
	defer cleanup(dbName, dir)

	assert.NoError(t, err)

	assert.NoError(t, db.Put([]byte("foo"), []byte("bar")))
	assert.NoError(t, db.Put([]byte("bar"), []byte("baz")))

	db.compactingWAL = db.walog
	db.compactingMemTable = db.memTable

	db.memTable = memtable.New()

	err = db.doCompaction()
	assert.NoError(t, err)

	assert.Nil(t, db.compactingMemTable)
	assert.Nil(t, db.compactingWAL)

	// Key found
	val, err := db.Get([]byte("foo"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("bar"), val)

	// Key not found
	val, err = db.Get([]byte("doo"))
	assert.NoError(t, err)
	assert.Nil(t, val)
}

func TestNew(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	_, err = New(dbName, DBOpts{dataDir: dir})
	defer cleanup(dbName, dir)

	assert.NoError(t, err)

	assert.True(t, dbExists(t, dbName, dir))
}

func TestNew_AlreadyExists(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	_, err = New(dbName, DBOpts{dataDir: dir})
	defer cleanup(dbName, dir)

	assert.NoError(t, err)

	_, err = New(dbName, DBOpts{dataDir: dir})
	assert.EqualError(t, err, "database foo already exists. use DB#Open instead")
}

func TestExists(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	_, err = New(dbName, DBOpts{dataDir: dir})
	defer cleanup(dbName, dir)

	assert.NoError(t, err)

	actual, err := exists(dbName, dir)
	assert.NoError(t, err)

	assert.Equal(t, dbExists(t, dbName, dir), actual)
}

func TestOpen(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	_, err = New(dbName, DBOpts{dataDir: dir})
	defer cleanup(dbName, dir)

	assert.NoError(t, err)

	// TODO: replace this logic with DB#Close when implemented
	closeDb(t, dbName, dir)

	db, err := Open(dbName, DBOpts{dataDir: dir})
	assert.NoError(t, err)

	assert.Equal(t, dbName, db.name)
}

func TestOpen_NotExist(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	_, err = Open("foo", DBOpts{dataDir: dir})
	assert.EqualError(t, err, "failed opening database foo. does not exist")
}

func TestOpenOrNew(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	assert.False(t, dbExists(t, dbName, dir))

	_, err = OpenOrNew(dbName, DBOpts{dataDir: dir})
	defer cleanup(dbName, dir)

	assert.NoError(t, err)
	assert.True(t, dbExists(t, dbName, dir))

	// TODO: replace this logic with DB#Close when implemented
	closeDb(t, dbName, dir)

	db, err := OpenOrNew(dbName, DBOpts{dataDir: dir})
	assert.NoError(t, err)

	assert.Equal(t, dbName, db.name)
}

func TestLocking(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"

	// assert lock doesn't already exist
	lockPath := path.Join(dir, dbName, lockFile)
	_, err = os.Stat(lockPath)
	assert.True(t, os.IsNotExist(err))

	db, err := New(dbName, DBOpts{dataDir: dir})
	defer cleanup(dbName, dir)
	assert.NoError(t, err)

	// Check for lock
	info, err := os.Stat(lockPath)
	assert.NoError(t, err)
	assert.NotNil(t, info)

	// Close and ensure lock gone
	db.Close()
	_, err = os.Stat(lockPath)
	assert.True(t, os.IsNotExist(err))
}

func TestFailIfLocked(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	// Set up existing lock file
	dbName := "foo"
	dbPath := path.Join(dir, dbName)
	err = os.MkdirAll(dbPath, 0755)
	defer cleanup(dbName, dir)

	assert.NoError(t, err)

	lockPath := path.Join(dbPath, lockFile)
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	assert.NoError(t, err)

	_, err = lockFile.WriteString(strconv.Itoa(os.Getpid() + 1))
	assert.NoError(t, err)

	// Try to open db; expect an error
	_, err = OpenOrNew(dbName, DBOpts{dataDir: dir})
	assert.EqualError(t, err, fmt.Sprintf("could not lock database: cannot lock database. already "+
		"locked by another process (%d)", os.Getpid()+1))
}

func TestMemtableFlush(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	db, err := New(dbName, DBOpts{dataDir: dir})
	defer cleanup(dbName, dir)

	assert.NoError(t, err)

	assert.NoError(t, db.Put([]byte("foo"), []byte("bar")))
	assert.NoError(t, db.Put([]byte("bar"), []byte("baz")))

	db.compactingWAL = db.walog
	db.compactingMemTable = db.memTable

	err = db.doCompaction()
	assert.NoError(t, err)

	assert.Nil(t, db.compactingMemTable)
	assert.Nil(t, db.compactingWAL)

	matches, err := filepath.Glob(path.Join(dir, dbName, "wal_*"))
	assert.NoError(t, err)
	assert.Zero(t, len(matches))

	matches, err = filepath.Glob(path.Join(dir, dbName, "sstable_*"))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(matches))

	// TODO: add tests for reading values once easier to parse a sstable
}

func cleanup(name string, datadir string) {
	os.RemoveAll(path.Join(datadir, name))
}

func closeDb(t *testing.T, dbName string, datadir string) {
	// Simulate a successful close by ensuring wal is removed before re-opening
	matches, err := filepath.Glob(path.Join(datadir, dbName, "wal_*"))
	assert.NoError(t, err)

	for _, match := range matches {
		err = os.Remove(match)
		assert.NoError(t, err)
	}
}

func dbExists(t *testing.T, dbName string, datadir string) bool {
	dbPath := path.Join(datadir, dbName)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return false
	} else if err == nil {
		return true
	}

	assert.FailNow(t, "could not check if database exists")

	return false
}
