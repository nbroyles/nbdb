package pkg

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	_, err = newDB(dbName, dir)
	defer cleanup(dbName, dir)

	assert.NoError(t, err)

	assert.True(t, dbExists(t, dbName, dir))
}

func TestNew_AlreadyExists(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	_, err = newDB(dbName, dir)
	defer cleanup(dbName, dir)

	assert.NoError(t, err)

	_, err = newDB(dbName, dir)
	assert.EqualError(t, err, "database foo already exists. use DB#Open instead")
}

func TestExists(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	_, err = newDB(dbName, dir)
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
	_, err = newDB(dbName, dir)
	defer cleanup(dbName, dir)

	assert.NoError(t, err)

	// TODO: replace this logic with DB#Close when implemented
	closeDb(t, dbName, dir)

	db, err := openDB(dbName, dir)
	assert.NoError(t, err)

	assert.Equal(t, dbName, db.name)
}

func TestOpen_NotExist(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	_, err = openDB("foo", dir)
	assert.EqualError(t, err, "failed opening database foo. does not exist")
}

func TestOpenOrNew(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "foo"
	assert.False(t, dbExists(t, dbName, dir))

	_, err = openOrNew(dbName, dir)
	defer cleanup(dbName, dir)

	assert.NoError(t, err)
	assert.True(t, dbExists(t, dbName, dir))

	// TODO: replace this logic with DB#Close when implemented
	closeDb(t, dbName, dir)

	db, err := openOrNew(dbName, dir)
	assert.NoError(t, err)

	assert.Equal(t, dbName, db.name)
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
