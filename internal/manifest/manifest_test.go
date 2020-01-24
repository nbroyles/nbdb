package manifest

import (
	"bytes"
	"encoding/binary"
	"os"
	"path"
	"testing"

	"github.com/nbroyles/nbdb/test"
	"github.com/stretchr/testify/assert"
)

func TestManifest_AddEntry(t *testing.T) {
	buf := bytes.Buffer{}
	man := NewManifest(&buf)

	entry1 := &Entry{
		deleted: false,
	}
	entry2 := &Entry{
		deleted: true,
	}

	man.AddEntry(entry1)

	eLen := binary.BigEndian.Uint32(buf.Bytes()[0:uint32size])
	actual, err := man.codec.DecodeEntry(buf.Bytes()[4 : 4+eLen])
	assert.NoError(t, err)
	assert.Equal(t, entry1, actual)

	man.AddEntry(entry2)

	e1End := 4 + eLen
	eLen = binary.BigEndian.Uint32(buf.Bytes()[e1End : e1End+uint32size])
	actual, err = man.codec.DecodeEntry(buf.Bytes()[e1End+uint32size : e1End+uint32size+eLen])
	assert.NoError(t, err)
	assert.Equal(t, entry2, actual)
}

func TestCreateManifestFile(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "manifest_test"
	dbPath := path.Join(dir, dbName)

	test.MakeDB(t, dbPath)
	defer test.CleanupDB(dbPath)

	m := CreateManifestFile(dbName, dir)

	assert.True(t, test.FileExists(t, m.Name()))
}

func TestLoadLatest(t *testing.T) {
	dir, err := os.Getwd()
	assert.NoError(t, err)

	dbName := "manifest_test"
	dbPath := path.Join(dir, dbName)

	test.MakeDB(t, dbPath)
	defer test.CleanupDB(dbPath)

	// Create manifest
	m := CreateManifestFile(dbName, dir)
	assert.True(t, test.FileExists(t, m.Name()))
	man := NewManifest(m)

	// Add some entries
	man.AddEntry(&Entry{deleted: false})
	man.AddEntry(&Entry{deleted: true})

	// Open as new manifest
	man2 := LoadLatest(dbName, dir)

	assert.Equal(t, man.entries, man2.entries)
}
