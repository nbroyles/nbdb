package manifest

import (
	"bytes"
	"encoding/binary"
	"os"
	"path"
	"testing"

	"github.com/nbroyles/nbdb/internal/sstable"

	"github.com/nbroyles/nbdb/test"
	"github.com/stretchr/testify/assert"
)

func TestManifest_AddEntry(t *testing.T) {
	buf := bytes.Buffer{}
	man := NewManifest(&buf)

	entry1 := &Entry{
		metadata: &sstable.Metadata{
			Level:    0,
			Filename: "foo",
		},
		deleted: false,
	}
	entry2 := &Entry{
		metadata: &sstable.Metadata{
			Level:    0,
			Filename: "bar",
		},
		deleted: true,
	}

	assert.NoError(t, man.AddEntry(entry1))

	eLen := binary.BigEndian.Uint32(buf.Bytes()[0:uint32size])
	actual, err := man.codec.DecodeEntry(buf.Bytes()[4 : 4+eLen])
	assert.NoError(t, err)
	assert.Equal(t, entry1, actual)

	assert.NoError(t, man.AddEntry(entry2))

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

	m, err := CreateManifestFile(dbName, dir)
	assert.NoError(t, err)

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
	m, err := CreateManifestFile(dbName, dir)
	assert.NoError(t, err)
	assert.True(t, test.FileExists(t, m.Name()))
	man := NewManifest(m)

	// Add some entries
	assert.NoError(t, man.AddEntry(&Entry{metadata: &sstable.Metadata{Level: 0, Filename: ""}, deleted: false}))
	assert.NoError(t, man.AddEntry(&Entry{metadata: &sstable.Metadata{Level: 0, Filename: ""}, deleted: true}))

	// Open as new manifest
	_, man2, err := LoadLatest(dbName, dir)
	assert.NoError(t, err)

	assert.Equal(t, man.entries, man2.entries)
}
