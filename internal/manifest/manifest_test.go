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
			StartKey: []byte("foo"),
			EndKey:   []byte("bar"),
		},
		deleted: false,
	}
	entry2 := &Entry{
		metadata: &sstable.Metadata{
			Level:    0,
			Filename: "bar",
			StartKey: []byte("baz"),
			EndKey:   []byte("bax"),
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

	meta := man.MetadataForLevel(0)
	assert.Equal(t, 1, len(meta))
	assert.Equal(t, entry1.metadata, meta[0])
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
	assert.NoError(t, man.AddEntry(&Entry{metadata: &sstable.Metadata{Level: 0, Filename: "", StartKey: []byte(""), EndKey: []byte("")}, deleted: false}))
	assert.NoError(t, man.AddEntry(&Entry{metadata: &sstable.Metadata{Level: 0, Filename: "", StartKey: []byte(""), EndKey: []byte("")}, deleted: true}))

	// Open as new manifest
	_, man2, err := LoadLatest(dbName, dir)
	assert.NoError(t, err)

	assert.Equal(t, man.entries, man2.entries)
	assert.Equal(t, 1, len(man.MetadataForLevel(0)))
}

func TestManifest_MetadataForLevel(t *testing.T) {
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
	md0_1 := &sstable.Metadata{Level: 0, Filename: "", StartKey: []byte(""), EndKey: []byte("")}
	md0_2 := &sstable.Metadata{Level: 0, Filename: "", StartKey: []byte(""), EndKey: []byte("")}
	md1_1 := &sstable.Metadata{Level: 1, Filename: "", StartKey: []byte(""), EndKey: []byte("")}
	assert.NoError(t, man.AddEntry(&Entry{metadata: md0_1, deleted: false}))
	assert.NoError(t, man.AddEntry(&Entry{metadata: md0_2, deleted: false}))
	assert.NoError(t, man.AddEntry(&Entry{metadata: md1_1, deleted: false}))

	l0Meta := man.MetadataForLevel(0)
	l1Meta := man.MetadataForLevel(1)

	assert.Equal(t, 2, len(l0Meta))
	assert.Equal(t, []*sstable.Metadata{md0_1, md0_2}, l0Meta)

	assert.Equal(t, 1, len(l1Meta))
	assert.Equal(t, []*sstable.Metadata{md1_1}, l1Meta)
}
