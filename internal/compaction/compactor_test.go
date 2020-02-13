package compaction

import (
	"path"
	"testing"

	"github.com/nbroyles/nbdb/internal/manifest"
	"github.com/nbroyles/nbdb/internal/memtable/interfaces"
	"github.com/nbroyles/nbdb/internal/sstable"
	"github.com/nbroyles/nbdb/internal/test"
	"github.com/nbroyles/nbdb/internal/util"
	"github.com/stretchr/testify/assert"
)

// TODO Test:
// - level 0 not full, level 1 full, merge into existing level 2

func TestCompactor_Compact_Level0NotFull(t *testing.T) {
	dataDir, dbName := test.ConfigureDataDir(t, "foo")
	defer test.Cleanup(t, path.Join(dataDir, dbName))

	md1 := writeTable(t, 0, "sst1", test.NewStaticIterator(map[string]string{
		"aaa": "blarg",
		"baz": "bax",
	}), dataDir, dbName)

	md2 := writeTable(t, 0, "sst2", test.NewStaticIterator(map[string]string{
		"foo":   "butt",
		"howdy": "time",
	}), dataDir, dbName)

	md3 := writeTable(t, 0, "sst3", test.NewStaticIterator(map[string]string{
		"ohhh":   "brother",
		"whoomp": "there it is",
	}), dataDir, dbName)

	mfile, err := manifest.CreateManifestFile(dbName, dataDir)
	assert.NoError(t, err)
	man := manifest.NewManifest(mfile)

	assert.NoError(t, man.AddEntry(manifest.NewEntry(md1, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md2, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md3, false)))

	c := New(man, dataDir, dbName)

	assert.NoError(t, c.Compact())

	assert.Equal(t, []*sstable.Metadata{md1, md2, md3}, man.MetadataForLevel(0))
}

func TestCompactor_Compact_Level0Full(t *testing.T) {
	dataDir, dbName := test.ConfigureDataDir(t, "foo")
	defer test.Cleanup(t, path.Join(dataDir, dbName))

	md1 := writeTable(t, 0, "sst1", test.NewStaticIterator(map[string]string{
		"aaa": "blarg",
		"baz": "bax",
	}), dataDir, dbName)

	md2 := writeTable(t, 0, "sst2", test.NewStaticIterator(map[string]string{
		"foo":   "butt",
		"howdy": "time",
	}), dataDir, dbName)

	md3 := writeTable(t, 0, "sst3", test.NewStaticIterator(map[string]string{
		"ohhh":   "brother",
		"whoomp": "there it is",
	}), dataDir, dbName)

	md4 := writeTable(t, 0, "sst4", test.NewStaticIterator(map[string]string{
		"full": "af",
	}), dataDir, dbName)

	mfile, err := manifest.CreateManifestFile(dbName, dataDir)
	assert.NoError(t, err)
	man := manifest.NewManifest(mfile)

	assert.NoError(t, man.AddEntry(manifest.NewEntry(md1, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md2, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md3, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md4, false)))

	c := New(man, dataDir, dbName)

	assert.NoError(t, c.Compact())

	assert.Equal(t, 0, len(man.MetadataForLevel(0)))
	assert.Equal(t, 1, len(man.MetadataForLevel(1)))

	actual := man.MetadataForLevel(1)[0]
	assert.Equal(t, &sstable.Metadata{
		Level:    1,
		Filename: actual.Filename,
		StartKey: []byte("aaa"),
		EndKey:   []byte("whoomp"),
	}, actual)
}

func TestCompactor_Compact_Level0FullExistingLevel1WithOverlap(t *testing.T) {
	dataDir, dbName := test.ConfigureDataDir(t, "foo")
	defer test.Cleanup(t, path.Join(dataDir, dbName))

	md1 := writeTable(t, 0, "sst1", test.NewStaticIterator(map[string]string{
		"aaa": "blarg",
		"baz": "bax",
	}), dataDir, dbName)

	md2 := writeTable(t, 0, "sst2", test.NewStaticIterator(map[string]string{
		"foo":   "butt",
		"howdy": "time",
	}), dataDir, dbName)

	md3 := writeTable(t, 0, "sst3", test.NewStaticIterator(map[string]string{
		"ohhh":   "brother",
		"whoomp": "there it is",
	}), dataDir, dbName)

	md4 := writeTable(t, 0, "sst4", test.NewStaticIterator(map[string]string{
		"full": "af",
	}), dataDir, dbName)

	md5 := writeTable(t, 1, "sst5", test.NewStaticIterator(map[string]string{
		"nah": "dude",
		"zig": "zag",
	}), dataDir, dbName)

	mfile, err := manifest.CreateManifestFile(dbName, dataDir)
	assert.NoError(t, err)
	man := manifest.NewManifest(mfile)

	assert.NoError(t, man.AddEntry(manifest.NewEntry(md1, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md2, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md3, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md4, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md5, false)))

	c := New(man, dataDir, dbName)

	assert.NoError(t, c.Compact())

	assert.Equal(t, 0, len(man.MetadataForLevel(0)))
	assert.Equal(t, 1, len(man.MetadataForLevel(1)))

	actual := man.MetadataForLevel(1)[0]
	assert.Equal(t, &sstable.Metadata{
		Level:    1,
		Filename: actual.Filename,
		StartKey: []byte("aaa"),
		EndKey:   []byte("zig"),
	}, actual)
}

func TestCompactor_Compact_Level0FullExistingLevel1WithNoOverlap(t *testing.T) {
	dataDir, dbName := test.ConfigureDataDir(t, "foo")
	defer test.Cleanup(t, path.Join(dataDir, dbName))

	md1 := writeTable(t, 0, "sst1", test.NewStaticIterator(map[string]string{
		"aaa": "blarg",
		"baz": "bax",
	}), dataDir, dbName)

	md2 := writeTable(t, 0, "sst2", test.NewStaticIterator(map[string]string{
		"foo":   "butt",
		"howdy": "time",
	}), dataDir, dbName)

	md3 := writeTable(t, 0, "sst3", test.NewStaticIterator(map[string]string{
		"ohhh":   "brother",
		"whoomp": "there it is",
	}), dataDir, dbName)

	md4 := writeTable(t, 0, "sst4", test.NewStaticIterator(map[string]string{
		"full": "af",
	}), dataDir, dbName)

	md5 := writeTable(t, 1, "sst5", test.NewStaticIterator(map[string]string{
		"zig":   "zag",
		"zzzzz": "sadman",
	}), dataDir, dbName)

	mfile, err := manifest.CreateManifestFile(dbName, dataDir)
	assert.NoError(t, err)
	man := manifest.NewManifest(mfile)

	assert.NoError(t, man.AddEntry(manifest.NewEntry(md1, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md2, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md3, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md4, false)))
	assert.NoError(t, man.AddEntry(manifest.NewEntry(md5, false)))

	c := New(man, dataDir, dbName)

	assert.NoError(t, c.Compact())

	assert.Equal(t, 0, len(man.MetadataForLevel(0)))
	assert.Equal(t, 2, len(man.MetadataForLevel(1)))

	actuals := man.MetadataForLevel(1)
	assert.Equal(t, []*sstable.Metadata{
		{
			Level:    1,
			Filename: actuals[0].Filename,
			StartKey: []byte("zig"),
			EndKey:   []byte("zzzzz"),
		},
		{
			Level:    1,
			Filename: actuals[1].Filename,
			StartKey: []byte("aaa"),
			EndKey:   []byte("whoomp"),
		},
	}, actuals)
}

func writeTable(t *testing.T, level int, filename string, iter interfaces.InternalIterator, dataDir string, dbName string) *sstable.Metadata {
	file, err := util.CreateFile(filename, dbName, dataDir)
	assert.NoError(t, err)
	bldr := sstable.NewBuilder(filename, iter, level, file)

	meta, err := bldr.WriteTable()
	assert.NoError(t, err)

	return meta
}
