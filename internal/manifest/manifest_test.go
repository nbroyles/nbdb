package manifest

import (
	"bytes"
	"testing"

	"github.com/nbroyles/nbdb/internal/sstable"
	"github.com/stretchr/testify/assert"
)

func TestManifest_AddEntry(t *testing.T) {
	buf := bytes.Buffer{}
	man := NewManifest(&buf)

	entry1 := &Entry{
		metadata: &sstable.Metadata{},
		deleted:  false,
	}
	entry2 := &Entry{
		metadata: &sstable.Metadata{},
		deleted:  true,
	}

	man.AddEntry(entry1)

	actual, err := man.codec.DecodeEntry(buf.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, entry1, actual)

	man.AddEntry(entry2)

	actual, err = man.codec.DecodeEntry(buf.Bytes()[5:])
	assert.NoError(t, err)
	assert.Equal(t, entry2, actual)
}
