package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetadata_ContainsKey(t *testing.T) {
	md := Metadata{
		Level:    0,
		Filename: "foo",
		StartKey: []byte("alpha"),
		EndKey:   []byte("omega"),
	}

	assert.True(t, md.ContainsKey([]byte("foo")))
	assert.True(t, md.ContainsKey([]byte("alpha")))
	assert.True(t, md.ContainsKey([]byte("omega")))
	assert.False(t, md.ContainsKey([]byte("zomg")))
}
