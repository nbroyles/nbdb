package skiplist

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterator_HasNext(t *testing.T) {
	list1 := New(1)
	iter := NewIterator(list1)
	assert.False(t, iter.HasNext())

	list2 := New(1)
	put(list2, "foo", "bar")
	iter = NewIterator(list2)
	assert.True(t, iter.HasNext())
}

func TestIterator_Next(t *testing.T) {
	list := New(1)
	put(list, "foo", "bar")
	put(list, "baz", "bax")
	list.Delete([]byte("baz"))

	iter := NewIterator(list)

	// Remember, skip list is ordered, so next is opposite of insertion order
	assert.True(t, iter.HasNext())
	assertNextRecordEquals(t, iter, "baz", "bax", true)

	assert.True(t, iter.HasNext())
	assertNextRecordEquals(t, iter, "foo", "bar", false)

	assert.False(t, iter.HasNext())
}

func TestIterator_EmptyList(t *testing.T) {
	list := New(1)
	iter := NewIterator(list)

	assert.Panics(t, func() { iter.Next() })
}
