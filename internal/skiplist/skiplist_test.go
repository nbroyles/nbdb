package skiplist

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: more detailed tests
// - locking semantics
// - level generation
// - removal from multiple levels

func TestSkipList_RoundTrip(t *testing.T) {
	list := New(1)

	put(list, "howdy", "time")
	assertSkipListValue(t, list, "howdy", "time")
}

func TestSkipList_Put(t *testing.T) {
	list := New(1)

	put(list, "a", "lot")
	put(list, "of", "keys")
	put(list, "into", "this")
	put(list, "bad", "boy")
	put(list, "!!!!", "!!!!")

	assertSkipListValue(t, list, "a", "lot")
	assertSkipListValue(t, list, "of", "keys")
	assertSkipListValue(t, list, "into", "this")
	assertSkipListValue(t, list, "bad", "boy")
	assertSkipListValue(t, list, "!!!!", "!!!!")
}

func TestSkipList_Delete(t *testing.T) {
	list := New(1)

	put(list, "foo", "bar")

	assert.True(t, list.Delete([]byte("foo")))

	found, val := list.Get([]byte("foo"))
	assert.False(t, found)
	assert.Nil(t, val)

	assert.False(t, list.Delete([]byte("foo")))
}

func TestSkipList_Update(t *testing.T) {
	list := New(1)

	put(list, "foo", "bar")
	assertSkipListValue(t, list, "foo", "bar")

	put(list, "foo", "baz")
	assertSkipListValue(t, list, "foo", "baz")
}

func TestSkipList_MultipleInserts(t *testing.T) {
	list := New(1)

	list.insert([]byte("foo"), []byte("bar"))

	assert.Panics(t, func() {
		list.insert([]byte("foo"), []byte("bar"))
	})
}

func TestSkipList_UpdateNonExistentKey(t *testing.T) {
	list := New(1)

	assert.Panics(t, func() {
		list.update([]byte("foo"), []byte("bar"))
	})
}

func assertSkipListValue(t *testing.T, list *SkipList, key string, value string) {
	ok, actual := list.Get([]byte(key))

	assert.True(t, ok)
	assert.Equal(t, []byte(value), actual)
}
