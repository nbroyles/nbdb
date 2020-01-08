package skiplist

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: test locking semantics. also level generation?

func TestSkipList_RoundTrip(t *testing.T) {
	list := New()

	put(t, list, "howdy", "time")
	assertSkipListValue(t, list, "howdy", "time")
}

func TestSkipList_Put(t *testing.T) {
	list := New()

	put(t, list, "a", "lot")
	put(t, list, "of", "keys")
	put(t, list, "into", "this")
	put(t, list, "bad", "boy")
	put(t, list, "!!!!", "!!!!")

	assertSkipListValue(t, list, "a", "lot")
	assertSkipListValue(t, list, "of", "keys")
	assertSkipListValue(t, list, "into", "this")
	assertSkipListValue(t, list, "bad", "boy")
	assertSkipListValue(t, list, "!!!!", "!!!!")
}

func TestSkipList_Update(t *testing.T) {
	list := New()

	put(t, list, "foo", "bar")
	assertSkipListValue(t, list, "foo", "bar")

	put(t, list, "foo", "baz")
	assertSkipListValue(t, list, "foo", "baz")
}

func TestSkipList_MultipleInserts(t *testing.T) {
	list := New()

	list.insert([]byte("foo"), []byte("bar"))

	assert.Panics(t, func() {
		list.insert([]byte("foo"), []byte("bar"))
	})
}

func TestSkipList_UpdateNonExistentKey(t *testing.T) {
	list := New()

	assert.Panics(t, func() {
		list.update([]byte("foo"), []byte("bar"))
	})
}

func put(t *testing.T, list *SkipList, key string, value string) {
	list.Put([]byte(key), []byte(value))
}

func assertSkipListValue(t *testing.T, list *SkipList, key string, value string) {
	ok, actual := list.Get([]byte(key))

	assert.True(t, ok)
	assert.Equal(t, []byte(value), actual)
}
