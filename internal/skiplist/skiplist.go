package skiplist

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"
)

// TODO: make configurable?
const maxLevels = 32

// Node represents a node in the SkipList structure
type Node struct {
	next  []*Node
	key   []byte
	value []byte
}

// SkipList is an implementation of a data structure that provides
// O(log n) insertion and removal without complicated self-balancing logic
// required of similar tree-like structures (e.g. red/black, AVL trees)
// See the following for more details:
//   - https://en.wikipedia.org/wiki/Skip_list
//   - https://igoro.com/archive/skip-lists-are-fascinating/
type SkipList struct {
	// TODO: this level of locking is pretty heavy handed.
	// Investigate lockless skip lists?
	lock   sync.RWMutex
	head   *Node
	levels int
}

func New() *SkipList {
	rand.Seed(time.Now().UnixNano())

	return &SkipList{
		head:   &Node{next: make([]*Node, maxLevels)},
		levels: 1,
	}
}

// Get returns a boolean indicating whether the specified key
// was found in the list. If true, the value is returned as well
func (s *SkipList) Get(key []byte) (bool, []byte) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.get(key)
}

func (s *SkipList) get(key []byte) (bool, []byte) {
	c := s.head
	for i := s.levels; i >= 0; i-- {
		for ; c.next[i] != nil; c = c.next[i] {
			switch bytes.Compare(c.next[i].key, key) {
			case 0:
				return true, c.next[i].value
			case 1: // next key is greater than the key we're searching for
				break
			}
		}
	}

	return false, nil
}

// Put inserts or updates the value if the key already exists
func (s *SkipList) Put(key []byte, value []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if contains, _ := s.get(key); contains {
		s.update(key, value)
	} else {
		s.insert(key, value)
	}
}

func (s *SkipList) update(key []byte, value []byte) {
	c := s.head
	updated := false
	for i := s.levels; i >= 0; i-- {
		for ; c.next[i] != nil; c = c.next[i] {
			if bytes.Equal(c.next[i].key, key) {
				c.next[i].value = value
				updated = true
			}
		}
	}

	if !updated {
		log.Panicf("could not update key %v (%s) even though we expected it to exist!", key, string(key))
	}
}

func (s *SkipList) insert(key []byte, value []byte) {
	levels := s.generateLevels()

	if levels > s.levels {
		s.levels = levels
	}

	newNode := &Node{next: make([]*Node, levels), key: key, value: value}

	c := s.head
	for i := s.levels; i >= 0; i-- {
		for ; c.next[i] != nil; c = c.next[i] {
			// Stop moving rightward at this level if next key is greater
			// than key we plan to insert
			if bytes.Compare(c.next[i].key, key) > 0 {
				break
			} else if bytes.Equal(c.next[i].key, key) {
				log.Panicf("attempting to insert key %v (%s) that already exists. "+
					"this should not happen!", key, string(key))
			}
		}
		if levels > i {
			newNode.next[i] = c.next[i]
			c.next[i] = newNode
		}
	}
}

// Level generation shamelessly stolen from
//https://igoro.com/archive/skip-lists-are-fascinating/
func (s *SkipList) generateLevels() int {
	levels := 0
	for num := rand.Int31(); num&1 == 1; num >>= 1 {
		levels += 1
	}

	if levels == 0 {
		levels = 1
	}

	return levels
}
