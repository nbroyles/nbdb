package skiplist

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"

	"github.com/nbroyles/nbdb/internal/storage"
)

// TODO: make configurable?
const maxLevels = 32

// Node represents a node in the SkipList structure
type Node struct {
	next    []*Node
	key     []byte
	value   []byte
	deleted bool
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

var _ storage.InMemoryStore = &SkipList{}

func New(seed int64) *SkipList {
	rand.Seed(seed)

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
	for i := s.levels - 1; i >= 0; i-- {
	rightTraversal:
		for ; c.next[i] != nil; c = c.next[i] {
			switch bytes.Compare(c.next[i].key, key) {
			case 0:
				if c.next[i].deleted {
					return false, nil
				} else {
					return true, c.next[i].value
				}
			case 1: // next key is greater than the key we're searching for
				break rightTraversal
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

// Removes the specified key from the skip list. Returns true if
// key was removed and false if key was not present
func (s *SkipList) Delete(key []byte) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	c := s.head
	removed := false
	for i := s.levels - 1; i >= 0; i-- {
		for ; c.next[i] != nil; c = c.next[i] {
			if bytes.Equal(c.next[i].key, key) && !c.next[i].deleted {
				c.next[i].deleted = true
				removed = true
				break
			}
		}
	}

	return removed
}

func (s *SkipList) update(key []byte, value []byte) {
	c := s.head
	updated := false
	for i := s.levels - 1; i >= 0; i-- {
		for ; c.next[i] != nil; c = c.next[i] {
			if bytes.Equal(c.next[i].key, key) {
				c.next[i].value = value
				c.next[i].deleted = false
				updated = true
				break
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

	newNode := &Node{next: make([]*Node, levels), key: key, value: value, deleted: false}

	c := s.head
	for i := s.levels - 1; i >= 0; i-- {
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

// Print prints skip list in a pretty format. Should only be used for debugging
// Not particularly efficient. Would not recommend on larger lists
// TODO: represent deleted keys
func (s *SkipList) Print() {
	keysLoc := map[string]int{}
	idx := 1
	for node := s.head.next[0]; node != nil; node = node.next[0] {
		keysLoc[string(node.key)] = idx
		idx++
	}
	nodeWidth := 10

	for i := s.levels - 1; i >= 0; i-- {
		s.printNodeBorder(i, keysLoc, nodeWidth)
		fmt.Println()
		s.printNode(i, keysLoc, nodeWidth)
		fmt.Println()
		s.printNodeBorder(i, keysLoc, nodeWidth)
		fmt.Println()
	}
}

func (s *SkipList) printNodeBorder(i int, keysLoc map[string]int, nodeWidth int) {
	nextSlot := 1
	for node := s.head.next[i]; node != nil; node = node.next[i] {
		loc := keysLoc[string(node.key)]

		for nextSlot != loc {
			fmt.Printf(fmt.Sprint("%", nodeWidth, "s"), strings.Repeat(" ", nodeWidth))
			fmt.Print(" ")
			nextSlot++
		}

		fmt.Printf(fmt.Sprint("%", nodeWidth, "s"), strings.Repeat("-", nodeWidth))
		fmt.Print(" ")
		nextSlot++
	}
}

func (s *SkipList) printNode(i int, keysLoc map[string]int, nodeWidth int) {
	nextSlot := 1
	for node := s.head.next[i]; node != nil; node = node.next[i] {
		loc := keysLoc[string(node.key)]

		keySize := 4
		key := string(node.key)
		if len(key) > keySize {
			key = key[0:keySize]
		} else if len(key) < keySize {
			key = fmt.Sprintf(fmt.Sprint("%-", keySize, "s"), key)
		}

		for nextSlot != loc {
			fmt.Printf(fmt.Sprint("%", nodeWidth, "s"), strings.Repeat(" ", nodeWidth))
			fmt.Print(" ")
			nextSlot++
		}

		fmt.Printf(fmt.Sprint("%", nodeWidth, "s"), "|   "+key+" |")
		fmt.Print(" ")
		nextSlot++
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
