package memtable

import (
	"math/rand"
	"sync"
	"time"
)

type Memtable struct {
	skiplist *SkipList
	mu       sync.RWMutex
}

func NewMemtable() *Memtable {
	return &Memtable{
		skiplist: newSkipList(),
	}
}

func (m *Memtable) Put(key string, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skiplist.insert(key, value)
}

func (m *Memtable) Get(key string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.skiplist.search(key)
}

func (m *Memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.skiplist.insert(key, "DEL")
}

type SkipListNode struct {
	Key       string
	Value     string
	Tombstone bool
	Down      *SkipListNode
	Right     *SkipListNode
}

type SkipList struct {
	Head  *SkipListNode
	Level int
}

func newSkipList() *SkipList {
	return &SkipList{
		Head:  &SkipListNode{Key: "", Value: "", Right: nil, Down: nil},
		Level: 1,
	}
}

func (s *SkipList) search(key string) (*SkipListNode, bool) {
	if s == nil {
		return nil, false
	}

	currList := s.Head
	for currList != nil {

		for currList.Right != nil && currList.Right.Key < key {
			currList = currList.Right
		}

		if currList.Right != nil && currList.Right.Key == key {
			if currList.Right.Tombstone {
				return nil, false
			}
			return currList.Right, true
		}
		currList = currList.Down
	}
	return nil, false
}

func (s *SkipList) insert(key string, value string) {

	var stack []*SkipListNode
	curr := s.Head
	var existingNode *SkipListNode
	isFound := false

	for curr != nil {

		for curr.Right != nil && curr.Right.Key < key {
			curr = curr.Right
		}

		if curr.Right != nil && curr.Right.Key == key {
			existingNode = curr.Right
			isFound = true
		}

		stack = append(stack, curr)

		curr = curr.Down
	}

	if isFound {
		if value == "DEL" {
			existingNode.Tombstone = true
		} else {
			existingNode.Value = value
			existingNode.Tombstone = false
		}
		return
	}

	nextLevelNode := (*SkipListNode)(nil)
	isInserted := true
	level := 0

	rand.Seed(time.Now().UnixNano())

	for isInserted && level < len(stack) {
		prev := stack[len(stack)-1-level]
		stack = stack[:len(stack)-1]

		newNode := &SkipListNode{
			Key:   key,
			Value: value,
			Right: prev.Right,
			Down:  nextLevelNode,
		}

		prev.Right = newNode
		nextLevelNode = newNode

		if rand.Intn(2) == 1 {
			isInserted = true
		} else {
			isInserted = false
		}

		level++
	}

	if isInserted {
		newHead := &SkipListNode{Right: nil, Down: s.Head}
		s.Head = newHead
		s.Level++
	}
}

func (s *SkipList) getLastLevelList() *SkipListNode {
	curr := s.Head
	for curr.Down != nil {
		curr = curr.Down
	}
	return curr
}

func (m *Memtable) FlushToSSTable() {
	m.mu.Lock()
	defer m.mu.Unlock()

	it := m.skiplist.getLastLevelList()

	for it.Right != nil {
		key := it.Key
		value := it.Value

		if value != "DEL" {
			writeToSSTable(key, value)
		}
	}

	m.skiplist = newSkipList()
}
