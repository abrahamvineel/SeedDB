package memtable

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	FILEPATH           = "sstable_"
	BLOOM_FILTER_SIZE  = 2048
	HASH_FUNCTION_SIZE = 8
)

type LSMTree struct {
	memtable     Memtable
	sstables     []*SSTable
	memtableSize int
}

type Memtable struct {
	skiplist *SkipList
	mu       sync.RWMutex
}

type SSTable struct {
	file        *os.File
	index       map[string]uint64
	bloomFilter *BloomFilter
}

type BloomFilter struct {
	bitset       []bool
	size         uint64
	hashFunction []func(string) uint64
}

type SSTableHeader struct {
	MagicNumber       uint32
	DataBlockOffset   uint64
	IndexBlockOffset  uint64
	BloomFilterOffset uint64
}

type DataBlockEntry struct {
	KeyLength   uint64
	Key         string
	ValueLength uint64
	Value       string
}

type IndexBlockEntry struct {
	KeyLength uint64
	Key       string
	Offset    uint64
}

func NewLSMTree(size int) *LSMTree {
	return &LSMTree{
		memtable:     *NewMemtable(),
		sstables:     []*SSTable{},
		memtableSize: size,
	}
}

func (lsm *LSMTree) Put(key, value string) {

	lsm.memtable.Put(key, value)

	if lsm.memtable.getSize() >= lsm.memtableSize {
		sstable := &SSTable{}
		lsm.memtable.FlushToSSTable(sstable)

		bf := NewBloomFilter(BLOOM_FILTER_SIZE)

		for k := range sstable.index {
			bf.Insert(k)
		}

		sstable.bloomFilter = bf
		bf.SaveBloomFilterToFile(sstable)

		lsm.sstables = append(lsm.sstables, lsm.sstables...)
	}
}

func (lsm *LSMTree) Get(key string) (string, bool) {

	node, found := lsm.memtable.skiplist.search(key)
	if found {
		if node.Tombstone {
			return "", false
		}
		return node.Value, true
	}

	for _, sstable := range lsm.sstables {
		value, found := sstable.Lookup(key)
		if found {
			return value, true
		}
	}
	return "", false
}

func (lsm *LSMTree) Delete(key string) {
	lsm.memtable.Delete(key)
}

func (m *Memtable) getSize() int {
	return m.skiplist.getSize()
}

func (s *SkipList) getSize() int {
	curr := s.getLastLevelList()
	count := 0

	for curr.Right != nil {
		count++
		curr = curr.Right
	}
	return count
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

func (m *Memtable) FlushToSSTable(s *SSTable) {
	m.mu.Lock()
	defer m.mu.Unlock()

	it := m.skiplist.getLastLevelList()

	var dataBlockEntries []*DataBlockEntry

	for it.Right != nil {
		key := it.Key
		value := it.Value
		keyLen := uint64(len(key))
		valueLen := uint64(len(value))

		if value != "DEL" {
			dataBlockEntries = append(dataBlockEntries,
				&DataBlockEntry{KeyLength: keyLen, Key: key, ValueLength: valueLen, Value: value})
		}
	}

	s.WriteToSSTable(dataBlockEntries)

	m.skiplist = newSkipList()
}

func (s *SSTable) WriteToSSTable(datablockEntry []*DataBlockEntry) error {

	file, err := os.Create(FILEPATH + time.Now().Format(time.RFC3339) + ".db")
	if err != nil {
		return err
	}

	defer file.Close()

	var indexBlockEntries []IndexBlockEntry
	dataBlockOffset := uint64(16)

	for _, entry := range datablockEntry {
		offset := dataBlockOffset

		indexBlockEntries = append(indexBlockEntries, IndexBlockEntry{KeyLength: entry.KeyLength, Key: entry.Key, Offset: offset})

		keyLen := entry.KeyLength
		valueLen := entry.ValueLength

		binary.Write(file, binary.LittleEndian, keyLen)
		file.WriteString(entry.Key)
		binary.Write(file, binary.LittleEndian, valueLen)
		file.WriteString(entry.Value)

		dataBlockOffset += 4 + keyLen + valueLen
	}

	indexBlockOffset := dataBlockOffset
	var bloomFilterOffset uint64
	var indexBlockSize uint64 = 0

	for _, entry := range indexBlockEntries {
		keyLen := entry.KeyLength

		binary.Write(file, binary.LittleEndian, entry.KeyLength)
		file.WriteString(entry.Key)
		binary.Write(file, binary.LittleEndian, entry.Offset)

		indexBlockSize = 8 + keyLen + 8
	}

	bloomFilterOffset = dataBlockOffset + indexBlockSize

	file.Seek(0, 0)
	header := SSTableHeader{
		MagicNumber:       123123,
		DataBlockOffset:   12,
		IndexBlockOffset:  indexBlockOffset,
		BloomFilterOffset: bloomFilterOffset,
	}

	s.file = file

	binary.Write(file, binary.LittleEndian, header)
	return nil
}

func (s *SSTable) LoadSSTable(filename string) {

	//need to add s.file.open
	file, err := os.Open(filename)
	if err != nil {
		return
	}

	s.file = file
	defer file.Close()

	header := SSTableHeader{}
	binary.Read(file, binary.LittleEndian, &header)
	indexBlockOffset := header.IndexBlockOffset
	file.Seek(int64(indexBlockOffset), 0)

	bf := &BloomFilter{
		bitset:       make([]bool, BLOOM_FILTER_SIZE),
		size:         BLOOM_FILTER_SIZE,
		hashFunction: make([]func(string) uint64, HASH_FUNCTION_SIZE),
	}

	s.bloomFilter = bf

	s.index = make(map[string]uint64)
	for {
		var entry IndexBlockEntry

		err = binary.Read(file, binary.LittleEndian, &entry.KeyLength)
		if err != nil {
			break
		}

		keyBytes := make([]byte, entry.KeyLength)
		_, err = file.Read(keyBytes)
		if err != nil {
			break
		}
		entry.Key = string(keyBytes)

		err = binary.Read(file, binary.LittleEndian, &entry.Offset)
		if err != nil {
			break
		}
		s.index[entry.Key] = entry.Offset
	}
}

func (s *SSTable) Lookup(key string) (string, bool) {

	if !s.bloomFilter.MightContain(key) {
		return "", false
	}

	offset, exists := s.index[key]
	if !exists {
		return "", false
	}

	return s.readDataBlock(offset)
}

func (s *SSTable) readDataBlock(offset uint64) (string, bool) {
	s.file.Seek(int64(offset), 0)

	var keyLen uint64
	var valueLen uint64
	binary.Read(s.file, binary.LittleEndian, &keyLen)
	keyBytes := make([]byte, keyLen)
	s.file.Read(keyBytes)

	binary.Read(s.file, binary.LittleEndian, &valueLen)
	valueBytes := make([]byte, valueLen)
	s.file.Read(valueBytes)

	return string(valueBytes), true

}

func NewBloomFilter(size uint64) *BloomFilter {
	bf := &BloomFilter{
		bitset:       make([]bool, size),
		size:         size,
		hashFunction: make([]func(string) uint64, HASH_FUNCTION_SIZE),
	}

	for i := uint64(0); i < HASH_FUNCTION_SIZE; i++ {
		bf.hashFunction[i] = newHashFunction(i)
	}

	return bf
}

func newHashFunction(seed uint64) func(string) uint64 {
	return func(key string) uint64 {
		h := fnv.New64a()
		h.Write([]byte(key))
		return (h.Sum64() + seed) % seed
	}
}

func (bf *BloomFilter) Insert(key string) {
	for _, hashFunc := range bf.hashFunction {
		index := hashFunc(key) % bf.size
		bf.bitset[index] = true
	}
}

func (bf *BloomFilter) MightContain(key string) bool {
	for _, hashFunc := range bf.hashFunction {
		index := hashFunc(key) % bf.size
		if !bf.bitset[index] {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) SaveBloomFilterToFile(sst *SSTable) error {

	sst.file.Seek(0, 0)

	header := SSTableHeader{}
	binary.Read(sst.file, binary.LittleEndian, &header)
	sst.file.Seek(int64(header.BloomFilterOffset), 0)

	for _, bit := range bf.bitset {
		var b byte
		if bit {
			b = 1
		} else {
			b = 0
		}

		//need to optimize
		err := binary.Write(sst.file, binary.LittleEndian, b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bf *BloomFilter) LoadBloomFilterFromFile(sst *SSTable) error {
	sst.file.Seek(0, 0)

	sstHeader := SSTableHeader{}
	binary.Read(sst.file, binary.LittleEndian, &sstHeader)
	sst.file.Seek(int64(sstHeader.BloomFilterOffset), 0)

	for i := range bf.bitset {
		var b byte
		if err := binary.Read(sst.file, binary.LittleEndian, &b); err != nil {
			return err
		}
		bf.bitset[i] = (b == 1)
	}
	return nil
}
