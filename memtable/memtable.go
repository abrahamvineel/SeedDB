// memetable init
package memtable

type SkipListNode struct {
	Key   string
	Value string
	Down  *SkipListNode
	Right *SkipListNode
}

type SkipList struct {
	Head  *SkipListNode
	Level int
}

func NewSkipList() *SkipList {
	return &SkipList{
		Head:  &SkipListNode{Key: "", Value: "", Right: nil, Down: nil},
		Level: 1,
	}
}

func (s *SkipList) Search(key string) (*SkipListNode, bool) {
	if s == nil {
		return nil, false
	}

	currList := s.Head
	for currList != nil {

		for currList.Right != nil && currList.Right.Value <= key {
			currList = currList.Right
		}

		if currList.Right != nil && currList.Key == key {
			return currList.Right, true
		}
		currList = currList.Down
	}
	return nil, false
}

func insert(key string, value string) {
	skipList := Search(key)

}

func delete() {

}
