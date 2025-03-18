// memetable init
package memtable

type SkipList struct {
	Value uint64
	Down  *SkipList
	Left  *SkipList
}
