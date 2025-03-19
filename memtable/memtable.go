// memetable init
package memtable

type SkipList struct {
	Key   string
	Value string
	Down  *SkipList
	Left  *SkipList
	Right *SkipList
}

func (skipList *SkipList) search(key string) (string, bool) {
	if skipList == nil {
		return "", false
	}

	currList := skipList
	for currList != nil {

		for currList.Right != nil && currList.Right.Value <= key {
			currList = currList.Right
		}

		if currList.Key == key {
			return currList.Value, true
		}
		currList = currList.Down
	}
	return "", false
}
