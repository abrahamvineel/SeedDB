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
		if skipList.Key == key {
			return skipList.Value
		}
		if skipList.Key < key {
			skipList = skipList.Right
		} else if skipList.Key > key {
			skipList = skipList.Left
		} else {
			skipList = skipList.Down
		}
	}
	return ""
}
