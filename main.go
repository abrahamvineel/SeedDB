/*Stage 1: Build a basic in-memory key-value store with simple CRUD operations.
Stage 2: Add persistence to disk (e.g., using a write-ahead log or appending to a file).
Stage 3: Add concurrency using goroutines, locks, and channels.
Stage 4: Implement an LSM Tree-based or B-Tree-based on-disk storage model.
Stage 5: Scale out by implementing sharding or replication across nodes. */

package main

import "fmt"

type KeyValueStore struct {
	kvstore map[string]string
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{kvstore: make(map[string]string)}
}

func (kvstore *KeyValueStore) Put(key string, value string) {
	kvstore.kvstore[key] = value
}

func (keyvaluestore *KeyValueStore) Get(key string) (string, bool) {
	value, ok := keyvaluestore.kvstore[key]
	return value, ok
}

func (keyvaluestore *KeyValueStore) Delete(key string) {
	delete(keyvaluestore.kvstore, key)
}

func main() {
	kvstore := NewKeyValueStore()

	kvstore.Put("name1", "hello1")
	kvstore.Put("name2", "hello2")
	kvstore.Put("name3", "hello3")

	fmt.Println(kvstore.kvstore)

	fmt.Println(kvstore.Get("name3"))

	kvstore.Delete("name2")
	fmt.Println(kvstore.kvstore)

	fmt.Println(kvstore.Get("name4"))
}
