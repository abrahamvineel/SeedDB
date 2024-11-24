/*Stage 1: Build a basic in-memory key-value store with simple CRUD operations.
Stage 2: Add persistence to disk (e.g., using a write-ahead log or appending to a file).
Stage 3: Add concurrency using goroutines, locks, and channels.
Stage 4: Implement an LSM Tree-based or B-Tree-based on-disk storage model.
Stage 5: Scale out by implementing sharding or replication across nodes. */

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

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

func (kvstore *KeyValueStore) Save(filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return err
	}

	defer file.Close()

	for key, value := range kvstore.kvstore {
		_, err := file.WriteString(fmt.Sprintf("%s:%s\n", key, value))


		if err != nil {
			return err
		}
	}
	return nil
}

func (kvstore *KeyValueStore) Read(filename string) error {
	file, err := os.Open(filename)

	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, ":")

		if len(words) == 2 {
			fmt.Println(words)
			kvstore.Put(words[0], words[1])
		}
	}
	return scanner.Err()
}

func main() {
	kvstore := NewKeyValueStore()

	kvstore.Put("name1", "hello1")
	kvstore.Put("name2", "hello2")
	kvstore.Put("name3", "hello3")

	fmt.Println(kvstore.kvstore)

	fmt.Println(kvstore.Get("name3"))

	fmt.Println(kvstore.Save("test.txt"))

	kvstore.Read("test.txt")

	fmt.Println(kvstore.kvstore)
}
