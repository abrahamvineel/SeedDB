/*Stage 1: Build a basic in-memory key-value store with simple CRUD operations.
Stage 2: Add persistence to disk (e.g., using a write-ahead log or appending to a file).
Stage 3: Add concurrency using goroutines, locks, and channels.
Stage 4: Implement an LSM Tree-based or B-Tree-based on-disk storage model.
Stage 5: Scale out by implementing sharding or replication across nodes.

wal, memtable, sstable, lsm trees

need to implement inserting values to dat file format for sstables*/

package main

import (
	"kv_store/db/belldb"
)

// type BellDB struct {
// 	bellDB map[string]string
// }

// func getInstance() *BellDB {
// 	return &BellDB{bellDB: make(map[string]string)}
// }

// func (bellDB *BellDB) Put(key string, value string) {
// 	bellDB.bellDB[key] = value
// }

// func (bellDB *BellDB) Get(key string) (string, bool) {
// 	value, ok := bellDB.bellDB[key]
// 	return value, ok
// }

// func (bellDB *BellDB) Delete(key string) {
// 	delete(bellDB.bellDB, key)
// }

// func (bellDB *BellDB) Save(filename string) error {
// 	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

// 	if err != nil {
// 		return err
// 	}

// 	defer file.Close()

// 	for key, value := range kvstore.kvstore {
// 		_, err := file.WriteString(fmt.Sprintf("%s:%s\n", key, value))

// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (kvstore *KeyValueStore) Read(filename string) error {
// 	file, err := os.Open(filename)

// 	if err != nil {
// 		return err
// 	}

// 	scanner := bufio.NewScanner(file)

// 	for scanner.Scan() {
// 		line := scanner.Text()
// 		words := strings.Split(line, ":")

// 		if len(words) == 2 {
// 			fmt.Println(words)
// 			kvstore.Put(words[0], words[1])
// 		}
// 	}
// 	return scanner.Err()
// }

func main() {

	bellDB := belldb.GetInstance()

	bellDB.Put("name1", "hello1")
	bellDB.Put("name2", "hello2")
	bellDB.Put("name3", "hello3")

	// fmt.Println(kvstore.kvstore)

	// fmt.Println(kvstore.Get("name3"))

	bellDB.Save("test.db")

	bellDB.Read("test.db")

	// kvstore.Read("test.dat")

	// fmt.Println(kvstore.kvstore)
}
