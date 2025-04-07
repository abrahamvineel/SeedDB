/*Stage 1: Build a basic in-memory key-value store with simple CRUD operations.
Stage 2: Add persistence to disk (e.g., using a write-ahead log or appending to a file).
Stage 3: Add concurrency using goroutines, locks, and channels.
Stage 4: Implement an LSM Tree-based or B-Tree-based on-disk storage model.
Stage 5: Scale out by implementing sharding or replication across nodes.

wal, memtable, sstable, lsm trees

need to implement inserting values to dat file format for sstables*/

package main

import (
	"fmt"
	"kv_store/db/belldb"
)

func main() {

	bellDB := belldb.GetInstance()

	bellDB.Put("name1", "hello1")
	bellDB.Put("name2", "hello2")
	bellDB.Put("name3", "hello3")

	// fmt.Println(kvstore.kvstore)

	fmt.Println(bellDB.Get("name3"))

	bellDB.Save("test.db")

	// bellDB.Read("test.db")
}
