package belldb

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"os"
	"sync"

	"kv_store/db/lsmtree"
	"kv_store/db/wal"

	"github.com/vmihailenco/msgpack"
)

type BellDB struct {
	bellDB  map[string]string
	wal     *wal.WAL
	lsmTree *lsmtree.LSMTree
	mu      sync.Mutex
}

func GetInstance() *BellDB {
	return &BellDB{
		bellDB:  make(map[string]string),
		wal:     wal.NewWAL("wal.log"),
		lsmTree: lsmtree.NewLSMTree(2048),
	}
}

func (bellDB *BellDB) Put(key string, value string) {
	bellDB.mu.Lock()
	defer bellDB.mu.Unlock()

	logRecord, _ := bellDB.wal.CreateLogRecord(1, key, value)
	bellDB.wal.SequentialWrite(logRecord)
	bellDB.lsmTree.Put(key, value)
	bellDB.bellDB[key] = value
}

func (bellDB *BellDB) Get(key string) (string, bool) {
	value, found := bellDB.lsmTree.Get(key)

	if found {
		return value, found
	}
	fmt.Print("value")
	value, ok := bellDB.bellDB[key]
	return value, ok
}

func (bellDB *BellDB) Delete(key string) {
	bellDB.lsmTree.Delete(key)
	bellDB.wal.CreateLogRecord(3, key, "")
	delete(bellDB.bellDB, key)
}

type DBRecord struct {
	KeyLength   uint32
	Key         string
	ValueLength uint32
	Value       string
	CRC         uint32
}

func (bellDB *BellDB) Save(filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return err
	}

	defer file.Close()

	for key, value := range bellDB.bellDB {

		data := []byte(key + value)
		checksum := crc32.ChecksumIEEE(data)

		record := DBRecord{
			KeyLength:   uint32(len(key)),
			Key:         key,
			ValueLength: uint32(len(value)),
			Value:       value,
			CRC:         checksum,
		}

		serializeRec, serErr := msgpack.Marshal(record)
		serializeRec = append(serializeRec, '\n')

		if serErr != nil {
			return serErr
		}

		_, recErr := file.Write(serializeRec)

		if recErr != nil {
			return recErr
		}
	}
	return nil
}

func (bellDB *BellDB) Read(filename string) error {
	file, err := os.Open(filename)

	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		serializedData := scanner.Bytes()
		var record DBRecord
		err = msgpack.Unmarshal(serializedData, &record)

		if err != nil {
			fmt.Println("Deserialization error", err)
			continue
		}

		fmt.Printf("Deserialized Record: Key = %s, Value = %s\n", record.Key, record.Value)
	}
	return scanner.Err()
}
