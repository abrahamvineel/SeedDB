package belldb

import (
	"bufio"
	"fmt"
	"os"

	"github.com/vmihailenco/msgpack"
)

type BellDB struct {
	bellDB map[string]string
}

func GetInstance() *BellDB {
	return &BellDB{bellDB: make(map[string]string)}
}

func (bellDB *BellDB) Put(key string, value string) {
	bellDB.bellDB[key] = value
}

func (bellDB *BellDB) Get(key string) (string, bool) {
	value, ok := bellDB.bellDB[key]
	return value, ok
}

func (bellDB *BellDB) Delete(key string) {
	delete(bellDB.bellDB, key)
}

type DBRecord struct {
	KeyLength   uint32
	Key         string
	ValueLength uint32
	Value       string
}

func (bellDB *BellDB) Save(filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return err
	}

	defer file.Close()

	for key, value := range bellDB.bellDB {

		record := DBRecord{
			KeyLength:   uint32(len(key)),
			Key:         key,
			ValueLength: uint32(len(value)),
			Value:       value,
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
