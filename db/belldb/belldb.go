package belldb

import (
	"bufio"
	"fmt"
	"os"
	"strings"

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
		if err != nil {
			return serErr
		}

		_, recErr := file.WriteString(string(serializeRec))

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
		line := scanner.Text()
		words := strings.Split(line, ":")

		if len(words) == 2 {
			fmt.Println(words)
			bellDB.Put(words[0], words[1])
		}
	}
	return scanner.Err()
}
