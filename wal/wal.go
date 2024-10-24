//implement wal

// Essential Features:
// Durability
// Atomicity
// Sequential Logging
// Crash Recovery
// Checkpointing
// Efficient Log Truncation
// Log Synchronization
// Concurrency and Thread Safety
// Optional (Advanced) Features:
// 9. Log Compaction
// 10. Fault Tolerance
// 11. Batching Writes
// 12. Versioning or Log Metadata
// 13. Support for Distributed Systems
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/vmihailenco/msgpack/v5"
)

type LogRecord struct {
	LogSequenceNumber uint64
	Data              []byte
	CRC               uint32
}

type WAL struct {
	file                  *os.File
	LastLogSequenceNumber uint64
}

const (
	insert byte = 1
	update byte = 2
	delete byte = 3
)

func (wal *WAL) createWAL(operation byte, value string) (*WAL, error) {

	//generate lsn
	currOffset, err := wal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	var buffer bytes.Buffer

	buffer.WriteByte(operation)

	valueLen := uint32(len(value))
	binary.Write(&buffer, binary.LittleEndian, valueLen)
	buffer.Write([]byte(value))

	data := buffer.Bytes()

	//calculate checksum
	crc32Hash := crc32.NewIEEE()
	crc32Hash.Write(data)
	checksum := crc32Hash.Sum32()

	record := &LogRecord{
		LogSequenceNumber: uint64(currOffset),
		Data:              data,
		CRC:               checksum,
	}

	//serialize the record
	serializeRec, err := msgpack.Marshal(record)

	//write to file
	//need to write using file sync

	wal.file.WriteString(string(serializeRec))

	if err := wal.file.Sync(); err != nil {
		return nil, err
	}

	return wal, err
}

func ReadLogRecord() {
	//deserialize

	//break the record based on record length

	//check crc?

}

func NewWAL(filePath string) (*WAL, error) {

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:                  file,
		LastLogSequenceNumber: 0,
	}

	return wal, nil
}

func main() {
	wal, err := NewWAL("test.txt")

	if err != nil {
		fmt.Println("error in creating wal", err)
		return
	}

	wal.createWAL(1, "hello:world")

	defer wal.file.Close()
}

//checkpointing needs to be implemented separately
