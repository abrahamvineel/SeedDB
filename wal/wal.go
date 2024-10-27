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

type CheckPointLogRecord struct {
	LogSequenceNumber uint64
	CheckpointLSN     uint64
	CRC               uint32
}

const (
	insert byte = 1
	update byte = 2
	delete byte = 3
)

func (wal *WAL) createLogRecord(operation byte, value string) (*LogRecord, error) {
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

	return record, nil
}

func (wal *WAL) sequentialWrite(record *LogRecord) (*WAL, error) {

	//serialize the record
	serializeRec, err := msgpack.Marshal(record)
	if err != nil {
		return nil, err
	}

	wal.file.WriteString(string(serializeRec))
	if err := wal.file.Sync(); err != nil {
		return nil, err
	}

	return wal, nil
}

func (wal *WAL) batchWrite(logRecords []LogRecord) error {
	for _, record := range logRecords {

		serializeRecord, err := msgpack.Marshal(record)
		if err != nil {
			return err
		}

		if _, err := wal.file.Write(serializeRecord); err != nil {
			return err
		}
	}

	if err := wal.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (wal *WAL) createCheckpoint(logRecord *LogRecord, filePath string) error {

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return err
	}

	checkpointLSN := wal.LastLogSequenceNumber

	crc32Hash := crc32.NewIEEE()
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, checkpointLSN)
	crc32Hash.Write(buf)
	checksum := crc32Hash.Sum32()

	checkpointRecord := CheckPointLogRecord{
		LogSequenceNumber: checkpointLSN,
		CheckpointLSN:     checkpointLSN,
		CRC:               checksum,
	}

	serializedData, err := msgpack.Marshal(checkpointRecord)
	if err != nil {
		return err
	}

	if _, err := file.Write(serializedData); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}
	return nil
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

	logRecord, _ := wal.createLogRecord(1, "hello:world")
	wal.sequentialWrite(logRecord)

	defer wal.file.Close()
}
