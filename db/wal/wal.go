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
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/bwmarrin/snowflake"
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

type TransactionTableRecord struct {
	TransactionId uint64
	CheckpointLSN uint64
	Status        TTStatus
}

type TransactionTable struct {
	mu    sync.Mutex
	Table map[int64]*TransactionTableRecord
	file  *os.File
}

type TTStatus byte

const (
	BEGIN      TTStatus = 1
	COMMIT     TTStatus = 2
	CHECKPOINT TTStatus = 3
	ROLLBACK   TTStatus = 4
	ABORT      TTStatus = 5
)

type Operation byte

const (
	INSERT  Operation = 1
	UPDATE  Operation = 2
	DELETE  Operation = 3
	INVALID Operation = 4
)

func (wal *WAL) createLogRecord(operation string, value string) (*LogRecord, error) {
	//generate lsn
	currOffset, err := wal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	var buffer bytes.Buffer
	op, opErr := operationMapper(operation)

	if opErr != nil {
		return nil, opErr
	}

	buffer.WriteByte(byte(op))

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

func operationMapper(operation string) (Operation, error) {
	if operation == "insert" {
		return INSERT, nil
	} else if operation == "update" {
		return UPDATE, nil
	} else if operation == "delete" {
		return DELETE, nil
	}
	return INVALID, errors.New("invalid operation")
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

func (wal *WAL) createCheckpoint(filePath string) error {

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

//crash recover to be implemented

func (cpRecord *CheckPointLogRecord) transactionTableInsert() {

	//1 is machine id
	node, err := snowflake.NewNode(1)
	if err != nil {
		return
	}

	txnId := node.Generate()

	ttRecord := &TransactionTableRecord{
		TransactionId: txnId,
		CheckpointLSN: cpRecord.CheckpointLSN,
		Status:        BEGIN,
	}

}

func createTransactionTable(filename string) (*TransactionTableRecord, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
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

	logRecord, _ := wal.createLogRecord("insert", "hello:world")
	wal.sequentialWrite(logRecord)

	defer wal.file.Close()
}
