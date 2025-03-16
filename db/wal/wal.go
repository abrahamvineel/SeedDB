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
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/bwmarrin/snowflake"
	"github.com/vmihailenco/msgpack/v5"
)

type LogRecord struct {
	LogSequenceNumber uint64
	Data              []byte
	CRC               uint32
}

// type LogRecord struct {
// 	LogSequenceNumber uint64
// 	TransactionId     uint64
// 	Operation         byte
// 	Key               []byte
// 	Value             []byte
// 	Timestamp         int64
// 	CRC               uint32
// }

type WAL struct {
	file                  *os.File
	LastLogSequenceNumber uint64
	mu                    sync.Mutex
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
	Table map[int]*TransactionTableRecord
	file  *os.File
}

type TTStatus byte

const (
	BEGIN          TTStatus = 1
	COMMIT         TTStatus = 2
	CHECKPOINT     TTStatus = 3
	ROLLBACK       TTStatus = 4
	ABORT          TTStatus = 5
	INVALID_STATUS TTStatus = 4
)

type Operation byte

const (
	INSERT            Operation = 1
	UPDATE            Operation = 2
	DELETE            Operation = 3
	INVALID_OPERATION Operation = 4
)

func (wal *WAL) createLogRecord(operation int, value string) (*LogRecord, error) {
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

func operationMapper(operation int) (Operation, error) {
	if operation == 1 {
		return INSERT, nil
	} else if operation == 2 {
		return UPDATE, nil
	} else if operation == 3 {
		return DELETE, nil
	}
	return INVALID_OPERATION, errors.New("invalid operation")
}

func statusMapper(status uint8) (TTStatus, error) {
	if status == 1 {
		return BEGIN, nil
	} else if status == 2 {
		return COMMIT, nil
	} else if status == 3 {
		return CHECKPOINT, nil
	} else if status == 4 {
		return ROLLBACK, nil
	} else if status == 5 {
		return ABORT, nil
	}
	return INVALID_STATUS, errors.New("invalid status")
}

func (wal *WAL) sequentialWrite(record *LogRecord) (*WAL, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

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
	wal.mu.Lock()
	defer wal.mu.Unlock()

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

func (tt *TransactionTable) transactionTableInsert(cpRecord *CheckPointLogRecord) {

	tt.mu.Lock()
	defer tt.mu.Unlock()

	//1 is machine id
	node, err := snowflake.NewNode(1)
	if err != nil {
		return
	}

	txnId := node.Generate()

	tt.Table[txnId] = &TransactionTableRecord{
		TransactionId: txnId,
		CheckpointLSN: cpRecord.CheckpointLSN,
		Status:        BEGIN,
	}

	record := fmt.Sprintf("%d|%d|%d\n", txnId, cpRecord.CheckpointLSN, BEGIN)

	tt.file.WriteString(record)
	tt.file.Sync()
}

func createTransactionTable(filename string) (*TransactionTable, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		return nil, err
	}

	return &TransactionTable{
		Table: make(map[int]*TransactionTableRecord),
		file:  file,
	}, nil
}

func (tt *TransactionTable) loadTransactionTable() map[int]*TransactionTableRecord {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	scanner := bufio.NewScanner(tt.file)

	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "|")
		txnId, _ := strconv.Atoi(parts[0])
		status, _ := strconv.ParseInt(parts[2], 10, 8)

		mappedStatus, _ := statusMapper(uint8(status))
		record := &TransactionTableRecord{
			TransactionId: uint64(txnId),
			Status:        mappedStatus,
		}
		tt.Table[txnId] = record
	}
	return tt.Table
}

func (tt *TransactionTable) commitTransactions() error {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	var lastRecord TransactionTableRecord

	for txnId := range tt.Table {
		record := tt.Table[txnId]
		record.Status = COMMIT
		tt.Table[txnId] = record
		lastRecord = *record
	}

	err := tt.file.Truncate(0)
	if err != nil {
		return fmt.Errorf("Failed to truncate the file")
	}

	tt.file.Seek(0, 0)

	record := fmt.Sprintf("%d|%d|%d\n", lastRecord.TransactionId, lastRecord.CheckpointLSN, lastRecord.Status)
	tt.file.WriteString(record)

	return nil
}

// dirty page table

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
