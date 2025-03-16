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
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/vmihailenco/msgpack/v5"
)

type LogRecord struct {
	LogSequenceNumber uint64
	TransactionId     uint64
	Operation         byte
	KeyLength         uint32
	Key               string
	ValueLength       uint32
	Value             string
	Timestamp         int64
	CRC               uint32
}

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

type Operation byte

const (
	INSERT            Operation = 1
	UPDATE            Operation = 2
	DELETE            Operation = 3
	INVALID_OPERATION Operation = 4
)

func (wal *WAL) createLogRecord(operation int, key string, value string) (*LogRecord, error) {
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

	// valueLen := uint32(len(value))
	// binary.Write(&buffer, binary.LittleEndian, valueLen)
	// buffer.Write([]byte(value))

	data := []byte(key + value)

	//calculate checksum
	checksum := crc32.ChecksumIEEE(data)

	node, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}
	txnId := node.Generate()

	record := &LogRecord{
		LogSequenceNumber: uint64(currOffset),
		TransactionId:     txnId,
		Operation:         byte(operation),
		KeyLength:         uint32(len(key)),
		Key:               key,
		ValueLength:       uint32(len(value)),
		Value:             value,
		Timestamp:         time.Now().UnixMilli(),
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
