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
// Optional (Advanced) Features: 9. Log Compaction 10. Fault Tolerance 11. Batching Writes 12. Versioning or Log Metadata 13. Support for Distributed Systems
package main

import (
	"bytes"
	"encoding/binary"
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

func (wal *WAL) createWAL(filePath string, operation byte, key string, value string) (*WAL, error) {

	//gen lsn

	currOffset, err := wal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	var buffer bytes.Buffer

	buffer.WriteByte(operation)

	keyLen := uint32(len(key))
	binary.Write(&buffer, binary.LittleEndian, keyLen)
	buffer.Write([]byte(key))

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
	wal.file.WriteString(string(serializeRec))
	return wal, err
}

func (wal *WAL) generateLSN() uint64 {
	return wal.LastLogSequenceNumber
}

func createLogEntry() {}
