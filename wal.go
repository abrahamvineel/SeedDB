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

func (wal *WAL) createWAL(filePath string) (*WAL, error) {

	//gen lsn

	currOffset, err := wal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	crc32Hash := crc32.NewIEEE()

	//need to find way to get data
	crc32Hash.Write(data)

	checksum := crc32Hash.Sum32()

	record := &LogRecord{
		LogSequenceNumber: uint64(currOffset),
		Data:              nil,
		CRC:               checksum,
	}

	//serialize the record
	data, err := msgpack.Marshal(record)

	//write to file
}

func (wal *WAL) generateLSN() uint64 {
	return wal.LastLogSequenceNumber
}

func createLogEntry() {}
