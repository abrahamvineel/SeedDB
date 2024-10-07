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

import "os"

type LogRecord struct {
	LogSequenceNumber uint64
	Data              []byte
	CRC               uint32
}

type WAL struct {
	file                  *os.File
	LastLogSequenceNumber uint64
}

func createWAL(filePath string) (*WAL, error) {

}

func createLogEntry() {}
