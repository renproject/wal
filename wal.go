package wal

import (
	"fmt"
	"os"
)

// WAL implements a simple write-ahead-log that manages two files: a reader and
// a writer. The writer appends entries to the end of the log, and the reader
// iterates over entries from the beginning of the log. The WAL will grow
// indefinitely, and will not automatically delete itself.
type WAL struct {
	writer *os.File
	reader *os.File
	sync   bool
}

// Create a new WAL by opening the filename in write-only and read-only mode. If
// the files do not exist, they will be created. If sync mode is enabled, then
// all writes will result in an immediately sync to stable storage (this
// improves correctness, but at a substantial performance loss).
func New(filename string, sync bool) (*WAL, error) {
	writer, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening writer: %v", err)
	}
	reader, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening reader: %v", err)
	}
	return &WAL{
		writer: writer,
		reader: reader,
		sync:   sync,
	}, nil
}

// Sync all writes.
func (wal *WAL) Sync() error {
	return wal.writer.Sync()
}

// Close both the write-only and read-only files.
func (wal *WAL) Close() error {
	if err := wal.Sync(); err != nil {
		return fmt.Errorf("syncing writer: %v", err)
	}
	if err := wal.writer.Close(); err != nil {
		return fmt.Errorf("closing writer: %v", err)
	}
	if err := wal.reader.Close(); err != nil {
		return fmt.Errorf("closing reader: %v", err)
	}
	return nil
}

// Push a new entry to the end of the log. It is safe to call Push concurrently
// with Pop, but it is not safe to call concurrently with other Pushes.
func (wal *WAL) Push(entry []byte) error {
	return AppendEntryToFile(wal.writer, entry, wal.sync)
}

// Pop an entry from the beginning of the log. This does not delete the entry
// from the file. It is safe to call Pop concurrently with Push, but it
// is not safe to call concurrently with other Pops.
func (wal *WAL) Pop() ([]byte, error) {
	entry := []byte{}
	return NextEntryFromFile(wal.reader, entry[:])
}
