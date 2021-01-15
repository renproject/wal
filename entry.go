package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// AppendEntryToFile by writing the length of the entry as a big-endian uint32,
// and then writing the entry. This results in all entries being length
// prefixed, and no explicit entry separator is needed. If sync is true, then
// the write will be immediately synchronized to the disk.
func AppendEntryToFile(f *os.File, entry []byte, sync bool) error {
	if err := binary.Write(f, binary.BigEndian, uint32(len(entry))); err != nil {
		return fmt.Errorf("writing len: %v", err)
	}
	if _, err := f.Write(entry); err != nil {
		return fmt.Errorf("writing entry: %v", err)
	}
	if sync {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("syncing entry: %v", err)
		}
	}
	return nil
}

// NextEntryFromFile reads a big-endian uint32 length prefix, and then reads an
// entry of that length. It accepts a slice to which it may write the entry, and
// it returns a slice that is the entry. The accepted slice will be used
// directly if it has sufficient capacity, otherwise a new slice will be
// allocated and used.
func NextEntryFromFile(f *os.File, entry []byte) ([]byte, error) {
	n := uint32(0)
	if err := binary.Read(f, binary.BigEndian, &n); err != nil {
		if err == io.EOF {
			return entry, err
		}
		return []byte{}, fmt.Errorf("reading len: %v", err)
	}

	// Grow the entry if it does not have enough capacity. We do not need to
	// copy the contents of the entry, because we are about to clobber the
	// contents by reading from the file.
	if int(n) > cap(entry) {
		entry = make([]byte, n, n)
	}
	entry = entry[:n]

	m, err := f.Read(entry)
	if err != nil {
		if err == io.EOF {
			return entry[:m], err
		}
		return entry[:m], fmt.Errorf("reading entry: %v", err)
	}
	return entry[:m], nil
}
