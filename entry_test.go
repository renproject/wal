package wal_test

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"os"
	"testing"

	"github.com/renproject/wal"
)

func TestAppendToFileSync(t *testing.T) {
	testAppendToFile(t, true, false)
}

func TestAppendToFileNoSync(t *testing.T) {
	testAppendToFile(t, false, false)
}

func TestAppendToFileSyncOneByOne(t *testing.T) {
	testAppendToFile(t, true, true)
}

func TestAppendToFileNoSyncOneByOne(t *testing.T) {
	testAppendToFile(t, false, true)
}

func BenchmarkAppendToFileSync(b *testing.B) {
	benchmarkAppendToFile(b, true)
}

func BenchmarkAppendToFileNoSync(b *testing.B) {
	benchmarkAppendToFile(b, false)
}

func testAppendToFile(t *testing.T, sync, oneByOne bool) {
	defer os.RemoveAll("testAppendToFile.wal")
	f, err := os.Create("testAppendToFile.wal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	g, err := os.Open(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	n := 100000
	if sync {
		n = 1000
	}
	entries := make([][]byte, n)
	for i := range entries {
		entries[i] = make([]byte, 1024)
		if _, err := rand.Read(entries[i]); err != nil {
			t.Fatal(err)
		}
	}

	entry := []byte{}
	for i := range entries {
		if err := wal.AppendEntryToFile(f, entries[i], sync); err != nil {
			t.Fatal(err)
		}

		if oneByOne {
			if entry, err = wal.NextEntryFromFile(g, entry); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(entry, entries[i]) {
				t.Fatalf("expected %x to equal %x", entry, entries[i])
			}
		}
	}

	if !oneByOne {
		for i := range entries {
			if entry, err = wal.NextEntryFromFile(g, entry); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(entry, entries[i]) {
				t.Fatalf(
					"expected %v to equal %v",
					base64.RawURLEncoding.EncodeToString(entry),
					base64.RawURLEncoding.EncodeToString(entries[i]))
			}
		}
	}
}

func benchmarkAppendToFile(b *testing.B, sync bool) {
	defer os.RemoveAll("benchmarkAppendToFile.wal")
	f, err := os.Create("benchmarkAppendToFile.wal")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	entries := make([][]byte, b.N)
	for i := range entries {
		entries[i] = make([]byte, 1024)
		if _, err := rand.Read(entries[i]); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := range entries {
		if err := wal.AppendEntryToFile(f, entries[i], sync); err != nil {
			b.Fatal(err)
		}
	}
}
