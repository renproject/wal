package wal_test

import (
	"bytes"
	"crypto/rand"
	"os"
	"testing"

	"github.com/renproject/wal"
)

func TestPushPopSync(t *testing.T) {
	testPushPop(t, true, false)
}

func TestPushPopNoSync(t *testing.T) {
	testPushPop(t, false, false)
}

func TestPushPopSyncConcurrent(t *testing.T) {
	testPushPop(t, true, true)
}

func TestPushPopNoSyncConcurrent(t *testing.T) {
	testPushPop(t, false, true)
}

func testPushPop(t *testing.T, sync, concurrent bool) {
	defer os.RemoveAll("testPushPop.wal")
	w, err := wal.New("testPushPop.wal", sync)
	if err != nil {
		t.Fatal(err)
	}

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

	q := make(chan struct{}, 2)
	ch := make(chan struct{}, n)
	push := func() {
		defer func() { q <- struct{}{} }()
		for i := range entries {
			if err := w.Push(entries[i]); err != nil {
				t.Fatal(err)
			}
			ch <- struct{}{}
		}
	}
	pop := func() {
		defer func() { q <- struct{}{} }()
		for i := range entries {
			<-ch
			entry, err := w.Pop()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(entry, entries[i]) {
				t.Fatalf("expected %x to equal %x", entry, entries[i])
			}
		}
	}

	if concurrent {
		go push()
		go pop()
		<-q
		<-q
	} else {
		push()
		pop()
	}
}

func BenchmarkPushSync(b *testing.B) {
	benchmarkPush(b, true)
}

func BenchmarkPushNoSync(b *testing.B) {
	benchmarkPush(b, false)
}

func BenchmarkPop(b *testing.B) {
	benchmarkPop(b)
}

func benchmarkPush(b *testing.B, sync bool) {
	defer os.RemoveAll("benchmarkPush.wal")
	w, err := wal.New("benchmarkPush.wal", sync)
	if err != nil {
		b.Fatal(err)
	}

	entries := make([][]byte, b.N)
	for i := range entries {
		entries[i] = make([]byte, 1024)
		if _, err := rand.Read(entries[i]); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := range entries {
		if err := w.Push(entries[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkPop(b *testing.B) {
	defer os.RemoveAll("benchmarkPop.wal")
	w, err := wal.New("benchmarkPop.wal", false)
	if err != nil {
		b.Fatal(err)
	}

	entries := make([][]byte, b.N)
	for i := range entries {
		entries[i] = make([]byte, 1024)
		if _, err := rand.Read(entries[i]); err != nil {
			b.Fatal(err)
		}
	}
	for i := range entries {
		if err := w.Push(entries[i]); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := range entries {
		entry, err := w.Pop()
		if err != nil {
			b.Fatal(err)
		}
		if !bytes.Equal(entry, entries[i]) {
			b.Fatalf("expected %x to equal %x", entry, entries[i])
		}
	}
}
