package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

// --- Helper ---

func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := t.TempDir() + "/test.wal"
	s, err := New(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	return s
}

// --- Basic Operations ---

func TestSetAndGet(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()

	s.Set("name", []byte("sachin"))

	got, err := s.Get("name")
	if err != nil {
		t.Fatalf("expected key to exist: %v", err)
	}
	if string(got) != "sachin" {
		t.Errorf("got %s, want sachin", got)
	}
}

func TestGetMissing(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()

	_, err := s.Get("missing")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()

	s.Set("key", []byte("value"))
	s.Delete("key")

	_, err := s.Get("key")
	if err != ErrNotFound {
		t.Errorf("expected key to be deleted")
	}
}

func TestKeys(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()

	s.Set("a", []byte("1"))
	s.Set("b", []byte("2"))
	s.Set("c", []byte("3"))

	keys := s.Keys()
	if len(keys) != 3 {
		t.Errorf("got %d keys, want 3", len(keys))
	}
}

// --- Recovery ---

func TestRecovery(t *testing.T) {
	path := t.TempDir() + "/test.wal"

	// Write data
	s1, err := New(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	s1.Set("key1", []byte("value1"))
	s1.Set("key2", []byte("value2"))
	s1.Delete("key1")
	s1.Close()

	// Recover from WAL
	s2, err := New(path)
	if err != nil {
		t.Fatalf("failed to recover store: %v", err)
	}
	defer s2.Close()

	// key1 should be deleted
	_, err = s2.Get("key1")
	if err != ErrNotFound {
		t.Errorf("expected key1 to be deleted after recovery")
	}

	// key2 should exist
	val, err := s2.Get("key2")
	if err != nil {
		t.Fatalf("expected key2 to exist: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("got %s, want value2", val)
	}
}

// --- Corruption Detection ---

func TestCorruptionDetection(t *testing.T) {
	path := t.TempDir() + "/test.wal"

	// Write valid data
	s, err := New(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	s.Set("key", []byte("value"))
	s.Close()

	// Corrupt the file
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte("garbage"))
	f.Close()

	// Recovery should fail
	_, err = New(path)
	if err == nil {
		t.Error("expected corruption error, got nil")
	}
}

// --- Concurrency ---

func TestConcurrentWrites(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()

	var wg sync.WaitGroup
	const n = 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id%10)
			value := []byte(fmt.Sprintf("value-%d", id))
			s.Set(key, value)
		}(i)
	}

	wg.Wait()

	// Should have 10 unique keys (0-9)
	keys := s.Keys()
	if len(keys) != 10 {
		t.Errorf("got %d keys, want 10", len(keys))
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()

	s.Set("shared", []byte("initial"))

	var wg sync.WaitGroup
	const n = 100

	// Writers
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s.Set("shared", []byte(fmt.Sprintf("value-%d", id)))
		}(i)
	}

	// Readers
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Get("shared")
		}()
	}

	wg.Wait()

	// Key should exist with some value
	_, err := s.Get("shared")
	if err != nil {
		t.Errorf("expected shared key to exist: %v", err)
	}
}
