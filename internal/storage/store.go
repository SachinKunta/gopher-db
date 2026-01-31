package storage

import (
	"errors"
	"sync"
	"time"

	"github.com/SachinKunta/gopher-db/internal/wal"
)

var ErrNotFound = errors.New("key not found")

// Record holds a value and its metadata.
type Record struct {
	Value     []byte
	Timestamp uint64
}

// Store is a thread-safe key-value store backed by a WAL.
type Store struct {
	mu   sync.RWMutex
	data map[string]Record
	wal  *wal.WAL
}

// New creates a new Store with WAL at the given path.
// Automatically recovers existing data from the WAL.
func New(walPath string) (*Store, error) {
	w, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	s := &Store{
		data: make(map[string]Record),
		wal:  w,
	}

	if err := s.recover(); err != nil {
		return nil, err
	}

	return s, nil
}

// Close closes the underlying WAL.
func (s *Store) Close() error {
	return s.wal.Close()
}

// --- Public Operations ---

// Set stores a key-value pair.
// Writes to WAL first (durability), then to memory.
func (s *Store) Set(key string, value []byte) error {
	record := newRecord(value)

	if err := s.wal.Append("SET", key, record.Value, record.Timestamp); err != nil {
		return err
	}

	s.setInMemory(key, record)
	return nil
}

// Get retrieves a value by key.
// Returns ErrNotFound if key doesn't exist.
func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	record, exists := s.data[key]
	if !exists {
		return nil, ErrNotFound
	}
	return record.Value, nil
}

// Delete removes a key.
// Writes to WAL first, then removes from memory.
func (s *Store) Delete(key string) error {
	if err := s.wal.Append("DELETE", key, nil, 0); err != nil {
		return err
	}

	s.deleteInMemory(key)
	return nil
}

// Keys returns all keys in the store.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// --- Internal Helpers ---

func newRecord(value []byte) Record {
	return Record{
		Value:     value,
		Timestamp: uint64(time.Now().UnixNano()),
	}
}

func (s *Store) setInMemory(key string, record Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = record
}

func (s *Store) deleteInMemory(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// --- Recovery ---

// recover replays the WAL to rebuild in-memory state.
// Called once on startup.
func (s *Store) recover() error {
	entries, err := s.wal.Replay()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		s.applyEntry(entry)
	}
	return nil
}

func (s *Store) applyEntry(entry wal.Entry) {
	switch entry.Op {
	case "SET":
		s.data[entry.Key] = Record{
			Value:     entry.Value,
			Timestamp: entry.Timestamp,
		}
	case "DELETE":
		delete(s.data, entry.Key)
	}
}
