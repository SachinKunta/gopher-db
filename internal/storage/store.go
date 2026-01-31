package storage

import (
	"errors"
	"sync"
	"time"

	"github.com/SachinKunta/gopher-db/internal/wal"
)

var ErrNotFound = errors.New("key not found")

// Record holds value + metadata
type Record struct {
	Value     []byte
	Timestamp int64
}

// Store is a thread-safe key-value store
type Store struct {
	mu   sync.RWMutex
	data map[string]Record
	wal  *wal.WAL
}

func New(walPath string) (*Store, error) {
	w, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	s := &Store{
		data: make(map[string]Record),
		wal:  w,
	}

	// ON STARTUP: The store recovers itself
	s.recover()

	return s, nil
}

// Set stores a key-value pair
func (s *Store) Set(key string, value []byte) error {
	// Write to WAL
	if err := s.wal.Append("SET", key, value); err != nil {
		return err
	}

	// Write to memory
	s.mu.Lock()
	s.data[key] = Record{Value: value, Timestamp: time.Now().UnixNano()}
	s.mu.Unlock()
	return nil
}

// Get retrieves a value by key
func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, exists := s.data[key]
	if !exists {
		return nil, ErrNotFound
	}
	return rec.Value, nil
}

// Delete removes a key
func (s *Store) Delete(key string) {
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
}

// Keys returns all keys
func (s *Store) Keys() []string {
	s.mu.RLock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys

}

func (s *Store) recover() {
	entries := s.wal.Replay() // WAL now has a Replay method
	for _, e := range entries {
		switch e.Op {
		case "SET":
			s.data[e.Key] = Record{Value: []byte(e.Value), Timestamp: time.Now().UnixNano()}
		case "DELETE":
			delete(s.data, e.Key)
		}
	}
	s.wal.Close()
}
