package storage

import (
	"sync"
	"time"
)

// Record holds value + metadata
type Record struct {
	Value     string
	Timestamp int64
}

// Store is a thread-safe key-value store
type Store struct {
	// TODO: mutex
	// TODO: map of string -> Record
	mu   sync.RWMutex
	Data map[string]Record
}

// New creates a new Store
func New() *Store {
	// TODO
	return &Store{
		Data: make(map[string]Record),
	}
}

// Set stores a key-value pair
func (s *Store) Set(key, value string) {
	// TODO: lock, set with timestamp, unlock
	s.mu.Lock()
	s.Data[key] = Record{value, time.Now().Unix()}
	s.mu.Unlock()
}

// Get retrieves a value by key
func (s *Store) Get(key string) (string, bool) {
	// TODO: rlock, get, runlock
	s.mu.RLock()
	data, ok := s.Data[key]
	s.mu.RUnlock()
	return data.Value, ok

}

// Delete removes a key
func (s *Store) Delete(key string) {
	// TODO
	s.mu.Lock()
	delete(s.Data, key)
	s.mu.Unlock()
}

// Keys returns all keys
func (s *Store) Keys() []string {
	// TODO
	s.mu.RLock()
	keys := make([]string, 0, len(s.Data))
	for k := range s.Data {
		keys = append(keys, k)
	}
	return keys

}
