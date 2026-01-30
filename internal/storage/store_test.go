package storage

import "testing"

func TestSetAndGet(t *testing.T) {
	s := New()
	s.Set("key", "value")

	got, ok := s.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if got != "value" {
		t.Errorf("got %q, want %q", got, "value")
	}
}

func TestGetMissing(t *testing.T) {
	s := New()

	_, ok := s.Get("missing")
	if ok {
		t.Fatal("expected key to not exist")
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Set("key", "value")
	s.Delete("key")

	_, ok := s.Get("key")
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestKeys(t *testing.T) {
	s := New()
	s.Set("a", "1")
	s.Set("b", "2")
	s.Set("c", "3")

	keys := s.Keys()
	if len(keys) != 3 {
		t.Errorf("got %d keys, want 3", len(keys))
	}
}
