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
		t.Errorf("got %v, want %v", got, "value")
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
	s.Set("a", 1)
	s.Set("b", "two")
	s.Set("c", true)

	keys := s.Keys()
	if len(keys) != 3 {
		t.Errorf("got %d keys, want 3", len(keys))
	}
}

func TestDifferentTypes(t *testing.T) {
	s := New()

	s.Set("string", "hello")
	s.Set("int", 42)
	s.Set("bool", true)
	s.Set("slice", []int{1, 2, 3})

	if v, ok := s.Get("string"); !ok || v != "hello" {
		t.Errorf("string: got %v, want hello", v)
	}

	if v, ok := s.Get("int"); !ok || v != 42 {
		t.Errorf("int: got %v, want 42", v)
	}

	if v, ok := s.Get("bool"); !ok || v != true {
		t.Errorf("bool: got %v, want true", v)
	}

	if v, ok := s.Get("slice"); !ok {
		t.Errorf("slice: expected to exist")
	} else {
		slice, ok := v.([]int)
		if !ok || len(slice) != 3 {
			t.Errorf("slice: got %v, want [1 2 3]", v)
		}
	}
}
