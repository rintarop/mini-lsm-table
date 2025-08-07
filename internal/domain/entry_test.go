package domain

import (
	"testing"
	"time"
)

func TestNewPutEntry(t *testing.T) {
	key := []byte("test_key")
	value := []byte("test_value")

	entry := NewPutEntry(key, value)

	if string(entry.Key()) != string(key) {
		t.Errorf("Expected key %s, got %s", key, entry.Key())
	}

	if string(entry.Value()) != string(value) {
		t.Errorf("Expected value %s, got %s", value, entry.Value())
	}

	if entry.Type() != EntryTypePut {
		t.Errorf("Expected entry type PUT, got %v", entry.Type())
	}

	if entry.IsDeleted() {
		t.Error("Expected entry to not be deleted")
	}
}

func TestNewDeleteEntry(t *testing.T) {
	key := []byte("test_key")

	entry := NewDeleteEntry(key)

	if string(entry.Key()) != string(key) {
		t.Errorf("Expected key %s, got %s", key, entry.Key())
	}

	if entry.Value() != nil {
		t.Errorf("Expected nil value for delete entry, got %s", entry.Value())
	}

	if entry.Type() != EntryTypeDelete {
		t.Errorf("Expected entry type DELETE, got %v", entry.Type())
	}

	if !entry.IsDeleted() {
		t.Error("Expected entry to be deleted")
	}
}

func TestEntryCompare(t *testing.T) {
	entry1 := NewPutEntry([]byte("aaa"), []byte("value1"))
	entry2 := NewPutEntry([]byte("bbb"), []byte("value2"))
	entry3 := NewPutEntry([]byte("aaa"), []byte("value3"))

	if entry1.Compare(entry2) >= 0 {
		t.Error("Expected entry1 to be less than entry2")
	}

	if entry2.Compare(entry1) <= 0 {
		t.Error("Expected entry2 to be greater than entry1")
	}

	if entry1.Compare(entry3) != 0 {
		t.Error("Expected entry1 to be equal to entry3 in terms of key comparison")
	}
}

func TestEntryIsNewerThan(t *testing.T) {
	// Create entries with different timestamps
	time.Sleep(1 * time.Millisecond) // Ensure different timestamps
	entry1 := NewPutEntry([]byte("key"), []byte("value1"))
	time.Sleep(1 * time.Millisecond)
	entry2 := NewPutEntry([]byte("key"), []byte("value2"))

	if !entry2.IsNewerThan(entry1) {
		t.Error("Expected entry2 to be newer than entry1")
	}

	if entry1.IsNewerThan(entry2) {
		t.Error("Expected entry1 to not be newer than entry2")
	}
}
