package domain

import (
	"testing"
)

func TestMemTablePutAndGet(t *testing.T) {
	mt := NewMemTable(10)

	key := []byte("test_key")
	value := []byte("test_value")

	// Test Put
	err := mt.Put(key, value)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Test Get
	entry, err := mt.Get(key)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if string(entry.Key()) != string(key) {
		t.Errorf("Expected key %s, got %s", key, entry.Key())
	}

	if string(entry.Value()) != string(value) {
		t.Errorf("Expected value %s, got %s", value, entry.Value())
	}

	if entry.IsDeleted() {
		t.Error("Expected entry to not be deleted")
	}
}

func TestMemTableDelete(t *testing.T) {
	mt := NewMemTable(10)

	key := []byte("test_key")
	value := []byte("test_value")

	// Put then delete
	mt.Put(key, value)
	err := mt.Delete(key)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Get should return the delete marker
	entry, err := mt.Get(key)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if !entry.IsDeleted() {
		t.Error("Expected entry to be deleted")
	}
}

func TestMemTableGetNonExistentKey(t *testing.T) {
	mt := NewMemTable(10)

	key := []byte("non_existent_key")

	_, err := mt.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestMemTableCapacity(t *testing.T) {
	maxSize := 3
	mt := NewMemTable(maxSize)

	// Fill up the memtable
	for i := 0; i < maxSize; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i)}
		err := mt.Put(key, value)
		if err != nil {
			t.Fatalf("Expected no error for entry %d, got %v", i, err)
		}
	}

	if !mt.IsFull() {
		t.Error("Expected memtable to be full")
	}

	// Try to add one more entry
	err := mt.Put([]byte{byte(maxSize)}, []byte{byte(maxSize)})
	if err != ErrTableFull {
		t.Errorf("Expected ErrTableFull, got %v", err)
	}
}

func TestMemTableSize(t *testing.T) {
	mt := NewMemTable(10)

	if mt.Size() != 0 {
		t.Errorf("Expected size 0, got %d", mt.Size())
	}

	mt.Put([]byte("key1"), []byte("value1"))
	if mt.Size() != 1 {
		t.Errorf("Expected size 1, got %d", mt.Size())
	}

	mt.Put([]byte("key2"), []byte("value2"))
	if mt.Size() != 2 {
		t.Errorf("Expected size 2, got %d", mt.Size())
	}

	// Updating existing key shouldn't increase size
	mt.Put([]byte("key1"), []byte("new_value"))
	if mt.Size() != 2 {
		t.Errorf("Expected size 2 after update, got %d", mt.Size())
	}
}

func TestMemTableReadOnly(t *testing.T) {
	mt := NewMemTable(10)

	// Initially should not be read-only
	if mt.IsReadOnly() {
		t.Error("Expected memtable to not be read-only initially")
	}

	mt.Put([]byte("key"), []byte("value"))

	// Set to read-only
	mt.SetReadOnly()
	if !mt.IsReadOnly() {
		t.Error("Expected memtable to be read-only after setting")
	}

	// Should not be able to put new entries
	err := mt.Put([]byte("new_key"), []byte("new_value"))
	if err == nil {
		t.Error("Expected error when putting to read-only memtable")
	}

	// Should not be able to delete entries
	err = mt.Delete([]byte("key"))
	if err == nil {
		t.Error("Expected error when deleting from read-only memtable")
	}

	// Should still be able to read
	_, err = mt.Get([]byte("key"))
	if err != nil {
		t.Errorf("Expected no error when reading from read-only memtable, got %v", err)
	}
}

func TestMemTableGetAllEntries(t *testing.T) {
	mt := NewMemTable(10)

	// Add some entries
	mt.Put([]byte("key1"), []byte("value1"))
	mt.Put([]byte("key2"), []byte("value2"))
	mt.Delete([]byte("key3"))

	entries := mt.GetAllEntries()
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	// Verify entries are present
	keySet := make(map[string]bool)
	for _, entry := range entries {
		keySet[string(entry.Key())] = true
	}

	expectedKeys := []string{"key1", "key2", "key3"}
	for _, expectedKey := range expectedKeys {
		if !keySet[expectedKey] {
			t.Errorf("Expected key %s to be present in entries", expectedKey)
		}
	}
}
