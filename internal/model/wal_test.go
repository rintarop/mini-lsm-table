package model

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALWriteAndRecover(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "wal_test")
	defer os.RemoveAll(tmpDir)

	wal, err := NewWAL(tmpDir, "test.wal")
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Write some entries
	entries := []*Entry{
		NewPutEntry([]byte("key1"), []byte("value1")),
		NewPutEntry([]byte("key2"), []byte("value2")),
		NewDeleteEntry([]byte("key3")),
	}

	for _, entry := range entries {
		if err := wal.WriteEntry(entry); err != nil {
			t.Fatalf("Failed to write entry: %v", err)
		}
	}

	if err := wal.Flush(); err != nil {
		t.Fatalf("Failed to flush WAL: %v", err)
	}

	// Recover entries
	recoveredEntries, err := wal.Recover()
	if err != nil {
		t.Fatalf("Failed to recover entries: %v", err)
	}

	if len(recoveredEntries) != len(entries) {
		t.Errorf("Expected %d entries, got %d", len(entries), len(recoveredEntries))
	}

	// Verify recovered entries
	for i, recovered := range recoveredEntries {
		original := entries[i]

		if string(recovered.Key()) != string(original.Key()) {
			t.Errorf("Entry %d: expected key %s, got %s", i, original.Key(), recovered.Key())
		}

		if string(recovered.Value()) != string(original.Value()) {
			t.Errorf("Entry %d: expected value %s, got %s", i, original.Value(), recovered.Value())
		}

		if recovered.Type() != original.Type() {
			t.Errorf("Entry %d: expected type %v, got %v", i, original.Type(), recovered.Type())
		}

		if recovered.IsDeleted() != original.IsDeleted() {
			t.Errorf("Entry %d: expected deleted %v, got %v", i, original.IsDeleted(), recovered.IsDeleted())
		}
	}
}

func TestWALRecoverEmptyFile(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "wal_test_empty")
	defer os.RemoveAll(tmpDir)

	wal, err := NewWAL(tmpDir, "empty.wal")
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Recover from empty file
	entries, err := wal.Recover()
	if err != nil {
		t.Fatalf("Failed to recover from empty WAL: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("Expected 0 entries from empty WAL, got %d", len(entries))
	}
}

func TestWALRecoverNonExistentFile(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "wal_test_nonexistent")
	defer os.RemoveAll(tmpDir)

	wal, err := NewWAL(tmpDir, "nonexistent.wal")
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Remove the file to simulate non-existent WAL
	if err := wal.Remove(); err != nil {
		t.Fatalf("Failed to remove WAL file: %v", err)
	}

	// Recreate WAL object
	wal, err = NewWAL(tmpDir, "nonexistent.wal")
	if err != nil {
		t.Fatalf("Failed to recreate WAL: %v", err)
	}
	defer wal.Close()

	// Recover from non-existent file should return empty slice
	entries, err := wal.Recover()
	if err != nil {
		t.Fatalf("Failed to recover from non-existent WAL: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("Expected 0 entries from non-existent WAL, got %d", len(entries))
	}
}

func TestWALPersistence(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "wal_test_persistence")
	defer os.RemoveAll(tmpDir)

	// Create WAL and write an entry
	wal1, err := NewWAL(tmpDir, "persistence.wal")
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	entry := NewPutEntry([]byte("persistent_key"), []byte("persistent_value"))
	if err := wal1.WriteEntry(entry); err != nil {
		t.Fatalf("Failed to write entry: %v", err)
	}

	if err := wal1.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Create new WAL instance and recover
	wal2, err := NewWAL(tmpDir, "persistence.wal")
	if err != nil {
		t.Fatalf("Failed to create second WAL: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.Recover()
	if err != nil {
		t.Fatalf("Failed to recover entries: %v", err)
	}

	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}

	if string(entries[0].Key()) != "persistent_key" {
		t.Errorf("Expected key 'persistent_key', got '%s'", entries[0].Key())
	}
}
