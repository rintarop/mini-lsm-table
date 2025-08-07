package usecase

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Bloom0716/mini-bigtable/internal/domain"
)

func TestLSMTableServiceBasicOperations(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "lsm_test_basic")
	defer os.RemoveAll(tmpDir)

	service, err := NewLSMTableService(tmpDir, 3)
	if err != nil {
		t.Fatalf("Failed to create LSM service: %v", err)
	}
	defer service.Close()

	// Test Put and Get
	key := []byte("test_key")
	value := []byte("test_value")

	if err := service.Put(key, value); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	retrievedValue, err := service.Get(key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if string(retrievedValue) != string(value) {
		t.Errorf("Expected value %s, got %s", value, retrievedValue)
	}
}

func TestLSMTableServiceDelete(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "lsm_test_delete")
	defer os.RemoveAll(tmpDir)

	service, err := NewLSMTableService(tmpDir, 3)
	if err != nil {
		t.Fatalf("Failed to create LSM service: %v", err)
	}
	defer service.Close()

	key := []byte("test_key")
	value := []byte("test_value")

	// Put then delete
	if err := service.Put(key, value); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	if err := service.Delete(key); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Get should return key not found
	_, err = service.Get(key)
	if err != domain.ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestLSMTableServiceMemTableRotation(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "lsm_test_rotation")
	defer os.RemoveAll(tmpDir)

	maxSize := 2
	service, err := NewLSMTableService(tmpDir, maxSize)
	if err != nil {
		t.Fatalf("Failed to create LSM service: %v", err)
	}
	defer service.Close()

	// Fill up the first memtable
	for i := 0; i < maxSize; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i + 10)}
		if err := service.Put(key, value); err != nil {
			t.Fatalf("Failed to put entry %d: %v", i, err)
		}
	}

	// Check stats - should have full active table, no immutable tables yet
	activeSize, immutableCount := service.GetMemTableStats()
	if activeSize != maxSize {
		t.Errorf("Expected active size %d, got %d", maxSize, activeSize)
	}
	if immutableCount != 0 {
		t.Errorf("Expected 0 immutable tables, got %d", immutableCount)
	}

	// Add one more entry - this should trigger rotation
	overflowKey := []byte{byte(maxSize)}
	overflowValue := []byte{byte(maxSize + 10)}
	if err := service.Put(overflowKey, overflowValue); err != nil {
		t.Fatalf("Failed to put overflow entry: %v", err)
	}

	// Check stats - should have rotated
	activeSize, immutableCount = service.GetMemTableStats()
	if activeSize != 1 {
		t.Errorf("Expected active size 1 after rotation, got %d", activeSize)
	}
	if immutableCount != 1 {
		t.Errorf("Expected 1 immutable table after rotation, got %d", immutableCount)
	}

	// Verify all entries are still accessible
	for i := 0; i <= maxSize; i++ {
		key := []byte{byte(i)}
		expectedValue := []byte{byte(i + 10)}

		value, err := service.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %d after rotation: %v", i, err)
		}
		if string(value) != string(expectedValue) {
			t.Errorf("Entry %d: expected value %v, got %v", i, expectedValue, value)
		}
	}
}

func TestLSMTableServiceGetFromImmutableTable(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "lsm_test_immutable")
	defer os.RemoveAll(tmpDir)

	service, err := NewLSMTableService(tmpDir, 2)
	if err != nil {
		t.Fatalf("Failed to create LSM service: %v", err)
	}
	defer service.Close()

	// Put entries that will be in immutable table
	oldKey := []byte("old_key")
	oldValue := []byte("old_value")
	if err := service.Put(oldKey, oldValue); err != nil {
		t.Fatalf("Failed to put old entry: %v", err)
	}

	// Fill up to trigger rotation
	if err := service.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to put second entry: %v", err)
	}

	// This should trigger rotation
	if err := service.Put([]byte("new_key"), []byte("new_value")); err != nil {
		t.Fatalf("Failed to put new entry: %v", err)
	}

	// Should be able to get old entry from immutable table
	value, err := service.Get(oldKey)
	if err != nil {
		t.Fatalf("Failed to get old entry: %v", err)
	}
	if string(value) != string(oldValue) {
		t.Errorf("Expected old value %s, got %s", oldValue, value)
	}
}

func TestLSMTableServiceRecovery(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "lsm_test_recovery")
	defer os.RemoveAll(tmpDir)

	// Create service and add some data
	service1, err := NewLSMTableService(tmpDir, 5)
	if err != nil {
		t.Fatalf("Failed to create first LSM service: %v", err)
	}

	testEntries := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for key, value := range testEntries {
		if err := service1.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Delete one entry
	if err := service1.Delete([]byte("key2")); err != nil {
		t.Fatalf("Failed to delete key2: %v", err)
	}

	if err := service1.Close(); err != nil {
		t.Fatalf("Failed to close first service: %v", err)
	}

	// Create new service and recover
	service2, err := NewLSMTableService(tmpDir, 5)
	if err != nil {
		t.Fatalf("Failed to create second LSM service: %v", err)
	}
	defer service2.Close()

	if err := service2.Recovery(); err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}

	// Verify recovered data
	// key1 and key3 should exist
	for _, key := range []string{"key1", "key3"} {
		expectedValue := testEntries[key]
		value, err := service2.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s after recovery: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, value)
		}
	}

	// key2 should be deleted
	_, err = service2.Get([]byte("key2"))
	if err != domain.ErrKeyNotFound {
		t.Errorf("Expected key2 to be deleted after recovery, got error: %v", err)
	}
}
