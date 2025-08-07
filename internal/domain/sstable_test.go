package domain

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSSTableBuildAndGet(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "sstable_test")
	defer os.RemoveAll(tmpDir)

	// Create builder and add entries
	builder := NewSSTableBuilder(0, 10)

	entries := []*Entry{
		NewPutEntry([]byte("key1"), []byte("value1")),
		NewPutEntry([]byte("key3"), []byte("value3")),
		NewPutEntry([]byte("key2"), []byte("value2")),
		NewDeleteEntry([]byte("key4")),
	}

	for _, entry := range entries {
		builder.AddEntry(entry)
	}

	// Build SSTable
	sst, err := builder.Build(tmpDir, "test.sst")
	if err != nil {
		t.Fatalf("Failed to build SSTable: %v", err)
	}

	// Test Get operations
	value, err := sst.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if string(value.Value()) != "value1" {
		t.Errorf("Expected value1, got %s", value.Value())
	}

	// Test deleted entry
	deleteEntry, err := sst.Get([]byte("key4"))
	if err != nil {
		t.Fatalf("Failed to get key4: %v", err)
	}
	if !deleteEntry.IsDeleted() {
		t.Error("Expected key4 to be marked as deleted")
	}

	// Test non-existent key
	_, err = sst.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for nonexistent key, got %v", err)
	}
}

func TestSSTableMetadata(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "sstable_metadata_test")
	defer os.RemoveAll(tmpDir)

	builder := NewSSTableBuilder(1, 5)

	// Add entries in random order
	builder.AddEntry(NewPutEntry([]byte("zebra"), []byte("last")))
	builder.AddEntry(NewPutEntry([]byte("apple"), []byte("first")))
	builder.AddEntry(NewPutEntry([]byte("mango"), []byte("middle")))

	sst, err := builder.Build(tmpDir, "metadata_test.sst")
	if err != nil {
		t.Fatalf("Failed to build SSTable: %v", err)
	}

	metadata := sst.Metadata()

	// Check metadata
	if metadata.Level != 1 {
		t.Errorf("Expected level 1, got %d", metadata.Level)
	}

	if metadata.EntryCount != 3 {
		t.Errorf("Expected 3 entries, got %d", metadata.EntryCount)
	}

	// Check that min/max keys are correct (entries should be sorted)
	if string(metadata.MinKey) != "apple" {
		t.Errorf("Expected min key 'apple', got '%s'", metadata.MinKey)
	}

	if string(metadata.MaxKey) != "zebra" {
		t.Errorf("Expected max key 'zebra', got '%s'", metadata.MaxKey)
	}

	if metadata.FileSize == 0 {
		t.Error("Expected non-zero file size")
	}
}

func TestSSTableIterator(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "sstable_iterator_test")
	defer os.RemoveAll(tmpDir)

	builder := NewSSTableBuilder(0, 5)

	// Add entries
	expectedEntries := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for key, value := range expectedEntries {
		builder.AddEntry(NewPutEntry([]byte(key), []byte(value)))
	}

	sst, err := builder.Build(tmpDir, "iterator_test.sst")
	if err != nil {
		t.Fatalf("Failed to build SSTable: %v", err)
	}

	// Test iterator
	iter, err := sst.Iterator()
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Close()

	retrievedEntries := make(map[string]string)

	for iter.Next() {
		entry := iter.Entry()
		retrievedEntries[string(entry.Key())] = string(entry.Value())
	}

	if err := iter.Error(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}

	// Check that all entries were retrieved
	for key, expectedValue := range expectedEntries {
		if retrievedValue, exists := retrievedEntries[key]; !exists {
			t.Errorf("Key %s not found in iterator results", key)
		} else if retrievedValue != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, retrievedValue)
		}
	}
}

func TestSSTableGetAllEntries(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "sstable_getall_test")
	defer os.RemoveAll(tmpDir)

	builder := NewSSTableBuilder(0, 3)

	originalEntries := []*Entry{
		NewPutEntry([]byte("key1"), []byte("value1")),
		NewPutEntry([]byte("key2"), []byte("value2")),
		NewDeleteEntry([]byte("key3")),
	}

	for _, entry := range originalEntries {
		builder.AddEntry(entry)
	}

	sst, err := builder.Build(tmpDir, "getall_test.sst")
	if err != nil {
		t.Fatalf("Failed to build SSTable: %v", err)
	}

	// Get all entries
	allEntries, err := sst.GetAllEntries()
	if err != nil {
		t.Fatalf("Failed to get all entries: %v", err)
	}

	if len(allEntries) != len(originalEntries) {
		t.Errorf("Expected %d entries, got %d", len(originalEntries), len(allEntries))
	}

	// Entries should be sorted by key
	for i := 1; i < len(allEntries); i++ {
		if allEntries[i-1].Compare(allEntries[i]) >= 0 {
			t.Error("Entries are not sorted by key")
		}
	}
}

func TestSSTableWithBlockIndex(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "sstable_block_index_test")
	defer os.RemoveAll(tmpDir)

	// Create builder and add many entries to test block index
	builder := NewSSTableBuilder(0, 1000)

	// Add 500 entries to ensure multiple index blocks
	entries := make([]*Entry, 500)
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value := []byte(fmt.Sprintf("value_%04d", i))
		entry := NewPutEntry(key, value)
		entries[i] = entry
		builder.AddEntry(entry)
	}

	// Build SSTable
	sst, err := builder.Build(tmpDir, "block_index_test.sst")
	if err != nil {
		t.Fatalf("Failed to build SSTable: %v", err)
	}

	// Verify block index was created
	if sst.metadata.BlockIndex == nil {
		t.Fatal("Block index should not be nil")
	}

	// Block index should have multiple entries (500 entries / 100 block size = 5 index entries)
	expectedIndexSize := 5
	if sst.metadata.BlockIndex.Size() != expectedIndexSize {
		t.Errorf("Expected block index size %d, got %d", expectedIndexSize, sst.metadata.BlockIndex.Size())
	}

	// Test retrieval of various keys
	testKeys := []string{
		"key_0000", // First key
		"key_0050", // Early key
		"key_0150", // Middle key
		"key_0350", // Later key
		"key_0499", // Last key
	}

	for _, keyStr := range testKeys {
		key := []byte(keyStr)
		entry, err := sst.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", keyStr, err)
			continue
		}

		expectedValue := []byte(fmt.Sprintf("value_%s", keyStr[4:]))
		if string(entry.Value()) != string(expectedValue) {
			t.Errorf("Key %s: expected value %s, got %s", keyStr, expectedValue, entry.Value())
		}
	}

	// Test non-existent keys
	nonExistentKeys := []string{"key_-001", "key_0500", "nonexistent"}
	for _, keyStr := range nonExistentKeys {
		_, err := sst.Get([]byte(keyStr))
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound for key %s, got %v", keyStr, err)
		}
	}
}

func TestSSTableBlockIndexPerformance(t *testing.T) {
	// This test demonstrates the performance improvement with block index
	tmpDir := filepath.Join(os.TempDir(), "sstable_performance_test")
	defer os.RemoveAll(tmpDir)

	// Create a large SSTable
	builder := NewSSTableBuilder(0, 10000)

	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("performance_key_%06d", i))
		value := []byte(fmt.Sprintf("performance_value_%06d", i))
		builder.AddEntry(NewPutEntry(key, value))
	}

	sst, err := builder.Build(tmpDir, "performance_test.sst")
	if err != nil {
		t.Fatalf("Failed to build SSTable: %v", err)
	}

	// Test searching for keys throughout the range
	searchKeys := []string{
		"performance_key_000100", // Early
		"performance_key_000500", // Middle
		"performance_key_000900", // Late
	}

	for _, keyStr := range searchKeys {
		entry, err := sst.Get([]byte(keyStr))
		if err != nil {
			t.Errorf("Failed to get key %s: %v", keyStr, err)
			continue
		}

		expectedValue := fmt.Sprintf("performance_value_%s", keyStr[16:])
		if string(entry.Value()) != expectedValue {
			t.Errorf("Key %s: expected value %s, got %s", keyStr, expectedValue, entry.Value())
		}
	}

	// Verify that the block index helped us avoid reading unnecessary data
	if sst.metadata.BlockIndex.Size() == 0 {
		t.Error("Block index should contain entries for performance optimization")
	}
}

func TestSSTableBloomFilter(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "sstable_bloom_test")
	defer os.RemoveAll(tmpDir)

	builder := NewSSTableBuilder(0, 100)

	// Add many entries
	for i := 0; i < 50; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i + 100)}
		builder.AddEntry(NewPutEntry(key, value))
	}

	sst, err := builder.Build(tmpDir, "bloom_test.sst")
	if err != nil {
		t.Fatalf("Failed to build SSTable: %v", err)
	}

	// Test that bloom filter correctly identifies existing keys
	for i := 0; i < 50; i++ {
		key := []byte{byte(i)}
		if !sst.metadata.BloomFilter.Contains(key) {
			t.Errorf("Bloom filter should contain key %v", key)
		}
	}

	// Test bloom filter for non-existent keys
	// Note: There might be false positives, but we test a range that's likely to have some negatives
	falsePositives := 0
	for i := 200; i < 250; i++ {
		key := []byte{byte(i)}
		if sst.metadata.BloomFilter.Contains(key) {
			falsePositives++
		}
	}

	// With a good bloom filter, false positives should be relatively low
	if falsePositives > 25 { // More than 50% false positive rate is too high
		t.Errorf("Too many false positives: %d out of 50", falsePositives)
	}
}
