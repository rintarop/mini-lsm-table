package model

import (
	"bytes"
	"testing"
)

func TestBlockIndexBasicOperations(t *testing.T) {
	index := NewBlockIndex(100)

	// Add some index entries
	index.AddEntry([]byte("apple"), 100)
	index.AddEntry([]byte("banana"), 200)
	index.AddEntry([]byte("cherry"), 300)
	index.AddEntry([]byte("date"), 400)

	// Test finding offsets
	testCases := []struct {
		key            string
		expectedOffset uint64
	}{
		{"apple", 100},      // Exact match
		{"avocado", 100},    // Between apple and banana, should return apple's offset
		{"banana", 200},     // Exact match
		{"blueberry", 200},  // Between banana and cherry
		{"cherry", 300},     // Exact match
		{"coconut", 300},    // Between cherry and date
		{"date", 400},       // Exact match
		{"elderberry", 400}, // After date, should return date's offset
		{"aardvark", 0},     // Before apple, should return 0
	}

	for _, tc := range testCases {
		offset := index.FindOffset([]byte(tc.key))
		if offset != tc.expectedOffset {
			t.Errorf("Key %s: expected offset %d, got %d", tc.key, tc.expectedOffset, offset)
		}
	}
}

func TestBlockIndexSerialization(t *testing.T) {
	// Create index with some entries
	originalIndex := NewBlockIndex(50)
	originalIndex.AddEntry([]byte("key1"), 100)
	originalIndex.AddEntry([]byte("key2"), 200)
	originalIndex.AddEntry([]byte("key3"), 300)

	// Serialize to buffer
	var buffer bytes.Buffer
	if err := originalIndex.SerializeIndex(&buffer); err != nil {
		t.Fatalf("Failed to serialize index: %v", err)
	}

	// Deserialize from buffer
	deserializedIndex, err := DeserializeIndex(&buffer, 50)
	if err != nil {
		t.Fatalf("Failed to deserialize index: %v", err)
	}

	// Compare original and deserialized
	originalEntries := originalIndex.GetEntries()
	deserializedEntries := deserializedIndex.GetEntries()

	if len(originalEntries) != len(deserializedEntries) {
		t.Errorf("Expected %d entries, got %d", len(originalEntries), len(deserializedEntries))
	}

	for i, originalEntry := range originalEntries {
		deserializedEntry := deserializedEntries[i]

		if !bytes.Equal(originalEntry.Key, deserializedEntry.Key) {
			t.Errorf("Entry %d: expected key %s, got %s", i, originalEntry.Key, deserializedEntry.Key)
		}

		if originalEntry.Offset != deserializedEntry.Offset {
			t.Errorf("Entry %d: expected offset %d, got %d", i, originalEntry.Offset, deserializedEntry.Offset)
		}
	}
}

func TestBlockIndexEmptyIndex(t *testing.T) {
	index := NewBlockIndex(100)

	// Test with empty index
	offset := index.FindOffset([]byte("any_key"))
	if offset != 0 {
		t.Errorf("Expected offset 0 for empty index, got %d", offset)
	}

	if index.Size() != 0 {
		t.Errorf("Expected size 0 for empty index, got %d", index.Size())
	}
}

func TestBlockIndexSingleEntry(t *testing.T) {
	index := NewBlockIndex(100)
	index.AddEntry([]byte("middle"), 500)

	testCases := []struct {
		key            string
		expectedOffset uint64
	}{
		{"apple", 0},    // Before single entry
		{"middle", 500}, // Exact match
		{"zebra", 500},  // After single entry
	}

	for _, tc := range testCases {
		offset := index.FindOffset([]byte(tc.key))
		if offset != tc.expectedOffset {
			t.Errorf("Key %s: expected offset %d, got %d", tc.key, tc.expectedOffset, offset)
		}
	}
}

func TestBlockIndexBinarySearch(t *testing.T) {
	index := NewBlockIndex(10)

	// Add many entries to test binary search
	keys := []string{"a", "c", "e", "g", "i", "k", "m", "o", "q", "s", "u", "w", "y"}
	for i, key := range keys {
		index.AddEntry([]byte(key), uint64((i+1)*100))
	}

	// Test that we can find correct positions efficiently
	testCases := []struct {
		searchKey      string
		expectedOffset uint64
	}{
		{"b", 100},  // Between a(100) and c(200), should return a's offset
		{"d", 200},  // Between c(200) and e(300), should return c's offset
		{"h", 400},  // Between g(400) and i(500), should return g's offset
		{"p", 800},  // Between o(800) and q(900), should return o's offset
		{"z", 1300}, // After y(1300), should return y's offset
	}

	for _, tc := range testCases {
		offset := index.FindOffset([]byte(tc.searchKey))
		if offset != tc.expectedOffset {
			t.Errorf("Key %s: expected offset %d, got %d", tc.searchKey, tc.expectedOffset, offset)
		}
	}
}
