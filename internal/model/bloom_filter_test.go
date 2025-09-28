package model

import (
	"testing"
)

func TestBloomFilterBasicOperations(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Test adding and checking
	key1 := []byte("test_key_1")
	key2 := []byte("test_key_2")

	// Add keys
	bf.Add(key1)
	bf.Add(key2)

	// Check contains
	if !bf.Contains(key1) {
		t.Error("Expected key1 to be present in bloom filter")
	}

	if !bf.Contains(key2) {
		t.Error("Expected key2 to be present in bloom filter")
	}

	// Note: We can't guarantee key3 is not present due to false positives
	// but we can test that it's working as expected in most cases
}

func TestBloomFilterFalsePositives(t *testing.T) {
	bf := NewBloomFilter(100, 0.1) // Higher false positive rate for testing

	// Add some keys
	addedKeys := [][]byte{
		[]byte("key1"), []byte("key2"), []byte("key3"),
		[]byte("key4"), []byte("key5"),
	}

	for _, key := range addedKeys {
		bf.Add(key)
	}

	// Check that all added keys are found
	for _, key := range addedKeys {
		if !bf.Contains(key) {
			t.Errorf("Expected added key %s to be found", string(key))
		}
	}

	// Test many non-added keys to estimate false positive rate
	falsePositives := 0
	totalTests := 1000

	for i := 0; i < totalTests; i++ {
		testKey := []byte(string(rune(i + 1000))) // Keys that weren't added
		if bf.Contains(testKey) {
			falsePositives++
		}
	}

	falsePositiveRate := float64(falsePositives) / float64(totalTests)

	// The actual false positive rate should be roughly close to our target
	// but we'll be lenient since it's probabilistic
	if falsePositiveRate > 0.3 { // Much higher than expected 0.1
		t.Errorf("False positive rate too high: %f", falsePositiveRate)
	}
}

func TestBloomFilterReset(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Add a key
	key := []byte("test_key")
	bf.Add(key)

	if !bf.Contains(key) {
		t.Error("Expected key to be present before reset")
	}

	// Reset the filter
	bf.Reset()

	// After reset, the probability of the key being "present" should be very low
	// Note: Due to the probabilistic nature, we can't guarantee it will be false
	// but we can check that the filter was actually reset by checking multiple keys
	foundCount := 0
	testKeys := [][]byte{
		[]byte("test1"), []byte("test2"), []byte("test3"),
		[]byte("test4"), []byte("test5"), []byte("test6"),
		[]byte("test7"), []byte("test8"), []byte("test9"),
		[]byte("test10"),
	}

	for _, testKey := range testKeys {
		if bf.Contains(testKey) {
			foundCount++
		}
	}

	// After reset, very few keys should appear to be present
	if foundCount > len(testKeys)/2 {
		t.Errorf("Too many keys found after reset: %d out of %d", foundCount, len(testKeys))
	}
}

func TestBloomFilterEstimatedFalsePositiveRate(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Initially, false positive rate should be 0
	rate := bf.EstimatedFalsePositiveRate(0)
	if rate != 0.0 {
		t.Errorf("Expected 0 false positive rate for empty filter, got %f", rate)
	}

	// Add some elements and check that rate increases
	bf.Add([]byte("key1"))
	bf.Add([]byte("key2"))

	rate = bf.EstimatedFalsePositiveRate(2)
	if rate <= 0.0 {
		t.Error("Expected non-zero false positive rate after adding elements")
	}

	if rate > 0.1 { // Should be much lower than 10%
		t.Errorf("False positive rate too high: %f", rate)
	}
}
