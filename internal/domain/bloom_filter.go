package domain

import (
	"hash/fnv"
	"math"
)

// BloomFilter represents a probabilistic data structure for membership testing
type BloomFilter struct {
	bitArray  []bool
	size      uint32
	hashFuncs int
}

// NewBloomFilter creates a new bloom filter with specified capacity and false positive rate
func NewBloomFilter(capacity uint32, falsePositiveRate float64) *BloomFilter {
	// Calculate optimal size and hash function count
	size := uint32(-float64(capacity) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2)))
	hashFuncs := int(float64(size) * math.Log(2) / float64(capacity))

	if hashFuncs < 1 {
		hashFuncs = 1
	}

	return &BloomFilter{
		bitArray:  make([]bool, size),
		size:      size,
		hashFuncs: hashFuncs,
	}
}

// Add adds a key to the bloom filter
func (bf *BloomFilter) Add(key []byte) {
	hashes := bf.getHashes(key)
	for i := 0; i < bf.hashFuncs; i++ {
		index := (hashes[0] + uint32(i)*hashes[1]) % bf.size
		bf.bitArray[index] = true
	}
}

// Contains checks if a key might be in the filter
// Returns false if definitely not present, true if possibly present
func (bf *BloomFilter) Contains(key []byte) bool {
	hashes := bf.getHashes(key)
	for i := 0; i < bf.hashFuncs; i++ {
		index := (hashes[0] + uint32(i)*hashes[1]) % bf.size
		if !bf.bitArray[index] {
			return false
		}
	}
	return true
}

// getHashes generates two hash values for double hashing
func (bf *BloomFilter) getHashes(key []byte) [2]uint32 {
	h1 := fnv.New32a()
	h1.Write(key)
	hash1 := h1.Sum32()

	h2 := fnv.New32()
	h2.Write(key)
	hash2 := h2.Sum32()

	return [2]uint32{hash1, hash2}
}

// Reset clears all bits in the bloom filter
func (bf *BloomFilter) Reset() {
	for i := range bf.bitArray {
		bf.bitArray[i] = false
	}
}

// EstimatedFalsePositiveRate returns the current estimated false positive rate
func (bf *BloomFilter) EstimatedFalsePositiveRate(insertedElements uint32) float64 {
	if insertedElements == 0 {
		return 0.0
	}

	// Calculate the probability that a bit is still 0
	probability := math.Pow(1.0-1.0/float64(bf.size), float64(bf.hashFuncs)*float64(insertedElements))

	// False positive rate is (1 - probability)^k where k is number of hash functions
	return math.Pow(1.0-probability, float64(bf.hashFuncs))
}
