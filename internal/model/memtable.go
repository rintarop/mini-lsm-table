package model

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrTableFull   = errors.New("memtable is full")
)

// MemTable represents an in-memory table that stores entries
// This is an aggregate root in DDD terms
type MemTable struct {
	mu       sync.RWMutex
	entries  map[string]*Entry // Using map for simplicity; in production, use skip list
	maxSize  int
	size     int
	readOnly bool
}

// NewMemTable creates a new MemTable with the specified maximum size
func NewMemTable(maxSize int) *MemTable {
	return &MemTable{
		entries:  make(map[string]*Entry),
		maxSize:  maxSize,
		size:     0,
		readOnly: false,
	}
}

// Put adds or updates an entry in the MemTable
func (mt *MemTable) Put(key, value []byte) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.readOnly {
		return errors.New("memtable is read-only")
	}

	keyStr := string(key)
	entry := NewPutEntry(key, value)

	// Check if we're adding a new key and if we have space
	if _, exists := mt.entries[keyStr]; !exists && mt.size >= mt.maxSize {
		return ErrTableFull
	}

	// If key doesn't exist, increment size
	if _, exists := mt.entries[keyStr]; !exists {
		mt.size++
	}

	mt.entries[keyStr] = entry
	return nil
}

// Delete marks an entry as deleted by adding a tombstone
func (mt *MemTable) Delete(key []byte) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.readOnly {
		return errors.New("memtable is read-only")
	}

	keyStr := string(key)
	entry := NewDeleteEntry(key)

	// Check if we're adding a new key and if we have space
	if _, exists := mt.entries[keyStr]; !exists && mt.size >= mt.maxSize {
		return ErrTableFull
	}

	// If key doesn't exist, increment size
	if _, exists := mt.entries[keyStr]; !exists {
		mt.size++
	}

	mt.entries[keyStr] = entry
	return nil
}

// Get retrieves an entry from the MemTable
func (mt *MemTable) Get(key []byte) (*Entry, error) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	entry, exists := mt.entries[string(key)]
	if !exists {
		return nil, ErrKeyNotFound
	}

	return entry, nil
}

// Size returns the current number of entries in the MemTable
func (mt *MemTable) Size() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

// IsFull returns true if the MemTable has reached its maximum capacity
func (mt *MemTable) IsFull() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size >= mt.maxSize
}

// SetReadOnly marks the MemTable as read-only (used during flushing)
func (mt *MemTable) SetReadOnly() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.readOnly = true
}

// GetAllEntries returns all entries in the MemTable (used for flushing to disk)
func (mt *MemTable) GetAllEntries() []*Entry {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	entries := make([]*Entry, 0, len(mt.entries))
	for _, entry := range mt.entries {
		entries = append(entries, entry)
	}

	return entries
}

// IsReadOnly returns true if the MemTable is read-only
func (mt *MemTable) IsReadOnly() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.readOnly
}
