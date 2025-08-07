package domain

import (
	"bytes"
	"time"
)

// EntryType represents the type of an entry
type EntryType uint8

const (
	EntryTypePut EntryType = iota
	EntryTypeDelete
)

// Entry represents a key-value entry in the LSM-tree
// This is a domain entity that encapsulates the core business logic
type Entry struct {
	key       []byte
	value     []byte
	entryType EntryType
	timestamp time.Time
}

// NewPutEntry creates a new PUT entry
func NewPutEntry(key, value []byte) *Entry {
	return &Entry{
		key:       key,
		value:     value,
		entryType: EntryTypePut,
		timestamp: time.Now(),
	}
}

// NewDeleteEntry creates a new DELETE entry (tombstone)
func NewDeleteEntry(key []byte) *Entry {
	return &Entry{
		key:       key,
		value:     nil,
		entryType: EntryTypeDelete,
		timestamp: time.Now(),
	}
}

// Key returns the key of the entry
func (e *Entry) Key() []byte {
	return e.key
}

// Value returns the value of the entry
func (e *Entry) Value() []byte {
	return e.value
}

// Type returns the type of the entry
func (e *Entry) Type() EntryType {
	return e.entryType
}

// Timestamp returns the timestamp of the entry
func (e *Entry) Timestamp() time.Time {
	return e.timestamp
}

// IsDeleted returns true if this entry is a delete marker
func (e *Entry) IsDeleted() bool {
	return e.entryType == EntryTypeDelete
}

// Compare compares this entry with another entry by key
// Returns -1 if this entry's key is less than other's key,
// 0 if they are equal, and 1 if this entry's key is greater
func (e *Entry) Compare(other *Entry) int {
	return bytes.Compare(e.key, other.key)
}

// IsNewerThan returns true if this entry is newer than the other entry
func (e *Entry) IsNewerThan(other *Entry) bool {
	return e.timestamp.After(other.timestamp)
}
