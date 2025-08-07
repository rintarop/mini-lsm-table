package domain

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// IndexEntry represents an entry in the block index
type IndexEntry struct {
	Key    []byte // キー
	Offset uint64 // ファイル内のオフセット位置
}

// BlockIndex represents a sparse index for efficient SSTable lookups
type BlockIndex struct {
	entries   []IndexEntry
	blockSize int // エントリ数での間隔（例：100エントリごとにインデックス作成）
}

// NewBlockIndex creates a new block index
func NewBlockIndex(blockSize int) *BlockIndex {
	return &BlockIndex{
		entries:   make([]IndexEntry, 0),
		blockSize: blockSize,
	}
}

// AddEntry adds an index entry
func (idx *BlockIndex) AddEntry(key []byte, offset uint64) {
	// キーのコピーを作成（スライスの参照問題を避けるため）
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	idx.entries = append(idx.entries, IndexEntry{
		Key:    keyCopy,
		Offset: offset,
	})
}

// FindOffset finds the best starting offset for a given key
// Returns the offset to start searching from
func (idx *BlockIndex) FindOffset(targetKey []byte) uint64 {
	if len(idx.entries) == 0 {
		return 0
	}

	// Binary search to find the largest index entry with key <= targetKey
	left, right := 0, len(idx.entries)-1
	bestOffset := uint64(0)

	for left <= right {
		mid := left + (right-left)/2
		cmp := bytes.Compare(idx.entries[mid].Key, targetKey)

		if cmp <= 0 {
			// This entry's key <= targetKey, so it's a candidate
			bestOffset = idx.entries[mid].Offset
			left = mid + 1
		} else {
			// This entry's key > targetKey, search left
			right = mid - 1
		}
	}

	return bestOffset
}

// GetEntries returns all index entries (for serialization)
func (idx *BlockIndex) GetEntries() []IndexEntry {
	return idx.entries
}

// SerializeIndex serializes the index to a writer
func (idx *BlockIndex) SerializeIndex(writer io.Writer) error {
	// Write number of entries
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(idx.entries))); err != nil {
		return fmt.Errorf("failed to write entry count: %w", err)
	}

	// Write each entry
	for _, entry := range idx.entries {
		// Write key length and key
		if err := binary.Write(writer, binary.LittleEndian, uint32(len(entry.Key))); err != nil {
			return fmt.Errorf("failed to write key length: %w", err)
		}
		if _, err := writer.Write(entry.Key); err != nil {
			return fmt.Errorf("failed to write key: %w", err)
		}

		// Write offset
		if err := binary.Write(writer, binary.LittleEndian, entry.Offset); err != nil {
			return fmt.Errorf("failed to write offset: %w", err)
		}
	}

	return nil
}

// DeserializeIndex deserializes the index from a reader
func DeserializeIndex(reader io.Reader, blockSize int) (*BlockIndex, error) {
	index := NewBlockIndex(blockSize)

	// Read number of entries
	var entryCount uint32
	if err := binary.Read(reader, binary.LittleEndian, &entryCount); err != nil {
		return nil, fmt.Errorf("failed to read entry count: %w", err)
	}

	// Read each entry
	for i := uint32(0); i < entryCount; i++ {
		// Read key length
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read key length: %w", err)
		}

		// Read key
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}

		// Read offset
		var offset uint64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			return nil, fmt.Errorf("failed to read offset: %w", err)
		}

		index.AddEntry(key, offset)
	}

	return index, nil
}

// Size returns the number of index entries
func (idx *BlockIndex) Size() int {
	return len(idx.entries)
}
