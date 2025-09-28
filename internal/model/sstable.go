package model

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// SSTableMetadata contains metadata about an SSTable
type SSTableMetadata struct {
	Level       int
	FileName    string
	MinKey      []byte
	MaxKey      []byte
	EntryCount  uint32
	FileSize    uint64
	CreatedAt   time.Time
	BloomFilter *BloomFilter
	BlockIndex  *BlockIndex
}

// SSTable represents an immutable sorted string table on disk
type SSTable struct {
	metadata *SSTableMetadata
	filePath string
}

// SSTableBuilder builds SSTables from entries
type SSTableBuilder struct {
	entries     []*Entry
	bloomFilter *BloomFilter
	blockIndex  *BlockIndex
	level       int
	blockSize   int
}

// NewSSTableBuilder creates a new SSTable builder
func NewSSTableBuilder(level int, estimatedEntries uint32) *SSTableBuilder {
	blockSize := 100 // エントリ100個ごとにインデックスを作成
	return &SSTableBuilder{
		entries:     make([]*Entry, 0),
		bloomFilter: NewBloomFilter(estimatedEntries, 0.01),
		blockIndex:  NewBlockIndex(blockSize),
		level:       level,
		blockSize:   blockSize,
	}
}

// AddEntry adds an entry to the builder
func (builder *SSTableBuilder) AddEntry(entry *Entry) {
	builder.entries = append(builder.entries, entry)
	builder.bloomFilter.Add(entry.Key())
}

// Build creates an SSTable file from the collected entries
func (builder *SSTableBuilder) Build(dir, filename string) (*SSTable, error) {
	if len(builder.entries) == 0 {
		return nil, fmt.Errorf("cannot build SSTable with no entries")
	}

	// Sort entries by key
	sort.Slice(builder.entries, func(i, j int) bool {
		return builder.entries[i].Compare(builder.entries[j]) < 0
	})

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	filePath := filepath.Join(dir, filename)
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	var currentOffset uint64 = 0

	// Write entries and build block index
	for i, entry := range builder.entries {
		// Add to block index every blockSize entries
		if i%builder.blockSize == 0 {
			builder.blockIndex.AddEntry(entry.Key(), currentOffset)
		}

		entryStartOffset := currentOffset
		if err := builder.writeEntry(writer, entry); err != nil {
			return nil, fmt.Errorf("failed to write entry: %w", err)
		}

		// Calculate the size of this entry for offset tracking
		entrySize := uint64(4 + len(entry.Key()) + 4 + len(entry.Value()) + 1 + 8) // keyLen + key + valueLen + value + entryType + timestamp
		currentOffset = entryStartOffset + entrySize
	}

	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush writer: %w", err)
	}

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	// Create metadata
	metadata := &SSTableMetadata{
		Level:       builder.level,
		FileName:    filename,
		MinKey:      builder.entries[0].Key(),
		MaxKey:      builder.entries[len(builder.entries)-1].Key(),
		EntryCount:  uint32(len(builder.entries)),
		FileSize:    uint64(fileInfo.Size()),
		CreatedAt:   time.Now(),
		BloomFilter: builder.bloomFilter,
		BlockIndex:  builder.blockIndex,
	}

	return &SSTable{
		metadata: metadata,
		filePath: filePath,
	}, nil
}

// writeEntry writes a single entry to the writer
func (builder *SSTableBuilder) writeEntry(writer *bufio.Writer, entry *Entry) error {
	// Entry format: [keyLen][key][valueLen][value][entryType][timestamp]

	// Write key length and key
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(entry.key))); err != nil {
		return err
	}
	if _, err := writer.Write(entry.key); err != nil {
		return err
	}

	// Write value length and value
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(entry.value))); err != nil {
		return err
	}
	if _, err := writer.Write(entry.value); err != nil {
		return err
	}

	// Write entry type
	if err := binary.Write(writer, binary.LittleEndian, uint8(entry.entryType)); err != nil {
		return err
	}

	// Write timestamp
	if err := binary.Write(writer, binary.LittleEndian, entry.timestamp.UnixNano()); err != nil {
		return err
	}

	return nil
}

// LoadSSTable loads an existing SSTable from disk
func LoadSSTable(filePath string, metadata *SSTableMetadata) *SSTable {
	return &SSTable{
		metadata: metadata,
		filePath: filePath,
	}
}

// Get retrieves an entry by key from the SSTable
func (sst *SSTable) Get(key []byte) (*Entry, error) {
	// First check bloom filter
	if !sst.metadata.BloomFilter.Contains(key) {
		return nil, ErrKeyNotFound
	}

	// Open file and search for the key
	file, err := os.Open(sst.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %w", err)
	}
	defer file.Close()

	// Use block index to find starting position
	startOffset := uint64(0)
	if sst.metadata.BlockIndex != nil {
		startOffset = sst.metadata.BlockIndex.FindOffset(key)
	}

	// Seek to the starting position
	if _, err := file.Seek(int64(startOffset), 0); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d: %w", startOffset, err)
	}

	reader := bufio.NewReader(file)

	// Search from the starting position
	for {
		entry, err := sst.readEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read entry: %w", err)
		}

		cmp := entry.Compare(&Entry{key: key})
		if cmp == 0 {
			return entry, nil
		}
		if cmp > 0 {
			// Keys are sorted, so we've passed the target key
			break
		}
	}

	return nil, ErrKeyNotFound
}

// readEntry reads a single entry from the reader
func (sst *SSTable) readEntry(reader *bufio.Reader) (*Entry, error) {
	// Read key length
	var keyLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
		return nil, err
	}

	// Read key
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, err
	}

	// Read value length
	var valueLen uint32
	if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
		return nil, err
	}

	// Read value
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, err
	}

	// Read entry type
	var entryType uint8
	if err := binary.Read(reader, binary.LittleEndian, &entryType); err != nil {
		return nil, err
	}

	// Read timestamp
	var timestampNano int64
	if err := binary.Read(reader, binary.LittleEndian, &timestampNano); err != nil {
		return nil, err
	}

	entry := &Entry{
		key:       key,
		value:     value,
		entryType: EntryType(entryType),
		timestamp: time.Unix(0, timestampNano),
	}

	return entry, nil
}

// GetAllEntries returns all entries in the SSTable (for compaction)
func (sst *SSTable) GetAllEntries() ([]*Entry, error) {
	file, err := os.Open(sst.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var entries []*Entry

	for {
		entry, err := sst.readEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read entry: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Metadata returns the metadata of the SSTable
func (sst *SSTable) Metadata() *SSTableMetadata {
	return sst.metadata
}

// Remove removes the SSTable file from disk
func (sst *SSTable) Remove() error {
	return os.Remove(sst.filePath)
}

// Iterator creates an iterator for the SSTable
func (sst *SSTable) Iterator() (*SSTableIterator, error) {
	file, err := os.Open(sst.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %w", err)
	}

	return &SSTableIterator{
		sst:    sst,
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

// SSTableIterator provides sequential access to SSTable entries
type SSTableIterator struct {
	sst     *SSTable
	file    *os.File
	reader  *bufio.Reader
	current *Entry
	err     error
}

// Next advances the iterator to the next entry
func (it *SSTableIterator) Next() bool {
	entry, err := it.sst.readEntry(it.reader)
	if err != nil {
		it.err = err
		return false
	}

	it.current = entry
	return true
}

// Entry returns the current entry
func (it *SSTableIterator) Entry() *Entry {
	return it.current
}

// Error returns any error that occurred during iteration
func (it *SSTableIterator) Error() error {
	if it.err == io.EOF {
		return nil
	}
	return it.err
}

// Close closes the iterator
func (it *SSTableIterator) Close() error {
	return it.file.Close()
}
