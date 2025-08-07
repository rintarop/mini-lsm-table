package domain

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// WAL represents a Write-Ahead Log
// This is a domain service responsible for durability
type WAL struct {
	file   *os.File
	writer *bufio.Writer
	path   string
}

// NewWAL creates a new WAL with the specified file path
func NewWAL(dir, filename string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	path := filepath.Join(dir, filename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
		path:   path,
	}, nil
}

// WriteEntry writes an entry to the WAL
func (w *WAL) WriteEntry(entry *Entry) error {
	// Entry format: [keyLen][key][valueLen][value][entryType][timestamp]

	// Write key length and key
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(entry.key))); err != nil {
		return fmt.Errorf("failed to write key length: %w", err)
	}
	if _, err := w.writer.Write(entry.key); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}

	// Write value length and value
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(entry.value))); err != nil {
		return fmt.Errorf("failed to write value length: %w", err)
	}
	if _, err := w.writer.Write(entry.value); err != nil {
		return fmt.Errorf("failed to write value: %w", err)
	}

	// Write entry type
	if err := binary.Write(w.writer, binary.LittleEndian, uint8(entry.entryType)); err != nil {
		return fmt.Errorf("failed to write entry type: %w", err)
	}

	// Write timestamp (Unix nano)
	if err := binary.Write(w.writer, binary.LittleEndian, entry.timestamp.UnixNano()); err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}

	return nil
}

// Flush flushes the buffered writes to disk
func (w *WAL) Flush() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}
	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Recover reads entries from the WAL file and returns them
func (w *WAL) Recover() ([]*Entry, error) {
	// Close current file and reopen for reading
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("failed to close WAL for recovery: %w", err)
	}

	file, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return []*Entry{}, nil // No WAL file exists, return empty slice
		}
		return nil, fmt.Errorf("failed to open WAL for recovery: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var entries []*Entry

	for {
		entry, err := w.readEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read entry from WAL: %w", err)
		}
		entries = append(entries, entry)
	}

	// Reopen file for writing
	w.file, err = os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to reopen WAL for writing: %w", err)
	}
	w.writer = bufio.NewWriter(w.file)

	return entries, nil
}

// readEntry reads a single entry from the reader
func (w *WAL) readEntry(reader *bufio.Reader) (*Entry, error) {
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

// Remove removes the WAL file
func (w *WAL) Remove() error {
	if err := w.Close(); err != nil {
		return err
	}
	return os.Remove(w.path)
}
