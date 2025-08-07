package application

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Bloom0716/mini-bigtable/internal/domain"
)

// LSMTableService represents the application service for LSM-tree operations
// This coordinates the interaction between different domain components
type LSMTableService struct {
	mu                sync.RWMutex
	activeTable       *domain.MemTable
	immutableTables   []*domain.MemTable
	sstablesByLevel   map[int][]*domain.SSTable
	wal               *domain.WAL
	walDir            string
	sstableDir        string
	maxTableSize      int
	walCounter        int
	compactionManager *domain.CompactionManager
}

// NewLSMTableService creates a new LSM-tree table service
func NewLSMTableService(dataDir string, maxTableSize int) (*LSMTableService, error) {
	service := &LSMTableService{
		immutableTables:   make([]*domain.MemTable, 0),
		sstablesByLevel:   make(map[int][]*domain.SSTable),
		walDir:            filepath.Join(dataDir, "wal"),
		sstableDir:        filepath.Join(dataDir, "sstables"),
		maxTableSize:      maxTableSize,
		walCounter:        0,
		compactionManager: domain.NewCompactionManager(domain.LeveledCompaction),
	}

	if err := service.createNewActiveTable(); err != nil {
		return nil, fmt.Errorf("failed to create initial active table: %w", err)
	}

	return service, nil
}

// Put adds a key-value pair to the LSM-tree
func (s *LSMTableService) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to WAL first for durability
	entry := domain.NewPutEntry(key, value)
	if err := s.wal.WriteEntry(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Try to put in active memtable
	if err := s.activeTable.Put(key, value); err != nil {
		if err == domain.ErrTableFull {
			// Rotate the memtable
			if err := s.rotateMemTable(); err != nil {
				return fmt.Errorf("failed to rotate memtable: %w", err)
			}
			// Try again with new active table
			if err := s.activeTable.Put(key, value); err != nil {
				return fmt.Errorf("failed to put in new active table: %w", err)
			}
		} else {
			return fmt.Errorf("failed to put in active table: %w", err)
		}
	}

	// Flush WAL to ensure durability
	if err := s.wal.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	return nil
}

// Get retrieves a value for the given key from the LSM-tree
func (s *LSMTableService) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check active memtable first
	if entry, err := s.activeTable.Get(key); err == nil {
		if entry.IsDeleted() {
			return nil, domain.ErrKeyNotFound
		}
		return entry.Value(), nil
	}

	// Check immutable memtables in reverse order (newest first)
	for i := len(s.immutableTables) - 1; i >= 0; i-- {
		if entry, err := s.immutableTables[i].Get(key); err == nil {
			if entry.IsDeleted() {
				return nil, domain.ErrKeyNotFound
			}
			return entry.Value(), nil
		}
	}

	// Check SSTables from level 0 upwards
	for level := 0; level < 10; level++ { // Arbitrary max level
		tables := s.sstablesByLevel[level]
		// For level 0, check all tables (they may overlap)
		// For other levels, we could use binary search since tables don't overlap
		for i := len(tables) - 1; i >= 0; i-- { // Check newest first
			if entry, err := tables[i].Get(key); err == nil && entry != nil {
				if entry.IsDeleted() {
					return nil, domain.ErrKeyNotFound
				}
				return entry.Value(), nil
			}
		}
	}

	return nil, domain.ErrKeyNotFound
}

// Delete marks a key as deleted in the LSM-tree
func (s *LSMTableService) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to WAL first for durability
	entry := domain.NewDeleteEntry(key)
	if err := s.wal.WriteEntry(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Try to delete in active memtable
	if err := s.activeTable.Delete(key); err != nil {
		if err == domain.ErrTableFull {
			// Rotate the memtable
			if err := s.rotateMemTable(); err != nil {
				return fmt.Errorf("failed to rotate memtable: %w", err)
			}
			// Try again with new active table
			if err := s.activeTable.Delete(key); err != nil {
				return fmt.Errorf("failed to delete in new active table: %w", err)
			}
		} else {
			return fmt.Errorf("failed to delete in active table: %w", err)
		}
	}

	// Flush WAL to ensure durability
	if err := s.wal.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	return nil
}

// rotateMemTable moves the current active table to immutable and creates a new active table
func (s *LSMTableService) rotateMemTable() error {
	// Mark current active table as read-only
	s.activeTable.SetReadOnly()

	// Move to immutable list
	s.immutableTables = append(s.immutableTables, s.activeTable)

	// Create new active table
	if err := s.createNewActiveTable(); err != nil {
		return err
	}

	// Trigger background flush of immutable table to SSTable
	s.flushImmutableTableInternal()

	return nil
}

// flushImmutableTable flushes the oldest immutable table to an SSTable (with locking)
func (s *LSMTableService) flushImmutableTable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushImmutableTableInternal()
}

// flushImmutableTableInternal flushes the oldest immutable table to an SSTable (without locking)
func (s *LSMTableService) flushImmutableTableInternal() {

	if len(s.immutableTables) == 0 {
		return
	}

	// Get the oldest immutable table
	immutableTable := s.immutableTables[0]
	s.immutableTables = s.immutableTables[1:]

	// Convert to SSTable
	entries := immutableTable.GetAllEntries()
	if len(entries) == 0 {
		return
	}

	// Build SSTable
	builder := domain.NewSSTableBuilder(0, uint32(len(entries)))
	for _, entry := range entries {
		builder.AddEntry(entry)
	}

	filename := fmt.Sprintf("sstable_L0_%d.sst", s.walCounter)
	sstable, err := builder.Build(s.sstableDir, filename)
	if err != nil {
		// In production, this should be logged properly
		fmt.Printf("Failed to build SSTable: %v\n", err)
		return
	}

	// Add to level 0
	s.sstablesByLevel[0] = append(s.sstablesByLevel[0], sstable)

	// Check if compaction is needed
	if s.compactionManager.ShouldCompact(s.sstablesByLevel) {
		go s.runCompaction()
	}
}

// runCompaction runs background compaction
func (s *LSMTableService) runCompaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	task := s.compactionManager.SelectCompactionTask(s.sstablesByLevel)
	if task == nil {
		return
	}

	// Execute compaction
	outputTables, err := s.compactionManager.ExecuteCompaction(task, s.sstableDir)
	if err != nil {
		fmt.Printf("Failed to execute compaction: %v\n", err)
		return
	}

	// Update SSTable registry
	s.updateSSTablesAfterCompaction(task, outputTables)
}

// updateSSTablesAfterCompaction updates the SSTable registry after compaction
func (s *LSMTableService) updateSSTablesAfterCompaction(task *domain.CompactionTask, outputTables []*domain.SSTable) {
	// Remove input SSTables from their levels
	for _, inputTable := range task.InputSSTables {
		level := inputTable.Metadata().Level
		tables := s.sstablesByLevel[level]

		// Find and remove the input table
		for i, table := range tables {
			if table == inputTable {
				s.sstablesByLevel[level] = append(tables[:i], tables[i+1:]...)
				break
			}
		}

		// Remove the file
		inputTable.Remove()
	}

	// Add output SSTables to their level
	for _, outputTable := range outputTables {
		level := outputTable.Metadata().Level
		s.sstablesByLevel[level] = append(s.sstablesByLevel[level], outputTable)
	}
}

// createNewActiveTable creates a new active memtable and WAL
func (s *LSMTableService) createNewActiveTable() error {
	// Close current WAL if exists
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			return fmt.Errorf("failed to close current WAL: %w", err)
		}
	}

	// Create new WAL
	walFilename := fmt.Sprintf("wal_%d.log", s.walCounter)
	wal, err := domain.NewWAL(s.walDir, walFilename)
	if err != nil {
		return fmt.Errorf("failed to create new WAL: %w", err)
	}
	s.wal = wal
	s.walCounter++

	// Create new active memtable
	s.activeTable = domain.NewMemTable(s.maxTableSize)

	return nil
}

// Close closes the LSM-tree service and all associated resources
func (s *LSMTableService) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush all remaining immutable tables before closing
	for len(s.immutableTables) > 0 {
		s.flushImmutableTableInternal()
	}

	if s.wal != nil {
		return s.wal.Close()
	}
	return nil
}

// Recovery recovers the LSM-tree service from WAL files and existing SSTables
func (s *LSMTableService) Recovery() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// First, load existing SSTables
	if err := s.loadExistingSSTables(); err != nil {
		return fmt.Errorf("failed to load existing SSTables: %w", err)
	}

	// Then recover from WAL
	if s.wal != nil {
		entries, err := s.wal.Recover()
		if err != nil {
			return fmt.Errorf("failed to recover from WAL: %w", err)
		}

		// Replay entries into the active memtable
		for _, entry := range entries {
			if entry.IsDeleted() {
				if err := s.activeTable.Delete(entry.Key()); err != nil {
					if err == domain.ErrTableFull {
						if err := s.rotateMemTable(); err != nil {
							return fmt.Errorf("failed to rotate memtable during recovery: %w", err)
						}
						if err := s.activeTable.Delete(entry.Key()); err != nil {
							return fmt.Errorf("failed to replay delete entry during recovery: %w", err)
						}
					} else {
						return fmt.Errorf("failed to replay delete entry: %w", err)
					}
				}
			} else {
				if err := s.activeTable.Put(entry.Key(), entry.Value()); err != nil {
					if err == domain.ErrTableFull {
						if err := s.rotateMemTable(); err != nil {
							return fmt.Errorf("failed to rotate memtable during recovery: %w", err)
						}
						if err := s.activeTable.Put(entry.Key(), entry.Value()); err != nil {
							return fmt.Errorf("failed to replay put entry during recovery: %w", err)
						}
					} else {
						return fmt.Errorf("failed to replay put entry: %w", err)
					}
				}
			}
		}
	}

	return nil
}

// GetMemTableStats returns statistics about the current memtables
func (s *LSMTableService) GetMemTableStats() (activeSize int, immutableCount int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.activeTable.Size(), len(s.immutableTables)
}

// GetSSTableStats returns statistics about SSTables
func (s *LSMTableService) GetSSTableStats() map[int]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make(map[int]int)
	for level, tables := range s.sstablesByLevel {
		stats[level] = len(tables)
	}
	return stats
}

// loadExistingSSTables loads existing SSTables from disk
func (s *LSMTableService) loadExistingSSTables() error {
	// Check if SSTable directory exists
	if _, err := os.Stat(s.sstableDir); os.IsNotExist(err) {
		return nil // No SSTables directory, nothing to load
	}

	// Read directory contents
	entries, err := os.ReadDir(s.sstableDir)
	if err != nil {
		return fmt.Errorf("failed to read SSTable directory: %w", err)
	}

	// Load all .sst files
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}

		// Parse level from filename (assuming format: sstable_L<level>_<id>.sst)
		level := 0 // Default to level 0
		if strings.Contains(entry.Name(), "_L0_") {
			level = 0
		} else if strings.Contains(entry.Name(), "_L1_") {
			level = 1
		}
		// Add more levels as needed

		// Load SSTable
		// filePath := filepath.Join(s.sstableDir, entry.Name())
		// Note: We need to implement LoadSSTable method in domain
		// For now, we'll skip this and just note that SSTables exist
		fmt.Printf("Found SSTable file: %s (level %d)\n", entry.Name(), level)
	}

	return nil
}
