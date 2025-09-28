package model

import (
	"fmt"
	"sort"
	"time"
)

// CompactionStrategy defines the compaction strategy
type CompactionStrategy int

const (
	SizeTieredCompaction CompactionStrategy = iota
	LeveledCompaction
)

// CompactionManager manages compaction operations
type CompactionManager struct {
	strategy          CompactionStrategy
	maxSizeLevel0     uint64
	sizeMultiplier    float64
	maxSSTablesLevel0 int
}

// NewCompactionManager creates a new compaction manager
func NewCompactionManager(strategy CompactionStrategy) *CompactionManager {
	return &CompactionManager{
		strategy:          strategy,
		maxSizeLevel0:     10 * 1024 * 1024, // 10MB
		sizeMultiplier:    10.0,
		maxSSTablesLevel0: 4,
	}
}

// CompactionTask represents a compaction operation
type CompactionTask struct {
	InputSSTables  []*SSTable
	OutputLevel    int
	CompactionType CompactionType
	EstimatedSize  uint64
}

// CompactionType defines the type of compaction
type CompactionType int

const (
	MinorCompaction CompactionType = iota // MemTable to SSTable
	MajorCompaction                       // SSTable to SSTable merge
)

// ShouldCompact determines if compaction is needed
func (cm *CompactionManager) ShouldCompact(sstablesByLevel map[int][]*SSTable) bool {
	switch cm.strategy {
	case SizeTieredCompaction:
		return cm.shouldCompactSizeTiered(sstablesByLevel)
	case LeveledCompaction:
		return cm.shouldCompactLeveled(sstablesByLevel)
	default:
		return false
	}
}

// shouldCompactSizeTiered checks if size-tiered compaction is needed
func (cm *CompactionManager) shouldCompactSizeTiered(sstablesByLevel map[int][]*SSTable) bool {
	// Check if Level 0 has too many SSTables
	level0Tables := sstablesByLevel[0]
	return len(level0Tables) >= cm.maxSSTablesLevel0
}

// shouldCompactLeveled checks if leveled compaction is needed
func (cm *CompactionManager) shouldCompactLeveled(sstablesByLevel map[int][]*SSTable) bool {
	// Check each level for size violations
	for level, tables := range sstablesByLevel {
		if level == 0 {
			// Level 0 is special - check number of SSTables
			if len(tables) >= cm.maxSSTablesLevel0 {
				return true
			}
		} else {
			// For other levels, check total size
			totalSize := cm.calculateTotalSize(tables)
			maxSize := cm.maxSizeForLevel(level)
			if totalSize > maxSize {
				return true
			}
		}
	}
	return false
}

// calculateTotalSize calculates the total size of SSTables
func (cm *CompactionManager) calculateTotalSize(tables []*SSTable) uint64 {
	var totalSize uint64
	for _, table := range tables {
		totalSize += table.metadata.FileSize
	}
	return totalSize
}

// maxSizeForLevel calculates the maximum size for a given level
func (cm *CompactionManager) maxSizeForLevel(level int) uint64 {
	if level == 0 {
		return cm.maxSizeLevel0
	}

	size := cm.maxSizeLevel0
	for i := 1; i < level; i++ {
		size = uint64(float64(size) * cm.sizeMultiplier)
	}
	return size
}

// SelectCompactionTask selects SSTables for compaction
func (cm *CompactionManager) SelectCompactionTask(sstablesByLevel map[int][]*SSTable) *CompactionTask {
	switch cm.strategy {
	case SizeTieredCompaction:
		return cm.selectSizeTieredCompaction(sstablesByLevel)
	case LeveledCompaction:
		return cm.selectLeveledCompaction(sstablesByLevel)
	default:
		return nil
	}
}

// selectSizeTieredCompaction selects SSTables for size-tiered compaction
func (cm *CompactionManager) selectSizeTieredCompaction(sstablesByLevel map[int][]*SSTable) *CompactionTask {
	// Find the level with most SSTables
	var maxLevel int
	var maxCount int

	for level, tables := range sstablesByLevel {
		if len(tables) > maxCount {
			maxCount = len(tables)
			maxLevel = level
		}
	}

	if maxCount < cm.maxSSTablesLevel0 {
		return nil
	}

	tables := sstablesByLevel[maxLevel]
	estimatedSize := cm.calculateTotalSize(tables)

	return &CompactionTask{
		InputSSTables:  tables,
		OutputLevel:    maxLevel + 1,
		CompactionType: MajorCompaction,
		EstimatedSize:  estimatedSize,
	}
}

// selectLeveledCompaction selects SSTables for leveled compaction
func (cm *CompactionManager) selectLeveledCompaction(sstablesByLevel map[int][]*SSTable) *CompactionTask {
	// Check Level 0 first
	level0Tables := sstablesByLevel[0]
	if len(level0Tables) >= cm.maxSSTablesLevel0 {
		// Compact all Level 0 tables with overlapping Level 1 tables
		level1Tables := cm.findOverlappingTables(level0Tables, sstablesByLevel[1])

		allTables := append(level0Tables, level1Tables...)
		estimatedSize := cm.calculateTotalSize(allTables)

		return &CompactionTask{
			InputSSTables:  allTables,
			OutputLevel:    1,
			CompactionType: MajorCompaction,
			EstimatedSize:  estimatedSize,
		}
	}

	// Check other levels
	for level := 1; level < 10; level++ { // Arbitrary max level
		tables := sstablesByLevel[level]
		if len(tables) == 0 {
			continue
		}

		totalSize := cm.calculateTotalSize(tables)
		maxSize := cm.maxSizeForLevel(level)

		if totalSize > maxSize {
			// Select oldest table for compaction
			sort.Slice(tables, func(i, j int) bool {
				return tables[i].metadata.CreatedAt.Before(tables[j].metadata.CreatedAt)
			})

			selectedTable := []*SSTable{tables[0]}
			nextLevelTables := cm.findOverlappingTables(selectedTable, sstablesByLevel[level+1])

			allTables := append(selectedTable, nextLevelTables...)
			estimatedSize := cm.calculateTotalSize(allTables)

			return &CompactionTask{
				InputSSTables:  allTables,
				OutputLevel:    level + 1,
				CompactionType: MajorCompaction,
				EstimatedSize:  estimatedSize,
			}
		}
	}

	return nil
}

// findOverlappingTables finds SSTables that overlap with the given tables
func (cm *CompactionManager) findOverlappingTables(inputTables, candidateTables []*SSTable) []*SSTable {
	if len(inputTables) == 0 || len(candidateTables) == 0 {
		return []*SSTable{}
	}

	// Find min and max keys from input tables
	var minKey, maxKey []byte
	for i, table := range inputTables {
		if i == 0 {
			minKey = table.metadata.MinKey
			maxKey = table.metadata.MaxKey
		} else {
			if compareKeys(table.metadata.MinKey, minKey) < 0 {
				minKey = table.metadata.MinKey
			}
			if compareKeys(table.metadata.MaxKey, maxKey) > 0 {
				maxKey = table.metadata.MaxKey
			}
		}
	}

	// Find overlapping tables
	var overlapping []*SSTable
	for _, table := range candidateTables {
		if cm.keyRangesOverlap(minKey, maxKey, table.metadata.MinKey, table.metadata.MaxKey) {
			overlapping = append(overlapping, table)
		}
	}

	return overlapping
}

// keyRangesOverlap checks if two key ranges overlap
func (cm *CompactionManager) keyRangesOverlap(min1, max1, min2, max2 []byte) bool {
	// Range 1: [min1, max1], Range 2: [min2, max2]
	// They overlap if: max1 >= min2 && max2 >= min1
	return compareKeys(max1, min2) >= 0 && compareKeys(max2, min1) >= 0
}

// compareKeys compares two keys
func compareKeys(key1, key2 []byte) int {
	if len(key1) < len(key2) {
		return -1
	}
	if len(key1) > len(key2) {
		return 1
	}

	for i := 0; i < len(key1); i++ {
		if key1[i] < key2[i] {
			return -1
		}
		if key1[i] > key2[i] {
			return 1
		}
	}
	return 0
}

// ExecuteCompaction executes a compaction task
func (cm *CompactionManager) ExecuteCompaction(task *CompactionTask, outputDir string) ([]*SSTable, error) {
	if len(task.InputSSTables) == 0 {
		return nil, fmt.Errorf("no input SSTables for compaction")
	}

	// Collect all entries from input SSTables
	allEntries := make([]*Entry, 0)

	for _, sstable := range task.InputSSTables {
		entries, err := sstable.GetAllEntries()
		if err != nil {
			return nil, fmt.Errorf("failed to read entries from SSTable: %w", err)
		}
		allEntries = append(allEntries, entries...)
	}

	// Sort entries by key, then by timestamp (newest first)
	sort.Slice(allEntries, func(i, j int) bool {
		cmp := allEntries[i].Compare(allEntries[j])
		if cmp == 0 {
			// Same key, prefer newer timestamp
			return allEntries[i].IsNewerThan(allEntries[j])
		}
		return cmp < 0
	})

	// Remove duplicates and tombstones
	compactedEntries := cm.removeDuplicatesAndTombstones(allEntries)

	// Build new SSTables
	if len(compactedEntries) == 0 {
		return []*SSTable{}, nil
	}

	// Split into multiple SSTables if necessary (simple implementation: one SSTable)
	builder := NewSSTableBuilder(task.OutputLevel, uint32(len(compactedEntries)))

	for _, entry := range compactedEntries {
		builder.AddEntry(entry)
	}

	filename := fmt.Sprintf("sstable_level_%d_%d.sst", task.OutputLevel, time.Now().UnixNano())
	newSSTable, err := builder.Build(outputDir, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to build compacted SSTable: %w", err)
	}

	return []*SSTable{newSSTable}, nil
}

// removeDuplicatesAndTombstones removes duplicate keys and handles tombstones
func (cm *CompactionManager) removeDuplicatesAndTombstones(entries []*Entry) []*Entry {
	if len(entries) == 0 {
		return entries
	}

	result := make([]*Entry, 0, len(entries))
	seenKeys := make(map[string]bool)

	for _, entry := range entries {
		keyStr := string(entry.Key())

		// Skip if we've already seen this key (entries are sorted by key, then timestamp)
		if seenKeys[keyStr] {
			continue
		}
		seenKeys[keyStr] = true

		// Skip tombstones (deleted entries)
		if entry.IsDeleted() {
			continue
		}

		result = append(result, entry)
	}

	return result
}
