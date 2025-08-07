package domain

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestCompactionManagerShouldCompact(t *testing.T) {
	cm := NewCompactionManager(LeveledCompaction)

	// Test case: Level 0 has too many SSTables
	sstablesByLevel := make(map[int][]*SSTable)

	// Create mock SSTables for Level 0
	for i := 0; i < 5; i++ { // More than maxSSTablesLevel0 (4)
		metadata := &SSTableMetadata{
			Level:     0,
			FileName:  "test.sst",
			FileSize:  1024,
			CreatedAt: time.Now(),
		}
		sstable := &SSTable{metadata: metadata}
		sstablesByLevel[0] = append(sstablesByLevel[0], sstable)
	}

	if !cm.ShouldCompact(sstablesByLevel) {
		t.Error("Expected compaction to be needed for Level 0 with too many SSTables")
	}

	// Test case: Level 0 has acceptable number of SSTables
	sstablesByLevel[0] = sstablesByLevel[0][:3] // Reduce to 3 SSTables

	if cm.ShouldCompact(sstablesByLevel) {
		t.Error("Expected no compaction needed for Level 0 with acceptable number of SSTables")
	}
}

func TestCompactionManagerSelectTask(t *testing.T) {
	cm := NewCompactionManager(LeveledCompaction)
	sstablesByLevel := make(map[int][]*SSTable)

	// Create SSTables for Level 0 that need compaction
	for i := 0; i < 5; i++ {
		metadata := &SSTableMetadata{
			Level:     0,
			FileName:  "test.sst",
			MinKey:    []byte{byte(i)},
			MaxKey:    []byte{byte(i + 1)},
			FileSize:  1024,
			CreatedAt: time.Now().Add(-time.Duration(i) * time.Hour), // Different timestamps
		}
		sstable := &SSTable{metadata: metadata}
		sstablesByLevel[0] = append(sstablesByLevel[0], sstable)
	}

	// Create some Level 1 SSTables
	for i := 0; i < 2; i++ {
		metadata := &SSTableMetadata{
			Level:     1,
			FileName:  "test1.sst",
			MinKey:    []byte{byte(i)},
			MaxKey:    []byte{byte(i + 2)},
			FileSize:  2048,
			CreatedAt: time.Now(),
		}
		sstable := &SSTable{metadata: metadata}
		sstablesByLevel[1] = append(sstablesByLevel[1], sstable)
	}

	task := cm.SelectCompactionTask(sstablesByLevel)
	if task == nil {
		t.Fatal("Expected compaction task to be selected")
	}

	if task.OutputLevel != 1 {
		t.Errorf("Expected output level 1, got %d", task.OutputLevel)
	}

	if task.CompactionType != MajorCompaction {
		t.Errorf("Expected MajorCompaction, got %v", task.CompactionType)
	}

	if len(task.InputSSTables) == 0 {
		t.Error("Expected input SSTables to be selected")
	}
}

func TestCompactionExecution(t *testing.T) {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "compaction_test")
	defer os.RemoveAll(tmpDir)

	cm := NewCompactionManager(LeveledCompaction)

	// Create input SSTables with overlapping and duplicate data
	inputTables := make([]*SSTable, 0)

	// First SSTable
	builder1 := NewSSTableBuilder(0, 5)
	builder1.AddEntry(NewPutEntry([]byte("key1"), []byte("value1_old")))
	builder1.AddEntry(NewPutEntry([]byte("key2"), []byte("value2")))
	builder1.AddEntry(NewDeleteEntry([]byte("key3")))

	sst1, err := builder1.Build(tmpDir, "input1.sst")
	if err != nil {
		t.Fatalf("Failed to build first SSTable: %v", err)
	}
	inputTables = append(inputTables, sst1)

	// Second SSTable with some overlapping keys
	time.Sleep(1 * time.Millisecond) // Ensure different timestamp
	builder2 := NewSSTableBuilder(0, 5)
	builder2.AddEntry(NewPutEntry([]byte("key1"), []byte("value1_new"))) // Newer version
	builder2.AddEntry(NewPutEntry([]byte("key4"), []byte("value4")))
	builder2.AddEntry(NewPutEntry([]byte("key5"), []byte("value5")))

	sst2, err := builder2.Build(tmpDir, "input2.sst")
	if err != nil {
		t.Fatalf("Failed to build second SSTable: %v", err)
	}
	inputTables = append(inputTables, sst2)

	// Create compaction task
	task := &CompactionTask{
		InputSSTables:  inputTables,
		OutputLevel:    1,
		CompactionType: MajorCompaction,
	}

	// Execute compaction
	outputTables, err := cm.ExecuteCompaction(task, tmpDir)
	if err != nil {
		t.Fatalf("Failed to execute compaction: %v", err)
	}

	if len(outputTables) != 1 {
		t.Errorf("Expected 1 output table, got %d", len(outputTables))
	}

	outputTable := outputTables[0]

	// Verify compacted data
	// key1 should have the newer value
	entry, err := outputTable.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1 from compacted table: %v", err)
	}
	if string(entry.Value()) != "value1_new" {
		t.Errorf("Expected 'value1_new', got '%s'", entry.Value())
	}

	// key2 should exist
	_, err = outputTable.Get([]byte("key2"))
	if err != nil {
		t.Errorf("Expected key2 to exist in compacted table: %v", err)
	}

	// key3 should not exist (was deleted)
	_, err = outputTable.Get([]byte("key3"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected key3 to be removed from compacted table")
	}

	// key4 and key5 should exist
	for _, key := range []string{"key4", "key5"} {
		_, err = outputTable.Get([]byte(key))
		if err != nil {
			t.Errorf("Expected %s to exist in compacted table: %v", key, err)
		}
	}
}

func TestKeyRangeOverlap(t *testing.T) {
	cm := NewCompactionManager(LeveledCompaction)

	testCases := []struct {
		min1, max1, min2, max2 []byte
		shouldOverlap          bool
		description            string
	}{
		{
			[]byte("a"), []byte("d"),
			[]byte("c"), []byte("f"),
			true, "overlapping ranges",
		},
		{
			[]byte("a"), []byte("c"),
			[]byte("d"), []byte("f"),
			false, "non-overlapping ranges",
		},
		{
			[]byte("a"), []byte("d"),
			[]byte("d"), []byte("f"),
			true, "touching ranges",
		},
		{
			[]byte("b"), []byte("c"),
			[]byte("a"), []byte("d"),
			true, "range2 contains range1",
		},
		{
			[]byte("a"), []byte("d"),
			[]byte("b"), []byte("c"),
			true, "range1 contains range2",
		},
	}

	for _, tc := range testCases {
		result := cm.keyRangesOverlap(tc.min1, tc.max1, tc.min2, tc.max2)
		if result != tc.shouldOverlap {
			t.Errorf("%s: expected %v, got %v", tc.description, tc.shouldOverlap, result)
		}
	}
}

func TestRemoveDuplicatesAndTombstones(t *testing.T) {
	cm := NewCompactionManager(LeveledCompaction)

	// Create entries with duplicates and tombstones
	// Make sure to have different timestamps for proper sorting
	entries := []*Entry{
		NewPutEntry([]byte("key1"), []byte("value1_old")),
		NewPutEntry([]byte("key2"), []byte("value2")),
		NewDeleteEntry([]byte("key3")), // Tombstone
		NewPutEntry([]byte("key4"), []byte("value4")),
	}

	// Add newer versions after a delay to ensure different timestamps
	time.Sleep(1 * time.Millisecond)
	entries = append(entries, NewPutEntry([]byte("key1"), []byte("value1_new"))) // Duplicate key (newer)

	time.Sleep(1 * time.Millisecond)
	entries = append(entries, NewDeleteEntry([]byte("key4"))) // Delete key4 (newer)

	// Sort entries by key, then by timestamp (newest first for same key)
	sort.Slice(entries, func(i, j int) bool {
		cmp := entries[i].Compare(entries[j])
		if cmp == 0 {
			// Same key, prefer newer timestamp
			return entries[i].IsNewerThan(entries[j])
		}
		return cmp < 0
	})

	result := cm.removeDuplicatesAndTombstones(entries)

	// Should only have key1 (newest) and key2
	// key3 and key4 should be removed (tombstones)
	expectedKeys := []string{"key1", "key2"}

	if len(result) != len(expectedKeys) {
		t.Errorf("Expected %d entries, got %d", len(expectedKeys), len(result))
		// Print what we got for debugging
		for i, entry := range result {
			t.Logf("Result[%d]: key=%s, deleted=%v", i, entry.Key(), entry.IsDeleted())
		}
		return
	}

	for i, entry := range result {
		if i >= len(expectedKeys) {
			t.Errorf("More entries than expected: entry %d has key %s", i, entry.Key())
			continue
		}

		expectedKey := expectedKeys[i]
		if string(entry.Key()) != expectedKey {
			t.Errorf("Entry %d: expected key %s, got %s", i, expectedKey, entry.Key())
		}

		if entry.IsDeleted() {
			t.Errorf("Entry %d: should not be deleted", i)
		}
	}
}
