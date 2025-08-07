package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/Bloom0716/mini-bigtable/internal/application"
)

func main() {
	fmt.Println("Mini LSM-Tree Table Demo")

	// Create temporary directory for demo
	tmpDir := filepath.Join(os.TempDir(), "mini_lsm_demo")
	defer os.RemoveAll(tmpDir)

	// Create LSM service
	service, err := application.NewLSMTableService(tmpDir, 5) // Reduced to 5 to see rotation in basic demo
	if err != nil {
		log.Fatalf("Failed to create LSM service: %v", err)
	}
	defer service.Close()

	// Demo operations
	fmt.Println("\n=== Basic Operations Demo ===")

	// Put some key-value pairs
	entries := map[string]string{
		"user:1":       "Alice",
		"user:2":       "Bob",
		"user:3":       "Charlie",
		"config:db":    "localhost:5432",
		"config:cache": "redis:6379",
	}

	fmt.Println("Putting entries...")
	for key, value := range entries {
		if err := service.Put([]byte(key), []byte(value)); err != nil {
			log.Printf("Failed to put %s: %v", key, err)
		} else {
			fmt.Printf("  PUT %s = %s\n", key, value)
		}
	}

	fmt.Println("\nGetting entries...")
	for key := range entries {
		if value, err := service.Get([]byte(key)); err != nil {
			log.Printf("Failed to get %s: %v", key, err)
		} else {
			fmt.Printf("  GET %s = %s\n", key, string(value))
		}
	}

	// Delete an entry
	fmt.Println("\nDeleting user:2...")
	if err := service.Delete([]byte("user:2")); err != nil {
		log.Printf("Failed to delete user:2: %v", err)
	}

	// Try to get the deleted entry
	fmt.Println("Trying to get deleted entry...")
	if _, err := service.Get([]byte("user:2")); err != nil {
		fmt.Printf("  GET user:2 = (not found - correctly deleted)\n")
	} else {
		fmt.Printf("  GET user:2 = (ERROR: should be deleted)\n")
	}

	// Show memtable stats
	activeSize, immutableCount := service.GetMemTableStats()
	sstableStats := service.GetSSTableStats()
	fmt.Printf("\nMemTable Stats: Active size=%d, Immutable count=%d\n",
		activeSize, immutableCount)
	fmt.Printf("SSTable Stats: %v\n", sstableStats)

	// Debug: Check SSTable directory
	sstableDir := filepath.Join(tmpDir, "sstables")
	if entries, err := os.ReadDir(sstableDir); err == nil {
		fmt.Printf("SSTable files: ")
		for _, entry := range entries {
			fmt.Printf("%s ", entry.Name())
		}
		fmt.Println()
	}

	// Demo memtable rotation by adding more entries
	fmt.Println("\n=== MemTable Rotation Demo ===")
	fmt.Println("Adding more entries to trigger rotation...")

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("batch:%d", i)
		value := fmt.Sprintf("batch_value_%d", i)
		if err := service.Put([]byte(key), []byte(value)); err != nil {
			log.Printf("Failed to put %s: %v", key, err)
		} else {
			fmt.Printf("  PUT %s = %s\n", key, value)
		}

		// Show stats after each put
		activeSize, immutableCount = service.GetMemTableStats()
		fmt.Printf("    Stats: Active size=%d, Immutable count=%d\n",
			activeSize, immutableCount)
	}

	fmt.Println("\n=== Recovery Demo ===")
	fmt.Println("Closing and reopening service to test recovery...")

	if err := service.Close(); err != nil {
		log.Printf("Failed to close service: %v", err)
	}

	// Create new service and recover
	service2, err := application.NewLSMTableService(tmpDir, 5) // Match the first service
	if err != nil {
		log.Fatalf("Failed to create second LSM service: %v", err)
	}
	defer service2.Close()

	if err := service2.Recovery(); err != nil {
		log.Printf("Failed to recover: %v", err)
	} else {
		fmt.Println("Recovery completed successfully!")
	}

	// Verify some data is still there
	fmt.Println("Verifying recovered data...")
	testKeys := []string{"user:1", "user:3", "config:db", "batch:0", "batch:4"}
	for _, key := range testKeys {
		if value, err := service2.Get([]byte(key)); err != nil {
			fmt.Printf("  GET %s = (not found)\n", key)
		} else {
			fmt.Printf("  GET %s = %s\n", key, string(value))
		}
	}

	fmt.Println("\nDemo completed successfully!")

	// Demo block index performance
	fmt.Println("\n=== Block Index Performance Demo ===")
	fmt.Println("Adding many entries to demonstrate block index effectiveness...")

	// Add many entries to create larger SSTables
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("perf_test_%04d", i)
		value := fmt.Sprintf("performance_data_%04d", i)
		if err := service2.Put([]byte(key), []byte(value)); err != nil {
			log.Printf("Failed to put %s: %v", key, err)
		}
	}

	// Test search performance for various keys
	fmt.Println("Testing search performance with block index...")
	searchKeys := []string{
		"perf_test_0010", // Early key
		"perf_test_0100", // Middle key
		"perf_test_0190", // Late key
	}

	for _, key := range searchKeys {
		if value, err := service2.Get([]byte(key)); err != nil {
			fmt.Printf("  SEARCH %s = (not found)\n", key)
		} else {
			fmt.Printf("  SEARCH %s = %s\n", key, string(value))
		}
	}

	// Show final stats
	finalActiveSize, finalImmutableCount := service2.GetMemTableStats()
	finalSSTableStats := service2.GetSSTableStats()
	fmt.Printf("\nFinal Stats:\n")
	fmt.Printf("  MemTable: Active=%d, Immutable=%d\n", finalActiveSize, finalImmutableCount)
	fmt.Printf("  SSTable: %v\n", finalSSTableStats)

	fmt.Println("\n✅ LSM-Tree with Block Index Demo completed successfully!")
	fmt.Println("🚀 Block index provides efficient key lookups in large SSTables!")
}
