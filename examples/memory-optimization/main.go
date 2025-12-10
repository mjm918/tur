package main

import (
	"fmt"
	"os"
	"runtime"

	"tur/pkg/turdb"
)

func printMemStats(label string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("\n=== %s ===\n", label)
	fmt.Printf("Alloc = %v MB\n", m.Alloc/1024/1024)
	fmt.Printf("TotalAlloc = %v MB\n", m.TotalAlloc/1024/1024)
	fmt.Printf("Sys = %v MB\n", m.Sys/1024/1024)
	fmt.Printf("NumGC = %v\n\n", m.NumGC)
}

func main() {
	// Create a temporary database file
	dbPath := "example.db"
	defer os.Remove(dbPath)

	// Open database with default settings
	db, err := turdb.Open(dbPath)
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		return
	}
	defer db.Close()

	printMemStats("After opening database (default settings)")

	// Create a test table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, data TEXT)")
	if err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
		return
	}

	// Insert some data
	for i := 0; i < 100; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d, 'data_%d')", i, i))
		if err != nil {
			fmt.Printf("Failed to insert row %d: %v\n", i, err)
			return
		}
	}

	printMemStats("After inserting 100 rows")

	// Query with default settings
	result, err := db.Exec("SELECT * FROM test")
	if err != nil {
		fmt.Printf("Failed to query: %v\n", err)
		return
	}

	fmt.Printf("Retrieved %d rows\n", len(result.Rows))
	printMemStats("After querying (default settings)")

	// Now optimize for minimal memory
	fmt.Println("\n>>> Applying memory optimization <<<")
	_, err = db.Exec("PRAGMA optimize_memory")
	if err != nil {
		fmt.Printf("Failed to apply memory optimization: %v\n", err)
		return
	}

	runtime.GC()
	printMemStats("After PRAGMA optimize_memory")

	// Query again with optimized settings
	result, err = db.Exec("SELECT * FROM test")
	if err != nil {
		fmt.Printf("Failed to query: %v\n", err)
		return
	}

	fmt.Printf("Retrieved %d rows\n", len(result.Rows))
	printMemStats("After querying (optimized settings)")

	// Show current settings
	fmt.Println("\n=== Current Configuration ===")
	pragmas := []string{
		"page_cache_size",
		"query_cache_size",
		"vdbe_max_registers",
		"vdbe_max_cursors",
		"memory_budget",
		"result_streaming",
	}

	for _, pragma := range pragmas {
		result, err := db.Exec(fmt.Sprintf("PRAGMA %s", pragma))
		if err != nil {
			fmt.Printf("Error querying %s: %v\n", pragma, err)
			continue
		}

		if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
			fmt.Printf("%s = %v\n", pragma, result.Rows[0][0])
		}
	}
}
