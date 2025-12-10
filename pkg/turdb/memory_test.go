// pkg/turdb/memory_test.go
package turdb

import (
	"os"
	"path/filepath"
	"testing"
)

// TestOpenMemoryDatabase tests opening an in-memory database with :memory: path
func TestOpenMemoryDatabase(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	defer db.Close()

	// Verify no files were created
	// :memory: should not create any files on disk
	if _, err := os.Stat(":memory:"); !os.IsNotExist(err) {
		t.Error("In-memory database should not create files on disk")
	}
	if _, err := os.Stat(":memory:.lock"); !os.IsNotExist(err) {
		t.Error("In-memory database should not create lock files")
	}
	if _, err := os.Stat(":memory:-wal"); !os.IsNotExist(err) {
		t.Error("In-memory database should not create WAL files")
	}

	// Verify database is functional
	result, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result from CREATE TABLE")
	}

	// Insert data
	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query data
	result, err = db.Exec("SELECT id, name FROM test WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "Alice" {
		t.Errorf("Expected 'Alice', got %v", result.Rows[0][1])
	}
}

// TestMemoryDatabaseNoFilesCreated verifies no files are created in current directory
func TestMemoryDatabaseNoFilesCreated(t *testing.T) {
	// Get files before opening database
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	filesBefore, err := filepath.Glob(filepath.Join(dir, ":memory:*"))
	if err != nil {
		t.Fatalf("Failed to list files: %v", err)
	}

	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}

	// Create a table to ensure database is actually used
	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	db.Close()

	// Check no new files were created
	filesAfter, err := filepath.Glob(filepath.Join(dir, ":memory:*"))
	if err != nil {
		t.Fatalf("Failed to list files after: %v", err)
	}

	if len(filesAfter) > len(filesBefore) {
		t.Errorf("In-memory database created files on disk: before=%d, after=%d", len(filesBefore), len(filesAfter))
		// Clean up any files that were created
		for _, f := range filesAfter {
			os.Remove(f)
		}
	}
}

// TestMemoryDatabaseDataLostOnClose verifies data is lost when database is closed
func TestMemoryDatabaseDataLostOnClose(t *testing.T) {
	// First database - create and populate
	db1, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}

	_, err = db1.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db1.Exec("INSERT INTO test VALUES (1, 'data1')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	db1.Close()

	// Second database - should be fresh, no data from first
	db2, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open second in-memory database: %v", err)
	}
	defer db2.Close()

	// The table should not exist (fresh database)
	_, err = db2.Exec("SELECT * FROM test")
	if err == nil {
		t.Error("Expected error querying non-existent table, but query succeeded")
	}
}

// TestMemoryDatabaseMultipleInstances verifies each :memory: database is isolated
func TestMemoryDatabaseMultipleInstances(t *testing.T) {
	// Open two in-memory databases simultaneously
	db1, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open first in-memory database: %v", err)
	}
	defer db1.Close()

	db2, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open second in-memory database: %v", err)
	}
	defer db2.Close()

	// Create table in first database
	_, err = db1.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table in db1: %v", err)
	}

	_, err = db1.Exec("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert in db1: %v", err)
	}

	// Second database should not have this table
	_, err = db2.Exec("SELECT * FROM users")
	if err == nil {
		t.Error("Second database should not have users table from first database")
	}

	// Create different table in second database
	_, err = db2.Exec("CREATE TABLE products (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table in db2: %v", err)
	}

	// First database should not have products table
	_, err = db1.Exec("SELECT * FROM products")
	if err == nil {
		t.Error("First database should not have products table from second database")
	}
}
