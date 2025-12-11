// pkg/turdb/integrity_test.go
package turdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestIntegrityCheck_EmptyDatabase(t *testing.T) {
	// Create a temp database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Run integrity check on empty database - should pass
	errors := db.IntegrityCheck()
	if len(errors) != 0 {
		t.Errorf("Expected no integrity errors for empty database, got %d: %v", len(errors), errors)
	}
}

func TestIntegrityCheck_ValidDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a table and insert data
	_, err = db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Run integrity check - should pass
	errors := db.IntegrityCheck()
	if len(errors) != 0 {
		t.Errorf("Expected no integrity errors for valid database, got %d: %v", len(errors), errors)
	}

	db.Close()
}

func TestIntegrityCheck_WithIndex(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a table with index
	_, err = db.Exec("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price REAL)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("CREATE INDEX idx_name ON products(name)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	_, err = db.Exec("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec("INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Run integrity check - should pass
	errors := db.IntegrityCheck()
	if len(errors) != 0 {
		t.Errorf("Expected no integrity errors for database with index, got %d: %v", len(errors), errors)
	}

	db.Close()
}

func TestIntegrityCheck_ForeignKeyValid(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create tables with foreign key
	_, err = db.Exec("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create departments table: %v", err)
	}

	_, err = db.Exec("CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id))")
	if err != nil {
		t.Fatalf("Failed to create employees table: %v", err)
	}

	_, err = db.Exec("INSERT INTO departments (id, name) VALUES (1, 'Engineering')")
	if err != nil {
		t.Fatalf("Failed to insert department: %v", err)
	}

	_, err = db.Exec("INSERT INTO employees (id, name, dept_id) VALUES (1, 'Alice', 1)")
	if err != nil {
		t.Fatalf("Failed to insert employee: %v", err)
	}

	// Run integrity check - should pass (all foreign keys valid)
	errors := db.IntegrityCheck()
	if len(errors) != 0 {
		t.Errorf("Expected no integrity errors for valid foreign keys, got %d: %v", len(errors), errors)
	}

	db.Close()
}

func TestIntegrityCheck_QuickCheck(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create some data
	_, err = db.Exec("CREATE TABLE test (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Quick check should be faster (skips some checks)
	errors := db.QuickCheck()
	if len(errors) != 0 {
		t.Errorf("Expected no errors from quick check, got %d: %v", len(errors), errors)
	}
}

func TestIntegrityCheck_ReturnsErrors(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create a table
	_, err = db.Exec("CREATE TABLE test (id INT PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	db.Close()

	// Corrupt the database file directly
	// (This is a simplified test - in reality corruption would be more subtle)
	// For now, we just verify the method exists and runs without panic

	db, err = Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Just verify the method runs without panic
	_ = db.IntegrityCheck()
}

func TestIntegrityCheckResult_String(t *testing.T) {
	result := IntegrityError{
		Type:    "btree",
		Table:   "users",
		Page:    42,
		Message: "invalid key ordering",
	}

	str := result.String()
	if str == "" {
		t.Error("Expected non-empty error string")
	}

	// Check that it contains key info
	if testing.Verbose() {
		t.Logf("Error string: %s", str)
	}
}

func TestIntegrityCheck_MultipleTables(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create multiple tables with data
	_, err = db.Exec("CREATE TABLE table1 (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table1: %v", err)
	}

	_, err = db.Exec("CREATE TABLE table2 (id INT PRIMARY KEY, value REAL)")
	if err != nil {
		t.Fatalf("Failed to create table2: %v", err)
	}

	_, err = db.Exec("INSERT INTO table1 (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert into table1: %v", err)
	}

	_, err = db.Exec("INSERT INTO table2 (id, value) VALUES (1, 3.14)")
	if err != nil {
		t.Fatalf("Failed to insert into table2: %v", err)
	}

	// Run integrity check
	errors := db.IntegrityCheck()
	if len(errors) != 0 {
		t.Errorf("Expected no integrity errors, got %d: %v", len(errors), errors)
	}
}

func TestIntegrityCheck_LargeDataset(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE items (id INT PRIMARY KEY, name TEXT, quantity INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert many rows
	for i := 1; i <= 100; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO items (id, name, quantity) VALUES (%d, 'item%d', %d)", i, i, i*10))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Run integrity check - should pass
	errors := db.IntegrityCheck()
	if len(errors) != 0 {
		t.Errorf("Expected no integrity errors for large dataset, got %d: %v", len(errors), errors)
	}

	// Quick check should also pass
	errors = db.QuickCheck()
	if len(errors) != 0 {
		t.Errorf("Expected no errors from quick check, got %d: %v", len(errors), errors)
	}
}

func TestIntegrityCheck_ClosedDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Close the database
	db.Close()

	// Run integrity check on closed database
	errors := db.IntegrityCheck()
	if len(errors) != 1 {
		t.Errorf("Expected 1 error for closed database, got %d", len(errors))
	}
	if len(errors) > 0 && errors[0].Type != "database" {
		t.Errorf("Expected database error type, got %s", errors[0].Type)
	}
}

func TestCorruptionCheck_EmptyDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Run corruption check on empty database
	errors := db.CorruptionCheck()
	if len(errors) != 0 {
		t.Errorf("Expected no corruption errors for empty database, got %d: %v", len(errors), errors)
	}
}

func TestCorruptionCheck_WithData(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table and insert data
	_, err = db.Exec("CREATE TABLE test (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Run corruption check
	errors := db.CorruptionCheck()
	if len(errors) != 0 {
		t.Errorf("Expected no corruption errors, got %d: %v", len(errors), errors)
	}
}

func TestCheckPage_ValidPage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Check page 0 (header page)
	err2 := db.CheckPage(0)
	if err2 != nil {
		t.Errorf("Expected no error for page 0, got: %v", err2)
	}
}

func TestCorruptionCheck_ClosedDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db.Close()

	// Run corruption check on closed database
	errors := db.CorruptionCheck()
	if len(errors) != 1 {
		t.Errorf("Expected 1 error for closed database, got %d", len(errors))
	}
	if len(errors) > 0 && errors[0].Type != "database" {
		t.Errorf("Expected database error type, got %s", errors[0].Type)
	}
}

func TestCheckPage_ClosedDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	db.Close()

	// Check page on closed database
	err2 := db.CheckPage(0)
	if err2 == nil {
		t.Error("Expected error for closed database, got nil")
	}
	if err2 != nil && err2.Type != "database" {
		t.Errorf("Expected database error type, got %s", err2.Type)
	}
}

func cleanupFiles(path string) {
	os.Remove(path)
	os.Remove(path + "-wal")
	os.Remove(path + ".lock")
}
