// pkg/api/db_test.go
package api

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDB_HasRequiredFields(t *testing.T) {
	// Test that the DB struct has the required fields
	// This test verifies the design by checking that DB struct exists
	// and can be instantiated with expected behavior

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Verify DB has a valid path
	if db.Path() != dbPath {
		t.Errorf("expected path %q, got %q", dbPath, db.Path())
	}
}

func TestDB_Open_CreatesNewFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "new.db")

	// Verify file doesn't exist yet
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatal("test file should not exist before Open")
	}

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Verify file was created
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("database file should exist after Open")
	}
}

func TestDB_Open_ExistingFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "existing.db")

	// Create and close a database first
	db1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("first Open failed: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}

	// Re-open the existing database
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("second Open failed: %v", err)
	}
	defer db2.Close()

	// Should successfully open
	if db2.Path() != dbPath {
		t.Errorf("expected path %q, got %q", dbPath, db2.Path())
	}
}

func TestDB_Close(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "close.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Close should succeed
	if err := db.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestDB_Close_MultipleCallsError(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "multi_close.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// First close should succeed
	if err := db.Close(); err != nil {
		t.Errorf("first Close failed: %v", err)
	}

	// Second close should return error (already closed)
	if err := db.Close(); err == nil {
		t.Error("expected error on second Close, got nil")
	}
}
