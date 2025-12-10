// pkg/turdb/db_test.go
package turdb

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"tur/pkg/types"
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

func TestDB_ConcurrentConnections_SameFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "concurrent.db")

	// Open first connection
	db1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("first Open failed: %v", err)
	}
	defer db1.Close()

	// Opening a second connection to the same file should fail
	// because the file is locked by the first connection
	db2, err := Open(dbPath)
	if err == nil {
		db2.Close()
		t.Error("expected error opening second connection to same file, got nil")
	}
}

func TestDB_ConcurrentConnections_DifferentFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Open connections to different files should work
	db1, err := Open(filepath.Join(tmpDir, "db1.db"))
	if err != nil {
		t.Fatalf("first Open failed: %v", err)
	}
	defer db1.Close()

	db2, err := Open(filepath.Join(tmpDir, "db2.db"))
	if err != nil {
		t.Fatalf("second Open failed: %v", err)
	}
	defer db2.Close()

	// Both connections should be valid
	if db1.IsClosed() || db2.IsClosed() {
		t.Error("connections should not be closed")
	}
}

func TestDB_ReopenAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "reopen.db")

	// Open and close first connection
	db1, err := Open(dbPath)
	if err != nil {
		t.Fatalf("first Open failed: %v", err)
	}
	if err := db1.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}

	// After closing, we should be able to open a new connection
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("second Open failed: %v", err)
	}
	defer db2.Close()

	if db2.IsClosed() {
		t.Error("reopened connection should not be closed")
	}
}

func TestValueToGo_DateTimeTypes(t *testing.T) {
	// Test that valueToGo correctly converts date/time types
	// These types should NOT return nil
	
	tests := []struct {
		name     string
		value    types.Value
		expected string // Type name we expect (not nil)
	}{
		{"Date", types.NewDate(2025, 12, 10), "time.Time"},
		{"Time", types.NewTime(14, 30, 45, 0), "string"},
		{"TimeTZ", types.NewTimeTZ(14, 30, 45, 0, 0), "string"},
		{"Timestamp", types.NewTimestamp(2025, 12, 10, 14, 30, 45, 0), "time.Time"},
		{"TimestampTZ", types.NewTimestampTZ(time.Date(2025, 12, 10, 14, 30, 45, 0, time.UTC)), "time.Time"},
		{"Interval", types.NewInterval(12, 3600*1000000), "string"},
	}
	
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := valueToGo(tc.value)
			if result == nil {
				t.Errorf("valueToGo(%s) returned nil, expected non-nil %s", tc.name, tc.expected)
			}
		})
	}
}
