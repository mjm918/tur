// pkg/dbfile/database_test.go
package dbfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDatabase_Create(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	// File should exist
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Create() did not create database file")
	}

	// Check header was written correctly
	header := db.Header()
	if header == nil {
		t.Fatal("Header() returned nil")
	}
	if header.PageSize != DefaultPageSize {
		t.Errorf("PageSize = %d, want %d", header.PageSize, DefaultPageSize)
	}
	if header.PageCount < 1 {
		t.Errorf("PageCount = %d, want at least 1", header.PageCount)
	}
}

func TestDatabase_Create_WithOptions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	opts := &Options{
		PageSize: 8192,
	}

	db, err := Create(dbPath, opts)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	header := db.Header()
	if header.PageSize != 8192 {
		t.Errorf("PageSize = %d, want 8192", header.PageSize)
	}
}

func TestDatabase_Create_AlreadyExists(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create first database
	db1, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("First Create() error = %v", err)
	}
	db1.Close()

	// Try to create again - should fail
	_, err = Create(dbPath, nil)
	if err == nil {
		t.Error("Create() should fail for existing file")
	}
	if err != ErrDatabaseExists {
		t.Errorf("Create() error = %v, want ErrDatabaseExists", err)
	}
}

func TestDatabase_Open(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create database first
	db1, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	db1.Close()

	// Open existing database
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db2.Close()

	header := db2.Header()
	if header.PageSize != DefaultPageSize {
		t.Errorf("PageSize = %d, want %d", header.PageSize, DefaultPageSize)
	}
}

func TestDatabase_Open_NotExists(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "nonexistent.db")

	_, err := Open(dbPath, nil)
	if err == nil {
		t.Error("Open() should fail for non-existent file")
	}
	if err != ErrDatabaseNotFound {
		t.Errorf("Open() error = %v, want ErrDatabaseNotFound", err)
	}
}

func TestDatabase_Open_InvalidMagic(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create a file with invalid content (must be at least HeaderSize bytes)
	invalidData := make([]byte, HeaderSize)
	copy(invalidData, "InvalidMagicXXXX") // Wrong magic string
	if err := os.WriteFile(dbPath, invalidData, 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Open(dbPath, nil)
	if err == nil {
		t.Error("Open() should fail for invalid file")
	}
	if err != ErrInvalidMagic {
		t.Errorf("Open() error = %v, want ErrInvalidMagic", err)
	}
}

func TestDatabase_Close(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Double close should not error
	if err := db.Close(); err != nil {
		t.Errorf("Second Close() error = %v", err)
	}
}

func TestDatabase_PageSize(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	if ps := db.PageSize(); ps != DefaultPageSize {
		t.Errorf("PageSize() = %d, want %d", ps, DefaultPageSize)
	}
}

func TestDatabase_PageCount(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	// Initially should have at least 1 page (header + schema)
	if pc := db.PageCount(); pc < 1 {
		t.Errorf("PageCount() = %d, want at least 1", pc)
	}
}

func TestDatabase_Path(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	if p := db.Path(); p != dbPath {
		t.Errorf("Path() = %q, want %q", p, dbPath)
	}
}

func TestDatabase_FileSize(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	db.Close()

	// Check file size is at least one page
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() < int64(DefaultPageSize) {
		t.Errorf("File size = %d, want at least %d", info.Size(), DefaultPageSize)
	}
}
