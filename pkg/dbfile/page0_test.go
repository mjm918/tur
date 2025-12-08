// pkg/dbfile/page0_test.go
package dbfile

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestPage0_HeaderAtOffset0(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	db.Close()

	// Read raw file and verify header at offset 0
	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	// Magic string should be at offset 0
	if string(data[0:16]) != MagicString {
		t.Errorf("Magic string at offset 0 = %q, want %q", string(data[0:16]), MagicString)
	}
}

func TestPage0_HeaderFillsFirst100Bytes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	db.Close()

	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	// Page size should be at offset 16-17
	pageSize := binary.LittleEndian.Uint16(data[16:18])
	if pageSize != DefaultPageSize {
		t.Errorf("Page size at offset 16 = %d, want %d", pageSize, DefaultPageSize)
	}

	// Page count should be at offset 28-31
	pageCount := binary.LittleEndian.Uint32(data[28:32])
	if pageCount < 1 {
		t.Errorf("Page count at offset 28 = %d, want at least 1", pageCount)
	}
}

func TestPage0_UpdatesOnSync(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Modify header
	db.Header().ChangeCounter = 42

	// Sync changes
	if err := db.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	db.Close()

	// Read back and verify
	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	changeCounter := binary.LittleEndian.Uint32(data[24:28])
	if changeCounter != 42 {
		t.Errorf("ChangeCounter = %d, want 42", changeCounter)
	}
}

func TestPage0_Configuration_PageSize(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	opts := &Options{PageSize: 8192}
	db, err := Create(dbPath, opts)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	db.Close()

	// Reopen and verify configuration persisted
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db2.Close()

	if db2.PageSize() != 8192 {
		t.Errorf("PageSize() = %d, want 8192", db2.PageSize())
	}
}

func TestPage0_FileSizeMatchesPageSize(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	opts := &Options{PageSize: 8192}
	db, err := Create(dbPath, opts)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	db.Close()

	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	// File should be at least one page (8192 bytes)
	if info.Size() < 8192 {
		t.Errorf("File size = %d, want at least 8192", info.Size())
	}

	// File size should be a multiple of page size
	if info.Size()%8192 != 0 {
		t.Errorf("File size = %d, not a multiple of page size 8192", info.Size())
	}
}

func TestPage0_HeaderReservedArea(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	db.Close()

	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}

	// Reserved area (bytes 72-91) should be zero-initialized
	for i := 72; i < 92; i++ {
		if data[i] != 0 {
			t.Errorf("Reserved byte at offset %d = %d, want 0", i, data[i])
		}
	}
}

func TestPage0_TextEncoding(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	// Default text encoding should be UTF-8 (1)
	if db.Header().TextEncoding != 1 {
		t.Errorf("TextEncoding = %d, want 1 (UTF-8)", db.Header().TextEncoding)
	}
}

func TestPage0_WriteAndReadMultiple(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create and write
	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	db.Header().UserVersion = 100
	db.Header().ApplicationID = 0x54555244 // "TURD" in little-endian

	db.Sync()
	db.Close()

	// Open and verify
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db2.Close()

	if db2.Header().UserVersion != 100 {
		t.Errorf("UserVersion = %d, want 100", db2.Header().UserVersion)
	}

	if db2.Header().ApplicationID != 0x54555244 {
		t.Errorf("ApplicationID = %#x, want %#x", db2.Header().ApplicationID, 0x54555244)
	}
}
