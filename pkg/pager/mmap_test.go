// pkg/pager/mmap_test.go
package pager

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMmapFileCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	mf, err := OpenMmapFile(path, 4096)
	if err != nil {
		t.Fatalf("failed to create mmap file: %v", err)
	}
	defer mf.Close()

	if mf.Size() != 4096 {
		t.Errorf("expected size 4096, got %d", mf.Size())
	}
}

func TestMmapFileReadWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	mf, err := OpenMmapFile(path, 4096)
	if err != nil {
		t.Fatalf("failed to create mmap file: %v", err)
	}

	// Write data
	data := mf.Slice(100, 11)
	copy(data, []byte("hello world"))

	// Sync and close
	if err := mf.Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	mf.Close()

	// Reopen and verify
	mf2, err := OpenMmapFile(path, 0) // 0 = use existing size
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer mf2.Close()

	got := mf2.Slice(100, 11)
	if string(got) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", string(got))
	}
}

func TestMmapFileGrow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	mf, err := OpenMmapFile(path, 4096)
	if err != nil {
		t.Fatalf("failed to create mmap file: %v", err)
	}
	defer mf.Close()

	// Write to first page
	copy(mf.Slice(0, 5), []byte("page1"))

	// Grow the file
	if err := mf.Grow(8192); err != nil {
		t.Fatalf("grow failed: %v", err)
	}

	if mf.Size() != 8192 {
		t.Errorf("expected size 8192 after grow, got %d", mf.Size())
	}

	// Original data should still be there
	if string(mf.Slice(0, 5)) != "page1" {
		t.Error("data lost after grow")
	}
}

func TestMmapFileExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create file with regular IO first
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("existing data"))
	f.Close()

	// Open with mmap
	mf, err := OpenMmapFile(path, 0)
	if err != nil {
		t.Fatalf("failed to open existing file: %v", err)
	}
	defer mf.Close()

	if string(mf.Slice(0, 13)) != "existing data" {
		t.Error("existing data not preserved")
	}
}
