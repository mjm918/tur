// pkg/pager/storage_test.go
package pager

import (
	"testing"
)

// TestStorageInterface verifies that MmapFile implements the Storage interface
func TestStorageInterface(t *testing.T) {
	// This test ensures that MmapFile satisfies the Storage interface
	var _ Storage = (*MmapFile)(nil)
}

// TestMemoryStorageInterface verifies that MemoryStorage implements the Storage interface
func TestMemoryStorageInterface(t *testing.T) {
	// This test ensures that MemoryStorage satisfies the Storage interface
	var _ Storage = (*MemoryStorage)(nil)
}

// TestMemoryStorageBasicOperations tests basic read/write operations
func TestMemoryStorageBasicOperations(t *testing.T) {
	pageSize := 4096
	storage, err := NewMemoryStorage(int64(pageSize))
	if err != nil {
		t.Fatalf("Failed to create MemoryStorage: %v", err)
	}
	defer storage.Close()

	// Test initial size
	if storage.Size() != int64(pageSize) {
		t.Errorf("Expected initial size %d, got %d", pageSize, storage.Size())
	}

	// Test writing and reading data
	testData := []byte("Hello, TurDB!")
	slice := storage.Slice(0, len(testData))
	if slice == nil {
		t.Fatal("Failed to get slice from MemoryStorage")
	}
	copy(slice, testData)

	// Read back the data
	readSlice := storage.Slice(0, len(testData))
	if string(readSlice) != string(testData) {
		t.Errorf("Expected %q, got %q", testData, readSlice)
	}
}

// TestMemoryStorageGrow tests growing the storage
func TestMemoryStorageGrow(t *testing.T) {
	pageSize := 4096
	storage, err := NewMemoryStorage(int64(pageSize))
	if err != nil {
		t.Fatalf("Failed to create MemoryStorage: %v", err)
	}
	defer storage.Close()

	// Write data at the beginning
	testData := []byte("Initial data")
	slice := storage.Slice(0, len(testData))
	copy(slice, testData)

	// Grow the storage
	newSize := int64(pageSize * 2)
	if err := storage.Grow(newSize); err != nil {
		t.Fatalf("Failed to grow storage: %v", err)
	}

	// Verify new size
	if storage.Size() != newSize {
		t.Errorf("Expected size %d after grow, got %d", newSize, storage.Size())
	}

	// Verify original data is preserved
	readSlice := storage.Slice(0, len(testData))
	if string(readSlice) != string(testData) {
		t.Errorf("Data not preserved after grow: expected %q, got %q", testData, readSlice)
	}

	// Write data at the new end
	offset := pageSize
	endData := []byte("End data")
	endSlice := storage.Slice(offset, len(endData))
	if endSlice == nil {
		t.Fatal("Failed to get slice at new offset after grow")
	}
	copy(endSlice, endData)

	// Verify end data
	readEndSlice := storage.Slice(offset, len(endData))
	if string(readEndSlice) != string(endData) {
		t.Errorf("End data not written correctly: expected %q, got %q", endData, readEndSlice)
	}
}

// TestMemoryStorageSync tests that Sync is a no-op but doesn't error
func TestMemoryStorageSync(t *testing.T) {
	storage, err := NewMemoryStorage(4096)
	if err != nil {
		t.Fatalf("Failed to create MemoryStorage: %v", err)
	}
	defer storage.Close()

	// Sync should not return an error for in-memory storage
	if err := storage.Sync(); err != nil {
		t.Errorf("Sync should not return error for MemoryStorage: %v", err)
	}
}

// TestMemoryStorageSliceBounds tests boundary conditions for Slice
func TestMemoryStorageSliceBounds(t *testing.T) {
	pageSize := 4096
	storage, err := NewMemoryStorage(int64(pageSize))
	if err != nil {
		t.Fatalf("Failed to create MemoryStorage: %v", err)
	}
	defer storage.Close()

	// Valid slice at the end
	slice := storage.Slice(pageSize-10, 10)
	if slice == nil {
		t.Error("Expected valid slice at end of storage")
	}

	// Invalid slice past the end
	invalidSlice := storage.Slice(pageSize, 1)
	if invalidSlice != nil {
		t.Error("Expected nil slice when requesting past storage bounds")
	}

	// Invalid slice that extends past end
	invalidSlice2 := storage.Slice(pageSize-5, 10)
	if invalidSlice2 != nil {
		t.Error("Expected nil slice when request extends past storage bounds")
	}
}
