// pkg/pager/freelist_test.go
package pager

import (
	"testing"
)

func TestFreelistTrunkPage_Encode(t *testing.T) {
	// Test that a trunk page correctly encodes its next pointer and leaf pages
	trunk := &FreelistTrunkPage{
		NextTrunk: 5,
		LeafPages: []uint32{10, 11, 12},
	}

	data := make([]byte, 4096)
	trunk.Encode(data)

	// Verify next trunk pointer at offset 0
	nextTrunk := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	if nextTrunk != 5 {
		t.Errorf("NextTrunk: expected 5, got %d", nextTrunk)
	}

	// Verify leaf count at offset 4
	leafCount := uint32(data[4])<<24 | uint32(data[5])<<16 | uint32(data[6])<<8 | uint32(data[7])
	if leafCount != 3 {
		t.Errorf("LeafCount: expected 3, got %d", leafCount)
	}

	// Verify leaf page numbers starting at offset 8
	for i, expected := range []uint32{10, 11, 12} {
		offset := 8 + i*4
		leaf := uint32(data[offset])<<24 | uint32(data[offset+1])<<16 | uint32(data[offset+2])<<8 | uint32(data[offset+3])
		if leaf != expected {
			t.Errorf("Leaf[%d]: expected %d, got %d", i, expected, leaf)
		}
	}
}

func TestFreelistTrunkPage_Decode(t *testing.T) {
	// Build raw page data
	data := make([]byte, 4096)

	// Next trunk = 7
	data[0], data[1], data[2], data[3] = 0, 0, 0, 7

	// Leaf count = 2
	data[4], data[5], data[6], data[7] = 0, 0, 0, 2

	// Leaf pages: 20, 21
	data[8], data[9], data[10], data[11] = 0, 0, 0, 20
	data[12], data[13], data[14], data[15] = 0, 0, 0, 21

	trunk := DecodeFreelistTrunkPage(data)

	if trunk.NextTrunk != 7 {
		t.Errorf("NextTrunk: expected 7, got %d", trunk.NextTrunk)
	}

	if len(trunk.LeafPages) != 2 {
		t.Fatalf("LeafPages count: expected 2, got %d", len(trunk.LeafPages))
	}

	if trunk.LeafPages[0] != 20 || trunk.LeafPages[1] != 21 {
		t.Errorf("LeafPages: expected [20, 21], got %v", trunk.LeafPages)
	}
}

func TestFreelistTrunkPage_MaxLeaves(t *testing.T) {
	// A trunk page can hold (pageSize - 8) / 4 leaves
	// For 4096 byte pages: (4096 - 8) / 4 = 1022 leaves
	pageSize := 4096
	expected := (pageSize - 8) / 4

	actual := MaxLeavesPerTrunk(pageSize)
	if actual != expected {
		t.Errorf("MaxLeavesPerTrunk: expected %d, got %d", expected, actual)
	}
}

func TestFreelistTrunkPage_IsFull(t *testing.T) {
	trunk := &FreelistTrunkPage{
		NextTrunk: 0,
		LeafPages: make([]uint32, 0),
	}

	pageSize := 4096
	maxLeaves := MaxLeavesPerTrunk(pageSize)

	if trunk.IsFull(pageSize) {
		t.Error("Empty trunk should not be full")
	}

	// Fill to max
	trunk.LeafPages = make([]uint32, maxLeaves)
	if !trunk.IsFull(pageSize) {
		t.Error("Full trunk should be full")
	}
}

func TestFreelistTrunkPage_AddLeaf(t *testing.T) {
	trunk := &FreelistTrunkPage{
		NextTrunk: 0,
		LeafPages: []uint32{10, 11},
	}

	trunk.AddLeaf(12)

	if len(trunk.LeafPages) != 3 {
		t.Fatalf("Expected 3 leaves, got %d", len(trunk.LeafPages))
	}

	if trunk.LeafPages[2] != 12 {
		t.Errorf("Expected leaf page 12, got %d", trunk.LeafPages[2])
	}
}

func TestFreelistTrunkPage_PopLeaf(t *testing.T) {
	trunk := &FreelistTrunkPage{
		NextTrunk: 0,
		LeafPages: []uint32{10, 11, 12},
	}

	leaf, ok := trunk.PopLeaf()
	if !ok {
		t.Fatal("PopLeaf should succeed on non-empty trunk")
	}

	if leaf != 12 {
		t.Errorf("Expected popped leaf 12, got %d", leaf)
	}

	if len(trunk.LeafPages) != 2 {
		t.Errorf("Expected 2 leaves after pop, got %d", len(trunk.LeafPages))
	}

	// Pop remaining
	trunk.PopLeaf()
	trunk.PopLeaf()

	_, ok = trunk.PopLeaf()
	if ok {
		t.Error("PopLeaf should fail on empty trunk")
	}
}

// Freelist manager tests

func TestFreelist_New(t *testing.T) {
	fl := NewFreelist(4096)

	if fl.HeadPage() != 0 {
		t.Errorf("New freelist should have head page 0, got %d", fl.HeadPage())
	}

	if fl.FreeCount() != 0 {
		t.Errorf("New freelist should have 0 free pages, got %d", fl.FreeCount())
	}
}

func TestFreelist_AllocateFromEmpty(t *testing.T) {
	fl := NewFreelist(4096)

	// Allocating from empty freelist should return 0 (not found)
	pageNo, ok := fl.Allocate()
	if ok {
		t.Errorf("Allocate from empty freelist should return ok=false, got page %d", pageNo)
	}
}

func TestFreelist_FreeAndAllocate(t *testing.T) {
	fl := NewFreelist(4096)

	// Free page 5
	fl.Free(5)

	if fl.FreeCount() != 1 {
		t.Errorf("Expected 1 free page, got %d", fl.FreeCount())
	}

	// Allocate should return page 5
	pageNo, ok := fl.Allocate()
	if !ok {
		t.Fatal("Allocate should succeed when freelist is not empty")
	}

	if pageNo != 5 {
		t.Errorf("Expected page 5, got %d", pageNo)
	}

	if fl.FreeCount() != 0 {
		t.Errorf("Expected 0 free pages after allocate, got %d", fl.FreeCount())
	}
}

func TestFreelist_LIFO(t *testing.T) {
	fl := NewFreelist(4096)

	// Free pages in order: 10, 20, 30
	fl.Free(10)
	fl.Free(20)
	fl.Free(30)

	if fl.FreeCount() != 3 {
		t.Fatalf("Expected 3 free pages, got %d", fl.FreeCount())
	}

	// Allocate should return in LIFO order: 30, 20, 10
	p1, _ := fl.Allocate()
	p2, _ := fl.Allocate()
	p3, _ := fl.Allocate()

	if p1 != 30 {
		t.Errorf("First allocate: expected 30, got %d", p1)
	}
	if p2 != 20 {
		t.Errorf("Second allocate: expected 20, got %d", p2)
	}
	if p3 != 10 {
		t.Errorf("Third allocate: expected 10, got %d", p3)
	}
}

func TestFreelist_MultipleTrunks(t *testing.T) {
	// Use small page size to force multiple trunks
	pageSize := 32 // Can hold (32-8)/4 = 6 leaves per trunk

	fl := NewFreelist(pageSize)

	// Free more pages than one trunk can hold
	for i := uint32(10); i <= 20; i++ {
		fl.Free(i)
	}

	// Should have 11 free pages
	if fl.FreeCount() != 11 {
		t.Errorf("Expected 11 free pages, got %d", fl.FreeCount())
	}

	// Allocate all and verify we get them back
	allocated := make([]uint32, 0, 11)
	for i := 0; i < 11; i++ {
		p, ok := fl.Allocate()
		if !ok {
			t.Fatalf("Allocate failed at iteration %d", i)
		}
		allocated = append(allocated, p)
	}

	// Verify all pages were in range 10-20
	seen := make(map[uint32]bool)
	for _, p := range allocated {
		if p < 10 || p > 20 {
			t.Errorf("Unexpected page %d returned", p)
		}
		if seen[p] {
			t.Errorf("Duplicate page %d returned", p)
		}
		seen[p] = true
	}
}
