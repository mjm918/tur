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
