// pkg/pager/freelist.go
package pager

import "encoding/binary"

// FreelistTrunkPage represents a trunk page in the freelist.
// The freelist uses a linked list of trunk pages, where each trunk page
// contains pointers to leaf pages (free pages that can be allocated).
//
// Trunk Page Format:
//   Offset 0: 4-byte page number of next trunk (0 if last trunk)
//   Offset 4: 4-byte count of leaf pages in this trunk
//   Offset 8: Array of 4-byte leaf page numbers
//
// This design follows SQLite's freelist structure.
type FreelistTrunkPage struct {
	// NextTrunk is the page number of the next trunk page, or 0 if this is the last
	NextTrunk uint32

	// LeafPages contains the page numbers of free pages
	LeafPages []uint32
}

// MaxLeavesPerTrunk returns the maximum number of leaf pages that can fit
// in a trunk page of the given size.
// Formula: (pageSize - 8) / 4, where 8 is the header size (next + count)
func MaxLeavesPerTrunk(pageSize int) int {
	return (pageSize - 8) / 4
}

// Encode writes the trunk page to the given byte slice in big-endian format.
func (t *FreelistTrunkPage) Encode(data []byte) {
	// Write next trunk pointer at offset 0
	binary.BigEndian.PutUint32(data[0:4], t.NextTrunk)

	// Write leaf count at offset 4
	binary.BigEndian.PutUint32(data[4:8], uint32(len(t.LeafPages)))

	// Write leaf page numbers starting at offset 8
	for i, leaf := range t.LeafPages {
		offset := 8 + i*4
		binary.BigEndian.PutUint32(data[offset:offset+4], leaf)
	}
}

// DecodeFreelistTrunkPage decodes a trunk page from raw bytes.
func DecodeFreelistTrunkPage(data []byte) *FreelistTrunkPage {
	nextTrunk := binary.BigEndian.Uint32(data[0:4])
	leafCount := binary.BigEndian.Uint32(data[4:8])

	leaves := make([]uint32, leafCount)
	for i := uint32(0); i < leafCount; i++ {
		offset := 8 + i*4
		leaves[i] = binary.BigEndian.Uint32(data[offset : offset+4])
	}

	return &FreelistTrunkPage{
		NextTrunk: nextTrunk,
		LeafPages: leaves,
	}
}

// IsFull returns true if this trunk page cannot hold any more leaf pages.
func (t *FreelistTrunkPage) IsFull(pageSize int) bool {
	return len(t.LeafPages) >= MaxLeavesPerTrunk(pageSize)
}

// AddLeaf adds a leaf page to this trunk.
func (t *FreelistTrunkPage) AddLeaf(pageNo uint32) {
	t.LeafPages = append(t.LeafPages, pageNo)
}

// PopLeaf removes and returns the last leaf page from this trunk.
// Returns false if the trunk has no leaves.
func (t *FreelistTrunkPage) PopLeaf() (uint32, bool) {
	if len(t.LeafPages) == 0 {
		return 0, false
	}

	last := t.LeafPages[len(t.LeafPages)-1]
	t.LeafPages = t.LeafPages[:len(t.LeafPages)-1]
	return last, true
}

// IsEmpty returns true if this trunk has no leaf pages.
func (t *FreelistTrunkPage) IsEmpty() bool {
	return len(t.LeafPages) == 0
}

// LeafCount returns the number of leaf pages in this trunk.
func (t *FreelistTrunkPage) LeafCount() int {
	return len(t.LeafPages)
}
