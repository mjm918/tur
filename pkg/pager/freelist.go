// pkg/pager/freelist.go
package pager

import (
	"encoding/binary"
	"sort"
)

// Database header offsets for freelist fields
// Following the pattern from pager.go where:
//   Offset 0-16: Magic string
//   Offset 16-20: Page size
//   Offset 20-24: Page count
const (
	offsetFreelistHead  = 24 // First freelist trunk page number
	offsetFreePageCount = 28 // Total number of free pages
)

// GetFreelistHead reads the freelist head page number from a header.
func GetFreelistHead(header []byte) uint32 {
	return binary.LittleEndian.Uint32(header[offsetFreelistHead : offsetFreelistHead+4])
}

// PutFreelistHead writes the freelist head page number to a header.
func PutFreelistHead(header []byte, pageNo uint32) {
	binary.LittleEndian.PutUint32(header[offsetFreelistHead:offsetFreelistHead+4], pageNo)
}

// GetFreePageCount reads the free page count from a header.
func GetFreePageCount(header []byte) uint32 {
	return binary.LittleEndian.Uint32(header[offsetFreePageCount : offsetFreePageCount+4])
}

// PutFreePageCount writes the free page count to a header.
func PutFreePageCount(header []byte, count uint32) {
	binary.LittleEndian.PutUint32(header[offsetFreePageCount:offsetFreePageCount+4], count)
}

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

// Freelist manages the free page list in memory.
// This is a simplified in-memory implementation that will later be
// integrated with the pager for persistent storage.
//
// The freelist uses a LIFO (Last In, First Out) strategy for allocation
// to maximize locality of reference.
type Freelist struct {
	pageSize  int
	headPage  uint32   // Page number of first trunk (0 if empty)
	freeCount uint32   // Total number of free pages
	trunks    []*FreelistTrunkPage // In-memory trunk pages
}

// NewFreelist creates a new empty freelist.
func NewFreelist(pageSize int) *Freelist {
	return &Freelist{
		pageSize:  pageSize,
		headPage:  0,
		freeCount: 0,
		trunks:    nil,
	}
}

// HeadPage returns the page number of the first trunk page (0 if empty).
func (f *Freelist) HeadPage() uint32 {
	return f.headPage
}

// FreeCount returns the total number of free pages.
func (f *Freelist) FreeCount() uint32 {
	return f.freeCount
}

// Allocate removes and returns a free page from the freelist.
// Returns (0, false) if the freelist is empty.
func (f *Freelist) Allocate() (uint32, bool) {
	if f.freeCount == 0 || len(f.trunks) == 0 {
		return 0, false
	}

	// Get the first trunk (head)
	trunk := f.trunks[0]

	// Try to pop a leaf page
	if pageNo, ok := trunk.PopLeaf(); ok {
		f.freeCount--

		// If trunk is now empty and there are more trunks, remove this trunk
		if trunk.IsEmpty() && len(f.trunks) > 1 {
			f.trunks = f.trunks[1:]
			f.headPage = trunk.NextTrunk
		} else if trunk.IsEmpty() {
			// Last trunk is now empty
			f.trunks = nil
			f.headPage = 0
		}

		return pageNo, true
	}

	// Trunk was already empty - remove it and try the next trunk
	if len(f.trunks) > 1 {
		f.trunks = f.trunks[1:]
		f.headPage = trunk.NextTrunk
		return f.Allocate()
	}

	// This was the last trunk and it's empty - freelist is empty
	f.trunks = nil
	f.headPage = 0
	return 0, false
}

// Free adds a page to the freelist.
func (f *Freelist) Free(pageNo uint32) {
	// If no trunks exist, create one
	if len(f.trunks) == 0 {
		trunk := &FreelistTrunkPage{
			NextTrunk: 0,
			LeafPages: []uint32{pageNo},
		}
		f.trunks = []*FreelistTrunkPage{trunk}
		f.headPage = 1 // Placeholder - in real implementation, this would be allocated
		f.freeCount = 1
		return
	}

	// Try to add to the first trunk
	trunk := f.trunks[0]
	if !trunk.IsFull(f.pageSize) {
		trunk.AddLeaf(pageNo)
		f.freeCount++
		return
	}

	// First trunk is full - create a new trunk
	// In the real implementation, we'd allocate a new page for this trunk
	newTrunk := &FreelistTrunkPage{
		NextTrunk: f.headPage,
		LeafPages: []uint32{pageNo},
	}
	f.trunks = append([]*FreelistTrunkPage{newTrunk}, f.trunks...)
	f.headPage = f.headPage + 1 // Placeholder
	f.freeCount++
}

// PageRun represents a contiguous run of free pages.
type PageRun struct {
	Start  uint32 // First page in the run
	Length int    // Number of contiguous pages
}

// getAllFreePages returns a sorted list of all free page numbers.
func (f *Freelist) getAllFreePages() []uint32 {
	pages := make([]uint32, 0, f.freeCount)

	for _, trunk := range f.trunks {
		pages = append(pages, trunk.LeafPages...)
	}

	// Sort the pages
	sort.Slice(pages, func(i, j int) bool {
		return pages[i] < pages[j]
	})

	return pages
}

// ContiguousRuns analyzes the freelist and returns contiguous runs of free pages.
// This is useful for defragmentation analysis and vacuum operations.
func (f *Freelist) ContiguousRuns() []PageRun {
	if f.freeCount == 0 {
		return nil
	}

	pages := f.getAllFreePages()
	if len(pages) == 0 {
		return nil
	}

	runs := make([]PageRun, 0)
	currentRun := PageRun{Start: pages[0], Length: 1}

	for i := 1; i < len(pages); i++ {
		if pages[i] == pages[i-1]+1 {
			// Contiguous - extend current run
			currentRun.Length++
		} else {
			// Gap found - save current run and start new one
			runs = append(runs, currentRun)
			currentRun = PageRun{Start: pages[i], Length: 1}
		}
	}

	// Don't forget the last run
	runs = append(runs, currentRun)

	return runs
}

// Defragment reorganizes the freelist to group contiguous pages.
// This operation sorts the leaf pages within each trunk to enable
// better locality when allocating pages.
func (f *Freelist) Defragment() {
	if f.freeCount == 0 || len(f.trunks) == 0 {
		return
	}

	// Get all pages sorted
	pages := f.getAllFreePages()

	// Rebuild the trunk structure with sorted pages
	f.trunks = nil
	f.freeCount = 0

	for _, pageNo := range pages {
		f.Free(pageNo)
	}
}
