// pkg/pager/pager_test.go
package pager

import (
	"path/filepath"
	"testing"
)

func TestPagerCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	if p.PageSize() != 4096 {
		t.Errorf("expected page size 4096, got %d", p.PageSize())
	}
}

func TestPagerAllocatePage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Allocate first page (page 1, since page 0 is header)
	page, err := p.Allocate()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}
	if page.PageNo() != 1 {
		t.Errorf("expected page number 1, got %d", page.PageNo())
	}

	// Allocate second page
	page2, err := p.Allocate()
	if err != nil {
		t.Fatalf("failed to allocate second page: %v", err)
	}
	if page2.PageNo() != 2 {
		t.Errorf("expected page number 2, got %d", page2.PageNo())
	}
}

func TestPagerGetPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	// Allocate and write to a page
	page, _ := p.Allocate()
	pageNo := page.PageNo()
	copy(page.Data()[10:], []byte("test data"))
	page.SetDirty(true)
	p.Release(page)

	// Close and reopen
	p.Close()

	p2, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer p2.Close()

	// Get the page back
	page2, err := p2.Get(pageNo)
	if err != nil {
		t.Fatalf("failed to get page: %v", err)
	}

	if string(page2.Data()[10:19]) != "test data" {
		t.Errorf("data not persisted correctly")
	}
}

func TestPagerHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	p.Close()

	// Reopen and check header
	p2, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("failed to reopen: %v", err)
	}
	defer p2.Close()

	if p2.PageSize() != 4096 {
		t.Errorf("page size not persisted, got %d", p2.PageSize())
	}
}

func TestPagerLRUCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Small cache size to test eviction
	p, err := Open(path, Options{PageSize: 4096, CacheSize: 5})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Allocate more pages than cache can hold
	pageNos := make([]uint32, 10)
	for i := 0; i < 10; i++ {
		page, err := p.Allocate()
		if err != nil {
			t.Fatalf("failed to allocate page %d: %v", i, err)
		}
		pageNos[i] = page.PageNo()
		// Write unique data
		copy(page.Data()[0:8], []byte{byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i)})
		page.SetDirty(true)
		p.Release(page)
	}

	// Access pages in different order - should trigger evictions and re-loads from mmap
	for i := 9; i >= 0; i-- {
		page, err := p.Get(pageNos[i])
		if err != nil {
			t.Fatalf("failed to get page %d: %v", i, err)
		}
		// Verify data is correct (persisted via mmap)
		if page.Data()[0] != byte(i) {
			t.Errorf("page %d has wrong data: expected %d, got %d", i, i, page.Data()[0])
		}
		p.Release(page)
	}
}

// Freelist integration tests

func TestPagerFree_ReturnsPageToFreelist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Allocate a page
	page, err := p.Allocate()
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}
	freedPageNo := page.PageNo()
	p.Release(page)

	// Free the page - this is the new method we need to implement
	err = p.Free(freedPageNo)
	if err != nil {
		t.Fatalf("failed to free page: %v", err)
	}

	// Freelist count should now be 1
	if p.FreePageCount() != 1 {
		t.Errorf("expected 1 free page, got %d", p.FreePageCount())
	}
}

func TestPagerAllocate_UsesFreelist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Allocate and free a page
	page, _ := p.Allocate()
	freedPageNo := page.PageNo()
	p.Release(page)
	p.Free(freedPageNo)

	// Record page count before next allocation
	pageCountBefore := p.PageCount()

	// Allocate again - should reuse the freed page
	page2, err := p.Allocate()
	if err != nil {
		t.Fatalf("failed to allocate: %v", err)
	}

	// Should get the same page number back (LIFO)
	if page2.PageNo() != freedPageNo {
		t.Errorf("expected to reuse page %d, got %d", freedPageNo, page2.PageNo())
	}

	// Page count should not have increased
	if p.PageCount() != pageCountBefore {
		t.Errorf("page count increased from %d to %d when reusing freed page",
			pageCountBefore, p.PageCount())
	}
}

func TestPagerFreelist_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// First session: allocate and free pages
	p, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	// Allocate 3 pages
	var pages []*Page
	for i := 0; i < 3; i++ {
		page, _ := p.Allocate()
		pages = append(pages, page)
	}

	// Free pages 1 and 2 (keep page 0)
	p.Release(pages[1])
	p.Free(pages[1].PageNo())
	p.Release(pages[2])
	p.Free(pages[2].PageNo())
	p.Release(pages[0])

	// Close the pager
	p.Close()

	// Second session: reopen and verify freelist is restored
	p2, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to reopen pager: %v", err)
	}
	defer p2.Close()

	// Should have 2 free pages
	if p2.FreePageCount() != 2 {
		t.Errorf("expected 2 free pages after reopen, got %d", p2.FreePageCount())
	}

	// Allocating should reuse freed pages
	newPage, _ := p2.Allocate()
	if newPage.PageNo() != pages[2].PageNo() && newPage.PageNo() != pages[1].PageNo() {
		t.Errorf("expected to reuse freed page, got new page %d", newPage.PageNo())
	}
}

func TestPagerFreelist_LIFO(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Allocate 3 pages
	page1, _ := p.Allocate()
	page2, _ := p.Allocate()
	page3, _ := p.Allocate()
	p1No, p2No, p3No := page1.PageNo(), page2.PageNo(), page3.PageNo()
	p.Release(page1)
	p.Release(page2)
	p.Release(page3)

	// Free in order: 1, 2, 3
	p.Free(p1No)
	p.Free(p2No)
	p.Free(p3No)

	// Allocate should return in LIFO order: 3, 2, 1
	alloc1, _ := p.Allocate()
	alloc2, _ := p.Allocate()
	alloc3, _ := p.Allocate()

	if alloc1.PageNo() != p3No {
		t.Errorf("first alloc: expected %d (last freed), got %d", p3No, alloc1.PageNo())
	}
	if alloc2.PageNo() != p2No {
		t.Errorf("second alloc: expected %d, got %d", p2No, alloc2.PageNo())
	}
	if alloc3.PageNo() != p1No {
		t.Errorf("third alloc: expected %d (first freed), got %d", p1No, alloc3.PageNo())
	}
}

func TestPagerFreelist_GrowsWhenEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := Open(path, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Freelist is empty
	if p.FreePageCount() != 0 {
		t.Fatalf("expected empty freelist, got %d pages", p.FreePageCount())
	}

	// Allocate should still work by growing the file
	page, err := p.Allocate()
	if err != nil {
		t.Fatalf("allocate should succeed even with empty freelist: %v", err)
	}

	if page.PageNo() == 0 {
		t.Error("should not allocate page 0 (header page)")
	}
}
