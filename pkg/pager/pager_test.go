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
