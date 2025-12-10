// pkg/pager/pager_memory_test.go
package pager

import (
	"testing"
)

// TestOpenWithStorage tests creating a pager with a custom storage backend
func TestOpenWithStorage(t *testing.T) {
	pageSize := 4096

	// Create in-memory storage
	storage, err := NewMemoryStorage(int64(pageSize))
	if err != nil {
		t.Fatalf("Failed to create MemoryStorage: %v", err)
	}

	// Open pager with in-memory storage
	opts := Options{
		PageSize:  pageSize,
		CacheSize: 100,
	}

	p, err := OpenWithStorage(storage, opts)
	if err != nil {
		t.Fatalf("Failed to open pager with MemoryStorage: %v", err)
	}
	defer p.Close()

	// Verify pager is functional
	if p.PageSize() != pageSize {
		t.Errorf("Expected page size %d, got %d", pageSize, p.PageSize())
	}

	// Verify in-memory mode
	if !p.IsInMemory() {
		t.Error("Expected pager to be in-memory mode")
	}

	// Allocate a page
	page, err := p.Allocate()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	// Write data to the page
	testString := "Test data for in-memory page"
	copy(page.Data(), []byte(testString))
	page.SetDirty(true)
	pageNo := page.PageNo()

	// Verify data was written (page still in memory, backed by MemoryStorage)
	got := string(page.Data()[:len(testString)])
	if got != testString {
		t.Errorf("Data not written correctly: expected %q, got %q", testString, got)
	}

	p.Release(page)

	// Get the page again and verify data persists
	retrievedPage, err := p.Get(pageNo)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	// Verify data persists (page data should point to same underlying storage)
	got = string(retrievedPage.Data()[:len(testString)])
	if got != testString {
		t.Errorf("Data not persisted correctly: expected %q, got %q", testString, got)
	}

	p.Release(retrievedPage)
}

// TestInMemoryPagerNoWAL tests that in-memory pager doesn't create WAL files
func TestInMemoryPagerNoWAL(t *testing.T) {
	pageSize := 4096

	storage, err := NewMemoryStorage(int64(pageSize))
	if err != nil {
		t.Fatalf("Failed to create MemoryStorage: %v", err)
	}

	opts := Options{
		PageSize:  pageSize,
		CacheSize: 100,
	}

	p, err := OpenWithStorage(storage, opts)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	// Verify no WAL is created for in-memory pager
	if p.HasWAL() {
		t.Error("In-memory pager should not have WAL")
	}
}

// TestInMemoryPagerTransactions tests basic transaction support
func TestInMemoryPagerTransactions(t *testing.T) {
	pageSize := 4096

	storage, err := NewMemoryStorage(int64(pageSize))
	if err != nil {
		t.Fatalf("Failed to create MemoryStorage: %v", err)
	}

	opts := Options{
		PageSize:  pageSize,
		CacheSize: 100,
	}

	p, err := OpenWithStorage(storage, opts)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	// Begin transaction
	tx, err := p.BeginWrite()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Allocate and modify a page
	page, err := p.Allocate()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	originalData := make([]byte, len(page.Data()))
	copy(originalData, page.Data())

	copy(page.Data(), []byte("Transaction data"))
	p.MarkDirty(page)
	page.SetDirty(true)
	pageNo := page.PageNo()
	p.Release(page)

	// Commit transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify data persists after commit
	page, err = p.Get(pageNo)
	if err != nil {
		t.Fatalf("Failed to get page after commit: %v", err)
	}

	if string(page.Data()[:16]) != "Transaction data" {
		t.Errorf("Data not persisted after commit")
	}
	p.Release(page)
}

// TestInMemoryPagerRollback tests transaction rollback
func TestInMemoryPagerRollback(t *testing.T) {
	pageSize := 4096

	storage, err := NewMemoryStorage(int64(pageSize))
	if err != nil {
		t.Fatalf("Failed to create MemoryStorage: %v", err)
	}

	opts := Options{
		PageSize:  pageSize,
		CacheSize: 100,
	}

	p, err := OpenWithStorage(storage, opts)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	// Allocate a page first (outside transaction)
	page, err := p.Allocate()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	copy(page.Data(), []byte("Initial data"))
	pageNo := page.PageNo()
	p.Release(page)

	// Begin transaction
	tx, err := p.BeginWrite()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Modify the page
	page, err = p.Get(pageNo)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	p.MarkDirty(page)
	copy(page.Data(), []byte("Modified data"))
	page.SetDirty(true)
	p.Release(page)

	// Rollback transaction
	tx.Rollback()

	// Verify data is restored to original
	page, err = p.Get(pageNo)
	if err != nil {
		t.Fatalf("Failed to get page after rollback: %v", err)
	}

	if string(page.Data()[:12]) != "Initial data" {
		t.Errorf("Data not restored after rollback, got: %s", string(page.Data()[:12]))
	}
	p.Release(page)
}

// TestInMemoryPagerMultiplePages tests allocating and using multiple pages
func TestInMemoryPagerMultiplePages(t *testing.T) {
	pageSize := 4096

	storage, err := NewMemoryStorage(int64(pageSize))
	if err != nil {
		t.Fatalf("Failed to create MemoryStorage: %v", err)
	}

	opts := Options{
		PageSize:  pageSize,
		CacheSize: 100,
	}

	p, err := OpenWithStorage(storage, opts)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	// Allocate multiple pages
	pageNos := make([]uint32, 10)
	for i := 0; i < 10; i++ {
		page, err := p.Allocate()
		if err != nil {
			t.Fatalf("Failed to allocate page %d: %v", i, err)
		}

		// Write unique data to each page
		data := []byte("Page data " + string(rune('A'+i)))
		copy(page.Data(), data)
		page.SetDirty(true)
		pageNos[i] = page.PageNo()
		p.Release(page)
	}

	// Verify all pages have correct data
	for i, pageNo := range pageNos {
		page, err := p.Get(pageNo)
		if err != nil {
			t.Fatalf("Failed to get page %d: %v", i, err)
		}

		expected := "Page data " + string(rune('A'+i))
		if string(page.Data()[:len(expected)]) != expected {
			t.Errorf("Page %d has wrong data: expected %q, got %q",
				i, expected, string(page.Data()[:len(expected)]))
		}
		p.Release(page)
	}

	// Verify page count
	if p.PageCount() != uint32(11) { // 1 header page + 10 data pages
		t.Errorf("Expected 11 pages, got %d", p.PageCount())
	}
}
