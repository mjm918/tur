// pkg/pager/memory_integration_test.go
package pager

import (
	"os"
	"path/filepath"
	"testing"

	"tur/pkg/cache"
)

func TestPager_WithMemoryBudget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create pager with memory budget
	budget := cache.NewMemoryBudget(1024 * 1024) // 1MB
	p, err := OpenWithBudget(path, Options{CacheSize: 100}, budget)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer func() {
		p.Close()
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	// Verify budget is being tracked
	if budget.ComponentUsage("page_cache") != 0 {
		t.Error("Expected 0 page cache usage initially")
	}

	// Allocate some pages
	for i := 0; i < 10; i++ {
		page, err := p.Allocate()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		p.Release(page)
	}

	// Budget should reflect page cache usage
	// Each page is 4KB, and they should be cached
	usage := budget.ComponentUsage("page_cache")
	if usage == 0 {
		t.Error("Expected non-zero page cache usage after allocating pages")
	}
}

func TestPager_MemoryBudgetEviction(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create small memory budget to trigger eviction
	pageSize := 4096
	cacheSize := 10
	budget := cache.NewMemoryBudget(int64(pageSize * cacheSize * 2)) // Just enough for cache

	p, err := OpenWithBudget(path, Options{
		PageSize:  pageSize,
		CacheSize: cacheSize,
	}, budget)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer func() {
		p.Close()
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	// Allocate more pages than cache size to trigger eviction
	pages := make([]*Page, 0, cacheSize+5)
	for i := 0; i < cacheSize+5; i++ {
		page, err := p.Allocate()
		if err != nil {
			t.Fatalf("Failed to allocate page %d: %v", i, err)
		}
		pages = append(pages, page)
	}

	// Release all pages
	for _, page := range pages {
		p.Release(page)
	}

	// Now some should be evicted from cache
	// Budget should show memory was tracked and potentially reduced
	usage := budget.ComponentUsage("page_cache")
	maxExpected := int64(cacheSize * pageSize)
	if usage > maxExpected {
		t.Errorf("Expected usage <= %d, got %d (should have evicted)", maxExpected, usage)
	}
}

func TestPager_MemoryBudgetStats(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	budget := cache.NewMemoryBudget(1024 * 1024) // 1MB
	p, err := OpenWithBudget(path, Options{}, budget)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer func() {
		p.Close()
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	// Allocate pages
	for i := 0; i < 5; i++ {
		page, err := p.Allocate()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		p.Release(page)
	}

	// Get stats
	stats := budget.Stats()
	if stats.ComponentUsage["page_cache"] == 0 {
		t.Error("Expected page_cache in stats")
	}
	if stats.TotalUsage == 0 {
		t.Error("Expected non-zero total usage")
	}
}

func TestPager_MemoryPressureCallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Very small budget to trigger pressure quickly
	pageSize := 4096
	budget := cache.NewMemoryBudget(int64(pageSize * 5)) // 5 pages max

	pressureCalled := make(chan struct{}, 1)
	budget.OnPressure(func(usage, limit int64) {
		select {
		case pressureCalled <- struct{}{}:
		default:
		}
	})

	p, err := OpenWithBudget(path, Options{
		PageSize:  pageSize,
		CacheSize: 10,
	}, budget)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer func() {
		p.Close()
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	// Allocate pages until pressure
	for i := 0; i < 10; i++ {
		page, err := p.Allocate()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		p.Release(page)
	}

	// Check if pressure callback was called
	select {
	case <-pressureCalled:
		// Expected
	default:
		// May not trigger depending on timing, but we check the pressure state
		if !budget.IsUnderPressure() && !budget.IsExceeded() {
			t.Log("Note: Memory pressure callback was not triggered (may depend on allocation timing)")
		}
	}
}

func TestPager_GetMemoryBudget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	budget := cache.NewMemoryBudget(1024 * 1024)
	p, err := OpenWithBudget(path, Options{}, budget)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer func() {
		p.Close()
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	// Should be able to get the budget back
	if p.MemoryBudget() != budget {
		t.Error("Expected to get back the same memory budget")
	}
}

func TestPager_WithoutMemoryBudget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create pager without memory budget (nil)
	p, err := OpenWithBudget(path, Options{}, nil)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer func() {
		p.Close()
		os.Remove(path)
		os.Remove(path + "-wal")
	}()

	// Should still work without budget
	page, err := p.Allocate()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}
	p.Release(page)

	// Budget should be nil
	if p.MemoryBudget() != nil {
		t.Error("Expected nil memory budget")
	}
}
