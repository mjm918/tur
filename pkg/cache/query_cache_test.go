// pkg/cache/query_cache_test.go
package cache

import (
	"sync"
	"testing"
	"time"

	"tur/pkg/types"
)

func TestQueryCache_NewQueryCache(t *testing.T) {
	// Default capacity
	cache := NewQueryCache(0)
	if cache == nil {
		t.Fatal("NewQueryCache returned nil")
	}
	if cache.Capacity() != DefaultQueryCacheCapacity {
		t.Errorf("Expected default capacity %d, got %d", DefaultQueryCacheCapacity, cache.Capacity())
	}

	// Custom capacity
	cache2 := NewQueryCache(100)
	if cache2.Capacity() != 100 {
		t.Errorf("Expected capacity 100, got %d", cache2.Capacity())
	}
}

func TestQueryCache_GenerateCacheKey(t *testing.T) {
	// Same SQL and params should generate same key
	sql := "SELECT * FROM users WHERE id = ?"
	params := []types.Value{types.NewInt(42)}

	key1 := GenerateCacheKey(sql, params)
	key2 := GenerateCacheKey(sql, params)

	if key1 != key2 {
		t.Errorf("Same SQL and params should generate same key")
	}

	// Different SQL should generate different key
	sql2 := "SELECT * FROM users WHERE name = ?"
	key3 := GenerateCacheKey(sql2, params)

	if key1 == key3 {
		t.Errorf("Different SQL should generate different key")
	}

	// Different params should generate different key
	params2 := []types.Value{types.NewInt(99)}
	key4 := GenerateCacheKey(sql, params2)

	if key1 == key4 {
		t.Errorf("Different params should generate different key")
	}
}

func TestQueryCache_PutAndGet(t *testing.T) {
	cache := NewQueryCache(100)

	sql := "SELECT * FROM users WHERE id = ?"
	params := []types.Value{types.NewInt(1)}
	key := GenerateCacheKey(sql, params)

	columns := []string{"id", "name"}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewText("Alice")},
	}

	// Put result
	cache.Put(key, columns, rows, []string{"users"})

	// Get result
	result, ok := cache.Get(key)
	if !ok {
		t.Fatal("Expected to find cached result")
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

func TestQueryCache_GetMiss(t *testing.T) {
	cache := NewQueryCache(100)

	_, ok := cache.Get("nonexistent-key")
	if ok {
		t.Error("Expected cache miss for nonexistent key")
	}
}

func TestQueryCache_LRUEviction(t *testing.T) {
	// Small cache to trigger eviction
	cache := NewQueryCache(3)

	// Add 3 entries
	for i := 0; i < 3; i++ {
		key := GenerateCacheKey("SELECT ?", []types.Value{types.NewInt(int64(i))})
		cache.Put(key, []string{"col"}, [][]types.Value{{types.NewInt(int64(i))}}, []string{"table"})
	}

	// Add one more - should evict oldest
	key4 := GenerateCacheKey("SELECT ?", []types.Value{types.NewInt(999)})
	cache.Put(key4, []string{"col"}, [][]types.Value{{types.NewInt(999)}}, []string{"table"})

	// First entry should be evicted
	key1 := GenerateCacheKey("SELECT ?", []types.Value{types.NewInt(0)})
	_, ok := cache.Get(key1)
	if ok {
		t.Error("Oldest entry should have been evicted")
	}

	// Newest entry should still be there
	_, ok = cache.Get(key4)
	if !ok {
		t.Error("Newest entry should still be in cache")
	}
}

func TestQueryCache_InvalidateTable(t *testing.T) {
	cache := NewQueryCache(100)

	// Add entries for different tables
	key1 := GenerateCacheKey("SELECT * FROM users", nil)
	cache.Put(key1, []string{"id"}, [][]types.Value{{types.NewInt(1)}}, []string{"users"})

	key2 := GenerateCacheKey("SELECT * FROM orders", nil)
	cache.Put(key2, []string{"id"}, [][]types.Value{{types.NewInt(1)}}, []string{"orders"})

	key3 := GenerateCacheKey("SELECT * FROM users JOIN orders", nil)
	cache.Put(key3, []string{"id"}, [][]types.Value{{types.NewInt(1)}}, []string{"users", "orders"})

	// Invalidate users table
	cache.InvalidateTable("users")

	// users-only query should be invalidated
	_, ok := cache.Get(key1)
	if ok {
		t.Error("users query should be invalidated")
	}

	// orders-only query should still be valid
	_, ok = cache.Get(key2)
	if !ok {
		t.Error("orders query should still be cached")
	}

	// join query should be invalidated (touches users)
	_, ok = cache.Get(key3)
	if ok {
		t.Error("join query should be invalidated")
	}
}

func TestQueryCache_InvalidateAll(t *testing.T) {
	cache := NewQueryCache(100)

	// Add some entries
	for i := 0; i < 5; i++ {
		key := GenerateCacheKey("SELECT ?", []types.Value{types.NewInt(int64(i))})
		cache.Put(key, []string{"col"}, [][]types.Value{{types.NewInt(int64(i))}}, []string{"table"})
	}

	// Invalidate all
	cache.InvalidateAll()

	// All should be gone
	for i := 0; i < 5; i++ {
		key := GenerateCacheKey("SELECT ?", []types.Value{types.NewInt(int64(i))})
		_, ok := cache.Get(key)
		if ok {
			t.Errorf("Entry %d should be invalidated", i)
		}
	}
}

func TestQueryCache_Stats(t *testing.T) {
	cache := NewQueryCache(100)

	// Initial stats
	stats := cache.Stats()
	if stats.Hits != 0 || stats.Misses != 0 || stats.Entries != 0 {
		t.Error("Initial stats should be zero")
	}

	// Add entry
	key := GenerateCacheKey("SELECT 1", nil)
	cache.Put(key, []string{"col"}, [][]types.Value{{types.NewInt(1)}}, []string{"table"})

	// Cache hit
	_, _ = cache.Get(key)
	stats = cache.Stats()
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.Hits)
	}

	// Cache miss
	_, _ = cache.Get("nonexistent")
	stats = cache.Stats()
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}

	// Entry count
	if stats.Entries != 1 {
		t.Errorf("Expected 1 entry, got %d", stats.Entries)
	}
}

func TestQueryCache_HitRate(t *testing.T) {
	cache := NewQueryCache(100)

	// Add entry
	key := GenerateCacheKey("SELECT 1", nil)
	cache.Put(key, []string{"col"}, [][]types.Value{{types.NewInt(1)}}, []string{"table"})

	// 1 hit, 1 miss = 50% hit rate
	_, _ = cache.Get(key)         // hit
	_, _ = cache.Get("nonexistent") // miss

	stats := cache.Stats()
	if stats.HitRate < 0.49 || stats.HitRate > 0.51 {
		t.Errorf("Expected hit rate ~0.5, got %f", stats.HitRate)
	}
}

func TestQueryCache_TTL(t *testing.T) {
	cache := NewQueryCache(100)
	cache.SetTTL(50 * time.Millisecond)

	key := GenerateCacheKey("SELECT 1", nil)
	cache.Put(key, []string{"col"}, [][]types.Value{{types.NewInt(1)}}, []string{"table"})

	// Should be available immediately
	_, ok := cache.Get(key)
	if !ok {
		t.Error("Entry should be available immediately")
	}

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// Should be expired
	_, ok = cache.Get(key)
	if ok {
		t.Error("Entry should be expired")
	}
}

func TestQueryCache_ConcurrentAccess(t *testing.T) {
	cache := NewQueryCache(1000)

	var wg sync.WaitGroup
	iterations := 100

	// Multiple goroutines reading and writing
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := GenerateCacheKey("SELECT ?", []types.Value{types.NewInt(int64(j))})
				cache.Put(key, []string{"col"}, [][]types.Value{{types.NewInt(int64(j))}}, []string{"table"})
				cache.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// Just ensure no panics or deadlocks
	stats := cache.Stats()
	if stats.Hits == 0 && stats.Misses == 0 {
		t.Log("Cache was not accessed during concurrent test")
	}
}

func TestQueryCache_WithMemoryBudget(t *testing.T) {
	budget := NewMemoryBudget(1024 * 1024) // 1MB
	cache := NewQueryCacheWithBudget(100, budget)

	key := GenerateCacheKey("SELECT 1", nil)
	cache.Put(key, []string{"col"}, [][]types.Value{{types.NewInt(1)}}, []string{"table"})

	// Budget should reflect cache usage
	usage := budget.ComponentUsage("query_cache")
	if usage == 0 {
		t.Error("Expected non-zero query cache usage in budget")
	}

	// Invalidate and check budget released
	cache.InvalidateAll()
	usage = budget.ComponentUsage("query_cache")
	if usage != 0 {
		t.Errorf("Expected 0 query cache usage after invalidate, got %d", usage)
	}
}

func TestQueryCache_SetCapacity(t *testing.T) {
	cache := NewQueryCache(100)

	// Add some entries
	for i := 0; i < 50; i++ {
		key := GenerateCacheKey("SELECT ?", []types.Value{types.NewInt(int64(i))})
		cache.Put(key, []string{"col"}, [][]types.Value{{types.NewInt(int64(i))}}, []string{"table"})
	}

	// Reduce capacity - should evict
	cache.SetCapacity(10)

	stats := cache.Stats()
	if stats.Entries > 10 {
		t.Errorf("Expected at most 10 entries after capacity reduction, got %d", stats.Entries)
	}
}
