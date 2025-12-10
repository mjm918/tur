// pkg/cache/memory_budget_test.go
package cache

import (
	"sync"
	"testing"
	"time"
)

func TestMemoryBudget_NewMemoryBudget(t *testing.T) {
	// Test creating a new memory budget with default limit
	budget := NewMemoryBudget(0)
	if budget == nil {
		t.Fatal("NewMemoryBudget returned nil")
	}
	if budget.Limit() != DefaultMemoryLimit {
		t.Errorf("Expected default limit %d, got %d", DefaultMemoryLimit, budget.Limit())
	}

	// Test creating with custom limit
	customLimit := int64(1024 * 1024 * 100) // 100MB
	budget2 := NewMemoryBudget(customLimit)
	if budget2.Limit() != customLimit {
		t.Errorf("Expected custom limit %d, got %d", customLimit, budget2.Limit())
	}
}

func TestMemoryBudget_TrackUsage(t *testing.T) {
	budget := NewMemoryBudget(1024 * 1024) // 1MB limit

	// Register a component
	budget.RegisterComponent("page_cache")
	budget.RegisterComponent("stmt_cache")

	// Track usage
	budget.Track("page_cache", 4096)
	if budget.ComponentUsage("page_cache") != 4096 {
		t.Errorf("Expected page_cache usage 4096, got %d", budget.ComponentUsage("page_cache"))
	}

	budget.Track("stmt_cache", 1024)
	if budget.ComponentUsage("stmt_cache") != 1024 {
		t.Errorf("Expected stmt_cache usage 1024, got %d", budget.ComponentUsage("stmt_cache"))
	}

	// Total usage should be sum
	if budget.TotalUsage() != 5120 {
		t.Errorf("Expected total usage 5120, got %d", budget.TotalUsage())
	}
}

func TestMemoryBudget_Release(t *testing.T) {
	budget := NewMemoryBudget(1024 * 1024)
	budget.RegisterComponent("test")

	budget.Track("test", 4096)
	if budget.ComponentUsage("test") != 4096 {
		t.Errorf("Expected usage 4096, got %d", budget.ComponentUsage("test"))
	}

	// Release some memory
	budget.Release("test", 1024)
	if budget.ComponentUsage("test") != 3072 {
		t.Errorf("Expected usage 3072, got %d", budget.ComponentUsage("test"))
	}

	// Release all remaining
	budget.Release("test", 3072)
	if budget.ComponentUsage("test") != 0 {
		t.Errorf("Expected usage 0, got %d", budget.ComponentUsage("test"))
	}
}

func TestMemoryBudget_IsUnderPressure(t *testing.T) {
	limit := int64(1000)
	budget := NewMemoryBudget(limit)
	budget.RegisterComponent("test")

	// Under threshold (default 80%)
	budget.Track("test", 700)
	if budget.IsUnderPressure() {
		t.Error("Should not be under pressure at 70% usage")
	}

	// At or over threshold
	budget.Track("test", 100) // Now at 800 = 80%
	if !budget.IsUnderPressure() {
		t.Error("Should be under pressure at 80% usage")
	}

	budget.Track("test", 100) // Now at 900 = 90%
	if !budget.IsUnderPressure() {
		t.Error("Should be under pressure at 90% usage")
	}
}

func TestMemoryBudget_IsExceeded(t *testing.T) {
	limit := int64(1000)
	budget := NewMemoryBudget(limit)
	budget.RegisterComponent("test")

	// Under limit
	budget.Track("test", 900)
	if budget.IsExceeded() {
		t.Error("Should not be exceeded at 90% usage")
	}

	// At limit
	budget.Track("test", 100) // Now at 1000 = 100%
	if budget.IsExceeded() {
		t.Error("Should not be exceeded at exactly 100% usage")
	}

	// Over limit
	budget.Track("test", 100) // Now at 1100 = 110%
	if !budget.IsExceeded() {
		t.Error("Should be exceeded at 110% usage")
	}
}

func TestMemoryBudget_SetLimit(t *testing.T) {
	budget := NewMemoryBudget(1000)
	budget.RegisterComponent("test")
	budget.Track("test", 500)

	// Increase limit
	budget.SetLimit(2000)
	if budget.Limit() != 2000 {
		t.Errorf("Expected limit 2000, got %d", budget.Limit())
	}

	// Decrease limit
	budget.SetLimit(800)
	if budget.Limit() != 800 {
		t.Errorf("Expected limit 800, got %d", budget.Limit())
	}
}

func TestMemoryBudget_SetPressureThreshold(t *testing.T) {
	budget := NewMemoryBudget(1000)
	budget.RegisterComponent("test")

	// Default threshold is 0.8 (80%)
	budget.Track("test", 750)
	if budget.IsUnderPressure() {
		t.Error("Should not be under pressure at 75% with 80% threshold")
	}

	// Lower threshold to 70%
	budget.SetPressureThreshold(0.7)
	if !budget.IsUnderPressure() {
		t.Error("Should be under pressure at 75% with 70% threshold")
	}

	// Raise threshold to 90%
	budget.SetPressureThreshold(0.9)
	if budget.IsUnderPressure() {
		t.Error("Should not be under pressure at 75% with 90% threshold")
	}
}

func TestMemoryBudget_OnPressureCallback(t *testing.T) {
	budget := NewMemoryBudget(1000)
	budget.RegisterComponent("test")

	callbackCalled := make(chan struct{}, 1)
	var callbackUsage int64
	var callbackLimit int64
	var mu sync.Mutex

	budget.OnPressure(func(usage, limit int64) {
		mu.Lock()
		callbackUsage = usage
		callbackLimit = limit
		mu.Unlock()
		select {
		case callbackCalled <- struct{}{}:
		default:
		}
	})

	// Track below threshold - no callback
	budget.Track("test", 700)
	select {
	case <-callbackCalled:
		t.Error("Callback should not be called when below threshold")
	case <-time.After(50 * time.Millisecond):
		// Expected - no callback
	}

	// Track over threshold - callback should fire
	budget.Track("test", 150) // 850 = 85%

	select {
	case <-callbackCalled:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("Callback should be called when over threshold")
	}

	mu.Lock()
	if callbackUsage != 850 {
		t.Errorf("Expected callback usage 850, got %d", callbackUsage)
	}
	if callbackLimit != 1000 {
		t.Errorf("Expected callback limit 1000, got %d", callbackLimit)
	}
	mu.Unlock()
}

func TestMemoryBudget_Stats(t *testing.T) {
	budget := NewMemoryBudget(1024 * 1024) // 1MB
	budget.RegisterComponent("page_cache")
	budget.RegisterComponent("stmt_cache")

	budget.Track("page_cache", 4096)
	budget.Track("stmt_cache", 1024)

	stats := budget.Stats()

	if stats.Limit != 1024*1024 {
		t.Errorf("Expected limit %d, got %d", 1024*1024, stats.Limit)
	}
	if stats.TotalUsage != 5120 {
		t.Errorf("Expected total usage 5120, got %d", stats.TotalUsage)
	}
	if stats.ComponentUsage["page_cache"] != 4096 {
		t.Errorf("Expected page_cache 4096, got %d", stats.ComponentUsage["page_cache"])
	}
	if stats.ComponentUsage["stmt_cache"] != 1024 {
		t.Errorf("Expected stmt_cache 1024, got %d", stats.ComponentUsage["stmt_cache"])
	}
}

func TestMemoryBudget_ConcurrentAccess(t *testing.T) {
	budget := NewMemoryBudget(1024 * 1024 * 100) // 100MB
	budget.RegisterComponent("test")

	var wg sync.WaitGroup
	iterations := 1000

	// Multiple goroutines tracking and releasing
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				budget.Track("test", 1024)
				budget.Release("test", 1024)
			}
		}()
	}

	wg.Wait()

	// Final usage should be 0 (all tracked and released equally)
	if budget.ComponentUsage("test") != 0 {
		t.Errorf("Expected final usage 0, got %d", budget.ComponentUsage("test"))
	}
}

func TestMemoryBudget_HotDataTracking(t *testing.T) {
	budget := NewMemoryBudget(10000)
	budget.RegisterComponent("cache")

	// Track with access frequency info
	budget.TrackWithPriority("cache", "key1", 1000, PriorityHot)
	budget.TrackWithPriority("cache", "key2", 1000, PriorityCold)
	budget.TrackWithPriority("cache", "key3", 1000, PriorityWarm)

	// Get eviction candidates (cold first)
	candidates := budget.GetEvictionCandidates("cache", 1000)
	if len(candidates) == 0 {
		t.Error("Expected at least one eviction candidate")
	}

	// First candidate should be cold data
	if len(candidates) > 0 && candidates[0] != "key2" {
		t.Errorf("Expected first eviction candidate to be 'key2' (cold), got '%s'", candidates[0])
	}
}

func TestMemoryBudget_AccessTracking(t *testing.T) {
	budget := NewMemoryBudget(10000)
	budget.RegisterComponent("cache")

	// Track item
	budget.TrackWithPriority("cache", "key1", 1000, PriorityCold)

	// Record accesses to make it hot
	for i := 0; i < 10; i++ {
		budget.RecordAccess("cache", "key1")
	}

	// Check that priority was upgraded
	info := budget.GetItemInfo("cache", "key1")
	if info == nil {
		t.Fatal("Expected item info for key1")
	}
	if info.Priority != PriorityHot {
		t.Errorf("Expected priority Hot after many accesses, got %v", info.Priority)
	}
}

func TestMemoryBudget_DecayPriority(t *testing.T) {
	budget := NewMemoryBudget(10000)
	budget.RegisterComponent("cache")

	// Track hot item with backdated last access
	budget.TrackWithPriority("cache", "key1", 1000, PriorityHot)

	// Manually set last access to be old so decay triggers
	budget.SetItemLastAccess("cache", "key1", time.Now().Add(-time.Hour))

	// Decay items older than 1 minute
	budget.DecayPriorities("cache", time.Minute)

	// Check that priority was decayed
	info := budget.GetItemInfo("cache", "key1")
	if info == nil {
		t.Fatal("Expected item info for key1")
	}
	if info.Priority == PriorityHot {
		t.Error("Expected priority to decay from Hot")
	}
}
