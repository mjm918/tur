// pkg/cache/memory_budget.go
package cache

import (
	"sort"
	"sync"
	"time"
)

// DefaultMemoryLimit is the default memory budget (256MB)
const DefaultMemoryLimit = int64(256 * 1024 * 1024)

// DefaultPressureThreshold is the default threshold for memory pressure (80%)
const DefaultPressureThreshold = 0.8

// Priority represents the access priority of cached data
type Priority int

const (
	// PriorityCold represents rarely accessed data
	PriorityCold Priority = iota
	// PriorityWarm represents occasionally accessed data
	PriorityWarm
	// PriorityHot represents frequently accessed data
	PriorityHot
)

// ItemInfo holds metadata about a tracked item
type ItemInfo struct {
	Key        string
	Size       int64
	Priority   Priority
	AccessCount int64
	LastAccess time.Time
}

// MemoryBudgetStats contains statistics about memory usage
type MemoryBudgetStats struct {
	Limit          int64
	TotalUsage     int64
	ComponentUsage map[string]int64
	IsUnderPressure bool
	IsExceeded     bool
}

// PressureCallback is called when memory pressure is detected
type PressureCallback func(currentUsage, limit int64)

// MemoryBudget tracks memory usage across components and enforces limits
type MemoryBudget struct {
	mu                sync.RWMutex
	limit             int64
	pressureThreshold float64
	totalUsage        int64
	componentUsage    map[string]int64
	items             map[string]map[string]*ItemInfo // component -> key -> info
	pressureCallback  PressureCallback
	wasUnderPressure  bool
}

// NewMemoryBudget creates a new memory budget with the specified limit.
// If limit is 0 or negative, DefaultMemoryLimit is used.
func NewMemoryBudget(limit int64) *MemoryBudget {
	if limit <= 0 {
		limit = DefaultMemoryLimit
	}

	return &MemoryBudget{
		limit:             limit,
		pressureThreshold: DefaultPressureThreshold,
		componentUsage:    make(map[string]int64),
		items:             make(map[string]map[string]*ItemInfo),
	}
}

// Limit returns the current memory limit
func (mb *MemoryBudget) Limit() int64 {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.limit
}

// SetLimit updates the memory limit
func (mb *MemoryBudget) SetLimit(limit int64) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.limit = limit
}

// SetPressureThreshold sets the threshold (0.0 to 1.0) at which memory pressure is signaled
func (mb *MemoryBudget) SetPressureThreshold(threshold float64) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	if threshold < 0 {
		threshold = 0
	}
	if threshold > 1 {
		threshold = 1
	}
	mb.pressureThreshold = threshold
}

// RegisterComponent registers a component for memory tracking
func (mb *MemoryBudget) RegisterComponent(name string) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if _, exists := mb.componentUsage[name]; !exists {
		mb.componentUsage[name] = 0
		mb.items[name] = make(map[string]*ItemInfo)
	}
}

// Track adds memory usage for a component
func (mb *MemoryBudget) Track(component string, bytes int64) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	mb.componentUsage[component] += bytes
	mb.totalUsage += bytes

	// Check for pressure transition and fire callback
	mb.checkPressure()
}

// Release removes memory usage for a component
func (mb *MemoryBudget) Release(component string, bytes int64) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	usage := mb.componentUsage[component]
	if bytes > usage {
		bytes = usage
	}

	mb.componentUsage[component] -= bytes
	mb.totalUsage -= bytes
	if mb.totalUsage < 0 {
		mb.totalUsage = 0
	}
}

// TrackWithPriority tracks memory usage with priority information for eviction
func (mb *MemoryBudget) TrackWithPriority(component, key string, bytes int64, priority Priority) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if mb.items[component] == nil {
		mb.items[component] = make(map[string]*ItemInfo)
	}

	mb.items[component][key] = &ItemInfo{
		Key:        key,
		Size:       bytes,
		Priority:   priority,
		AccessCount: 0,
		LastAccess: time.Now(),
	}

	mb.componentUsage[component] += bytes
	mb.totalUsage += bytes

	mb.checkPressure()
}

// ReleaseItem releases a specific tracked item
func (mb *MemoryBudget) ReleaseItem(component, key string) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if items, ok := mb.items[component]; ok {
		if info, ok := items[key]; ok {
			mb.componentUsage[component] -= info.Size
			mb.totalUsage -= info.Size
			delete(items, key)
		}
	}
}

// RecordAccess records an access to an item, potentially upgrading its priority
func (mb *MemoryBudget) RecordAccess(component, key string) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if items, ok := mb.items[component]; ok {
		if info, ok := items[key]; ok {
			info.AccessCount++
			info.LastAccess = time.Now()

			// Upgrade priority based on access count
			if info.AccessCount >= 10 && info.Priority < PriorityHot {
				info.Priority = PriorityHot
			} else if info.AccessCount >= 3 && info.Priority < PriorityWarm {
				info.Priority = PriorityWarm
			}
		}
	}
}

// GetItemInfo returns information about a tracked item
func (mb *MemoryBudget) GetItemInfo(component, key string) *ItemInfo {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	if items, ok := mb.items[component]; ok {
		if info, ok := items[key]; ok {
			// Return a copy to avoid race conditions
			return &ItemInfo{
				Key:        info.Key,
				Size:       info.Size,
				Priority:   info.Priority,
				AccessCount: info.AccessCount,
				LastAccess: info.LastAccess,
			}
		}
	}
	return nil
}

// DecayPriorities reduces priority of items that haven't been accessed recently
func (mb *MemoryBudget) DecayPriorities(component string, maxAge time.Duration) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	now := time.Now()
	if items, ok := mb.items[component]; ok {
		for _, info := range items {
			if now.Sub(info.LastAccess) > maxAge {
				if info.Priority > PriorityCold {
					info.Priority--
				}
			}
		}
	}
}

// GetEvictionCandidates returns keys to evict to free the specified bytes
// Items are sorted by priority (cold first), then by least recently accessed
func (mb *MemoryBudget) GetEvictionCandidates(component string, bytesNeeded int64) []string {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	items, ok := mb.items[component]
	if !ok || len(items) == 0 {
		return nil
	}

	// Create a sorted list of items
	type sortableItem struct {
		key      string
		info     *ItemInfo
	}

	sortedItems := make([]sortableItem, 0, len(items))
	for key, info := range items {
		sortedItems = append(sortedItems, sortableItem{key: key, info: info})
	}

	// Sort by priority (ascending), then by last access (ascending = oldest first)
	sort.Slice(sortedItems, func(i, j int) bool {
		if sortedItems[i].info.Priority != sortedItems[j].info.Priority {
			return sortedItems[i].info.Priority < sortedItems[j].info.Priority
		}
		return sortedItems[i].info.LastAccess.Before(sortedItems[j].info.LastAccess)
	})

	// Select items until we have enough bytes
	var candidates []string
	var freedBytes int64
	for _, item := range sortedItems {
		if freedBytes >= bytesNeeded {
			break
		}
		candidates = append(candidates, item.key)
		freedBytes += item.info.Size
	}

	return candidates
}

// TotalUsage returns the total memory usage across all components
func (mb *MemoryBudget) TotalUsage() int64 {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.totalUsage
}

// ComponentUsage returns the memory usage for a specific component
func (mb *MemoryBudget) ComponentUsage(component string) int64 {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.componentUsage[component]
}

// IsUnderPressure returns true if memory usage exceeds the pressure threshold
func (mb *MemoryBudget) IsUnderPressure() bool {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return float64(mb.totalUsage) >= float64(mb.limit)*mb.pressureThreshold
}

// IsExceeded returns true if memory usage exceeds the limit
func (mb *MemoryBudget) IsExceeded() bool {
	mb.mu.RLock()
	defer mb.mu.RUnlock()
	return mb.totalUsage > mb.limit
}

// OnPressure registers a callback to be called when memory pressure is detected
func (mb *MemoryBudget) OnPressure(callback PressureCallback) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.pressureCallback = callback
}

// checkPressure checks if we crossed into pressure state and fires callback
// Must be called while holding the lock
func (mb *MemoryBudget) checkPressure() {
	isUnderPressure := float64(mb.totalUsage) >= float64(mb.limit)*mb.pressureThreshold

	// Only fire callback on transition into pressure state
	if isUnderPressure && !mb.wasUnderPressure && mb.pressureCallback != nil {
		// Fire callback outside lock to avoid deadlock
		callback := mb.pressureCallback
		usage := mb.totalUsage
		limit := mb.limit
		mb.wasUnderPressure = true

		go callback(usage, limit)
	} else if !isUnderPressure {
		mb.wasUnderPressure = false
	}
}

// SetItemLastAccess sets the last access time for an item (useful for testing)
func (mb *MemoryBudget) SetItemLastAccess(component, key string, t time.Time) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if items, ok := mb.items[component]; ok {
		if info, ok := items[key]; ok {
			info.LastAccess = t
		}
	}
}

// Stats returns statistics about memory usage
func (mb *MemoryBudget) Stats() MemoryBudgetStats {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	componentUsage := make(map[string]int64)
	for k, v := range mb.componentUsage {
		componentUsage[k] = v
	}

	return MemoryBudgetStats{
		Limit:           mb.limit,
		TotalUsage:      mb.totalUsage,
		ComponentUsage:  componentUsage,
		IsUnderPressure: float64(mb.totalUsage) >= float64(mb.limit)*mb.pressureThreshold,
		IsExceeded:      mb.totalUsage > mb.limit,
	}
}
