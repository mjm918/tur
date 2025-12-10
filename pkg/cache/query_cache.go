// pkg/cache/query_cache.go
package cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sync"
	"time"

	"tur/pkg/types"
)

// DefaultQueryCacheCapacity is the default number of query results to cache
const DefaultQueryCacheCapacity = 1000

// CachedResult holds the cached result of a query
type CachedResult struct {
	Columns   []string
	Rows      [][]types.Value
	Tables    []string // Tables this result depends on (for invalidation)
	CreatedAt time.Time
	Size      int64 // Estimated size in bytes
}

// QueryCacheStats holds statistics about the query cache
type QueryCacheStats struct {
	Hits     int64
	Misses   int64
	Entries  int
	Capacity int
	HitRate  float64
}

// cacheEntry holds a cached result and its LRU element
type queryCacheEntry struct {
	key     string
	result  *CachedResult
	element *list.Element
}

// QueryCache is an LRU cache for query results
type QueryCache struct {
	mu           sync.RWMutex
	capacity     int
	cache        map[string]*queryCacheEntry
	lru          *list.List
	tableIndex   map[string]map[string]struct{} // table -> set of cache keys
	hits         int64
	misses       int64
	ttl          time.Duration
	memoryBudget *MemoryBudget
}

// NewQueryCache creates a new query cache with the specified capacity.
// If capacity is 0 or negative, DefaultQueryCacheCapacity is used.
func NewQueryCache(capacity int) *QueryCache {
	return NewQueryCacheWithBudget(capacity, nil)
}

// NewQueryCacheWithBudget creates a new query cache with memory budget tracking.
func NewQueryCacheWithBudget(capacity int, budget *MemoryBudget) *QueryCache {
	if capacity <= 0 {
		capacity = DefaultQueryCacheCapacity
	}

	qc := &QueryCache{
		capacity:     capacity,
		cache:        make(map[string]*queryCacheEntry),
		lru:          list.New(),
		tableIndex:   make(map[string]map[string]struct{}),
		memoryBudget: budget,
	}

	if budget != nil {
		budget.RegisterComponent("query_cache")
	}

	return qc
}

// GenerateCacheKey creates a unique cache key from SQL and parameters
func GenerateCacheKey(sql string, params []types.Value) string {
	h := sha256.New()

	// Add SQL to hash
	h.Write([]byte(sql))

	// Add parameter separator
	h.Write([]byte{0})

	// Add each parameter
	for _, param := range params {
		// Write type
		h.Write([]byte{byte(param.Type())})

		// Write value based on type
		switch param.Type() {
		case types.TypeNull:
			// Nothing to write
		case types.TypeInt:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(param.Int()))
			h.Write(buf)
		case types.TypeFloat:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(param.Int())) // Bit representation
			h.Write(buf)
		case types.TypeText:
			h.Write([]byte(param.Text()))
		case types.TypeBlob:
			h.Write(param.Blob())
		case types.TypeVector:
			// Write vector bytes
			vec := param.Vector()
			if vec != nil {
				for _, f := range vec.Data() {
					buf := make([]byte, 4)
					binary.LittleEndian.PutUint32(buf, uint32(f))
					h.Write(buf)
				}
			}
		}
	}

	return hex.EncodeToString(h.Sum(nil))
}

// Capacity returns the cache capacity
func (qc *QueryCache) Capacity() int {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	return qc.capacity
}

// SetCapacity changes the cache capacity, evicting entries if necessary
func (qc *QueryCache) SetCapacity(capacity int) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	qc.capacity = capacity
	qc.evictIfNeeded()
}

// SetTTL sets the time-to-live for cache entries.
// Entries older than TTL are considered expired.
func (qc *QueryCache) SetTTL(ttl time.Duration) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.ttl = ttl
}

// Put adds or updates a cached result
func (qc *QueryCache) Put(key string, columns []string, rows [][]types.Value, tables []string) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	// Calculate size
	size := qc.estimateSize(columns, rows)

	// Create result
	result := &CachedResult{
		Columns:   columns,
		Rows:      rows,
		Tables:    tables,
		CreatedAt: time.Now(),
		Size:      size,
	}

	// Check if key already exists
	if entry, ok := qc.cache[key]; ok {
		// Update existing entry
		qc.releaseMemory(entry.result.Size)
		entry.result = result
		qc.lru.MoveToFront(entry.element)
		qc.trackMemory(key, size)
		return
	}

	// Add new entry
	elem := qc.lru.PushFront(key)
	qc.cache[key] = &queryCacheEntry{
		key:     key,
		result:  result,
		element: elem,
	}

	// Index by tables
	for _, table := range tables {
		if qc.tableIndex[table] == nil {
			qc.tableIndex[table] = make(map[string]struct{})
		}
		qc.tableIndex[table][key] = struct{}{}
	}

	// Track memory
	qc.trackMemory(key, size)

	// Evict if needed
	qc.evictIfNeeded()
}

// Get retrieves a cached result
func (qc *QueryCache) Get(key string) (*CachedResult, bool) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	entry, ok := qc.cache[key]
	if !ok {
		qc.misses++
		return nil, false
	}

	// Check TTL expiration
	if qc.ttl > 0 && time.Since(entry.result.CreatedAt) > qc.ttl {
		qc.removeEntry(key)
		qc.misses++
		return nil, false
	}

	// Move to front (most recently used)
	qc.lru.MoveToFront(entry.element)
	qc.hits++

	return entry.result, true
}

// InvalidateTable removes all cached results that depend on the specified table
func (qc *QueryCache) InvalidateTable(table string) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	keys, ok := qc.tableIndex[table]
	if !ok {
		return
	}

	// Remove all entries that depend on this table
	for key := range keys {
		qc.removeEntry(key)
	}

	// Clear the table index entry
	delete(qc.tableIndex, table)
}

// InvalidateAll clears the entire cache
func (qc *QueryCache) InvalidateAll() {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	// Release all memory
	for _, entry := range qc.cache {
		qc.releaseMemory(entry.result.Size)
	}

	// Clear everything
	qc.cache = make(map[string]*queryCacheEntry)
	qc.lru = list.New()
	qc.tableIndex = make(map[string]map[string]struct{})
}

// Stats returns cache statistics
func (qc *QueryCache) Stats() QueryCacheStats {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	total := qc.hits + qc.misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(qc.hits) / float64(total)
	}

	return QueryCacheStats{
		Hits:     qc.hits,
		Misses:   qc.misses,
		Entries:  len(qc.cache),
		Capacity: qc.capacity,
		HitRate:  hitRate,
	}
}

// removeEntry removes an entry from the cache (called while holding lock)
func (qc *QueryCache) removeEntry(key string) {
	entry, ok := qc.cache[key]
	if !ok {
		return
	}

	// Release memory
	qc.releaseMemory(entry.result.Size)

	// Remove from table index
	for _, table := range entry.result.Tables {
		if keys, ok := qc.tableIndex[table]; ok {
			delete(keys, key)
			if len(keys) == 0 {
				delete(qc.tableIndex, table)
			}
		}
	}

	// Remove from LRU and cache
	qc.lru.Remove(entry.element)
	delete(qc.cache, key)
}

// evictIfNeeded removes entries until within capacity (called while holding lock)
func (qc *QueryCache) evictIfNeeded() {
	for qc.lru.Len() > qc.capacity {
		elem := qc.lru.Back()
		if elem == nil {
			break
		}

		key := elem.Value.(string)
		qc.removeEntry(key)
	}
}

// estimateSize estimates the memory size of a cached result
func (qc *QueryCache) estimateSize(columns []string, rows [][]types.Value) int64 {
	var size int64

	// Column names
	for _, col := range columns {
		size += int64(len(col))
	}

	// Rows
	for _, row := range rows {
		for _, val := range row {
			switch val.Type() {
			case types.TypeNull:
				size += 8 // Type info
			case types.TypeInt:
				size += 16 // Type + int64
			case types.TypeFloat:
				size += 16 // Type + float64
			case types.TypeText:
				size += int64(8 + len(val.Text()))
			case types.TypeBlob:
				size += int64(8 + len(val.Blob()))
			case types.TypeVector:
				if vec := val.Vector(); vec != nil {
					size += int64(8 + vec.Dimension()*4)
				} else {
					size += 8
				}
			default:
				size += 8
			}
		}
	}

	// Add overhead for slice headers, pointers, etc.
	size += int64(len(rows) * 24) // Row slice overhead
	size += 64                     // Base overhead

	return size
}

// trackMemory tracks memory usage in the budget
func (qc *QueryCache) trackMemory(key string, bytes int64) {
	if qc.memoryBudget == nil {
		return
	}
	qc.memoryBudget.TrackWithPriority("query_cache", key, bytes, PriorityWarm)
}

// releaseMemory releases memory tracking
func (qc *QueryCache) releaseMemory(bytes int64) {
	if qc.memoryBudget == nil {
		return
	}
	qc.memoryBudget.Release("query_cache", bytes)
}
