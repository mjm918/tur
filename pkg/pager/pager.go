// pkg/pager/pager.go
package pager

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"tur/pkg/cache"
	"tur/pkg/wal"
)

const (
	// Database header constants
	headerSize      = 100
	magicString     = "TurDB format 1\x00"
	defaultPageSize = 4096
)

var (
	ErrInvalidHeader   = errors.New("invalid database header")
	ErrPageNotFound    = errors.New("page not found")
	ErrNoTransaction   = errors.New("no active transaction")
	ErrTxAlreadyActive = errors.New("transaction already active")
)

// Options configures the pager
type Options struct {
	PageSize  int  // Page size in bytes (default 4096)
	CacheSize int  // Number of pages to cache (default 1000)
	ReadOnly  bool // Open in read-only mode
}

// cacheEntry holds a page and its LRU list element
type cacheEntry struct {
	page    *Page
	element *list.Element
}

// Pager manages database pages and caching
type Pager struct {
	mu        sync.RWMutex
	mmap      *MmapFile
	path      string // Database file path
	pageSize  int
	pageCount uint32
	cache     map[uint32]*cacheEntry
	lru       *list.List // LRU list (front = most recent)
	cacheSize int

	// WAL support
	wal           *wal.WAL
	inTransaction bool
	dirtyPages    map[uint32][]byte // Page number -> original data (for rollback)

	// Freelist support
	freelist *Freelist

	// Memory budget tracking
	memoryBudget *cache.MemoryBudget
}

// Transaction represents an active write transaction
type Transaction struct {
	pager *Pager
}

// Open opens or creates a database file
func Open(path string, opts Options) (*Pager, error) {
	return OpenWithBudget(path, opts, nil)
}

// OpenWithBudget opens or creates a database file with memory budget tracking
func OpenWithBudget(path string, opts Options, budget *cache.MemoryBudget) (*Pager, error) {
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	cacheSize := opts.CacheSize
	if cacheSize == 0 {
		cacheSize = 1000
	}

	// Try to open existing file first
	mf, err := OpenMmapFile(path, int64(pageSize))
	if err != nil {
		return nil, err
	}

	p := &Pager{
		mmap:         mf,
		path:         path,
		pageSize:     pageSize,
		cache:        make(map[uint32]*cacheEntry),
		lru:          list.New(),
		cacheSize:    cacheSize,
		dirtyPages:   make(map[uint32][]byte),
		freelist:     NewFreelist(pageSize),
		memoryBudget: budget,
	}

	// Register with memory budget if provided
	if budget != nil {
		budget.RegisterComponent("page_cache")
	}

	// Check if this is a new file or existing database
	header := mf.Slice(0, headerSize)
	if string(header[0:len(magicString)]) == magicString {
		// Existing database - read header
		p.pageSize = int(binary.LittleEndian.Uint32(header[16:20]))
		p.pageCount = binary.LittleEndian.Uint32(header[20:24])

		// Load freelist from header
		freelistHead := GetFreelistHead(header)
		freePageCount := GetFreePageCount(header)
		p.loadFreelist(freelistHead, freePageCount)
	} else {
		// New database - initialize header
		p.pageCount = 1 // Header page is page 0
		p.writeHeader()
	}

	// Open or create WAL file
	walPath := path + "-wal"
	w, err := wal.Open(walPath, wal.Options{PageSize: pageSize})
	if err != nil {
		mf.Close()
		return nil, err
	}

	// If WAL has frames, recover them
	if w.FrameCount() > 0 {
		_, err = w.Recover(path)
		if err != nil {
			w.Close()
			mf.Close()
			return nil, err
		}
	}

	p.wal = w

	return p, nil
}

// writeHeader writes the database header to page 0
func (p *Pager) writeHeader() {
	header := p.mmap.Slice(0, headerSize)
	copy(header[0:16], magicString)
	binary.LittleEndian.PutUint32(header[16:20], uint32(p.pageSize))
	binary.LittleEndian.PutUint32(header[20:24], p.pageCount)

	// Write freelist info to header
	if p.freelist != nil {
		PutFreelistHead(header, p.freelist.HeadPage())
		PutFreePageCount(header, p.freelist.FreeCount())
	}
}

// PageSize returns the page size
func (p *Pager) PageSize() int {
	return p.pageSize
}

// PageCount returns the number of pages
func (p *Pager) PageCount() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pageCount
}

// Allocate creates a new page
func (p *Pager) Allocate() (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var pageNo uint32

	// Try to allocate from freelist first
	if p.freelist != nil && p.freelist.FreeCount() > 0 {
		if freedPage, ok := p.allocateFromFreelistPersistent(); ok {
			pageNo = freedPage
			// Page already exists in file, just need to get it
			return p.getPageLocked(pageNo)
		}
	}

	// Freelist empty - grow the file
	pageNo = p.pageCount
	p.pageCount++

	// Ensure file is large enough
	requiredSize := int64(p.pageCount) * int64(p.pageSize)
	if requiredSize > p.mmap.Size() {
		// Grow by at least 10% or to required size
		newSize := p.mmap.Size() + p.mmap.Size()/10
		if newSize < requiredSize {
			newSize = requiredSize
		}
		if err := p.mmap.Grow(newSize); err != nil {
			return nil, err
		}
		// After remap, all cached page data slices are invalid
		// Clear the cache to force re-fetching from new mmap
		p.invalidateCache()
	}

	// Update header with new page count
	p.writeHeader()

	// Create page backed by mmap
	offset := int(pageNo) * p.pageSize
	data := p.mmap.Slice(offset, p.pageSize)
	page := NewPageWithData(pageNo, data)
	page.Pin()

	// Clear the page data (newly allocated pages should be zeroed)
	for i := range data {
		data[i] = 0
	}

	// Add to cache with LRU tracking
	elem := p.lru.PushFront(pageNo)
	p.cache[pageNo] = &cacheEntry{page: page, element: elem}

	// Track memory usage
	p.trackCacheMemory(pageNo, int64(p.pageSize))

	// Evict if needed
	p.evictIfNeeded()

	return page, nil
}

// allocateFromFreelistPersistent allocates a page from the freelist and updates disk.
// Returns leaf pages first (LIFO), then trunk pages when empty.
func (p *Pager) allocateFromFreelistPersistent() (uint32, bool) {
	if len(p.freelist.trunks) == 0 {
		return 0, false
	}

	trunk := p.freelist.trunks[0]
	currentHead := p.freelist.headPage

	// Try to pop a leaf page first
	if leafPage, ok := trunk.PopLeaf(); ok {
		p.freelist.freeCount--

		// Update trunk on disk
		offset := int(currentHead) * p.pageSize
		data := p.mmap.Slice(offset, p.pageSize)
		trunk.Encode(data)

		// Update header
		p.writeHeader()

		return leafPage, true
	}

	// No more leaves - return the trunk page itself
	// Move to next trunk
	nextTrunk := trunk.NextTrunk
	p.freelist.freeCount--

	if nextTrunk != 0 && len(p.freelist.trunks) > 1 {
		// Move to next trunk
		p.freelist.trunks = p.freelist.trunks[1:]
		p.freelist.headPage = nextTrunk
	} else if nextTrunk != 0 {
		// Load next trunk from disk
		offset := int(nextTrunk) * p.pageSize
		data := p.mmap.Slice(offset, p.pageSize)
		loadedTrunk := DecodeFreelistTrunkPage(data)
		p.freelist.trunks = []*FreelistTrunkPage{loadedTrunk}
		p.freelist.headPage = nextTrunk
	} else {
		// No more trunks - freelist is empty
		p.freelist.trunks = nil
		p.freelist.headPage = 0
	}

	// Update header
	p.writeHeader()

	return currentHead, true
}

// Get retrieves a page by number
func (p *Pager) Get(pageNo uint32) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check cache first
	if entry, ok := p.cache[pageNo]; ok {
		entry.page.Pin()
		// Move to front of LRU
		p.lru.MoveToFront(entry.element)
		// Record access for priority tracking
		p.recordCacheAccess(pageNo)
		return entry.page, nil
	}

	// Check bounds
	if pageNo >= p.pageCount {
		return nil, ErrPageNotFound
	}

	// Load from mmap
	offset := int(pageNo) * p.pageSize
	data := p.mmap.Slice(offset, p.pageSize)
	if data == nil {
		return nil, ErrPageNotFound
	}

	page := NewPageWithData(pageNo, data)
	page.Pin()

	// Add to cache with LRU tracking
	elem := p.lru.PushFront(pageNo)
	p.cache[pageNo] = &cacheEntry{page: page, element: elem}

	// Track memory usage
	p.trackCacheMemory(pageNo, int64(p.pageSize))

	// Evict if needed
	p.evictIfNeeded()

	return page, nil
}

// invalidateCache clears all cached pages after mmap regrowth
// This is necessary because the underlying memory region changes after remap
func (p *Pager) invalidateCache() {
	// Release memory for all cached pages
	if p.memoryBudget != nil {
		for pageNo := range p.cache {
			p.releaseCacheMemory(pageNo)
		}
	}

	// Clear LRU list
	p.lru = list.New()
	// Clear cache map
	p.cache = make(map[uint32]*cacheEntry)
}

// evictIfNeeded removes unpinned pages from cache if over capacity
func (p *Pager) evictIfNeeded() {
	// Check both LRU cache size and memory budget pressure
	for p.lru.Len() > p.cacheSize || p.shouldEvictForMemory() {
		// Get least recently used (back of list)
		elem := p.lru.Back()
		if elem == nil {
			break
		}

		pageNo := elem.Value.(uint32)
		entry := p.cache[pageNo]
		if entry == nil {
			p.lru.Remove(elem)
			continue
		}

		// Don't evict pinned pages
		if entry.page.IsPinned() {
			// Move to front so we try other pages
			p.lru.MoveToFront(elem)
			break // All remaining pages are likely pinned
		}

		// Release memory tracking
		p.releaseCacheMemory(pageNo)

		// Remove from cache and LRU
		p.lru.Remove(elem)
		delete(p.cache, pageNo)
	}
}

// shouldEvictForMemory returns true if memory budget is exceeded
func (p *Pager) shouldEvictForMemory() bool {
	if p.memoryBudget == nil {
		return false
	}
	return p.memoryBudget.IsExceeded()
}

// Release unpins a page
func (p *Pager) Release(page *Page) {
	page.Unpin()
}

// Sync flushes all changes to disk
func (p *Pager) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.writeHeader()
	return p.mmap.Sync()
}

// Close closes the pager
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close WAL
	if p.wal != nil {
		p.wal.Close()
	}

	// Write header before closing
	p.writeHeader()

	// Sync and close mmap
	if err := p.mmap.Sync(); err != nil {
		p.mmap.Close()
		return err
	}

	return p.mmap.Close()
}

// BeginWrite starts a write transaction
func (p *Pager) BeginWrite() (*Transaction, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.inTransaction {
		return nil, ErrTxAlreadyActive
	}

	p.inTransaction = true
	p.dirtyPages = make(map[uint32][]byte)

	return &Transaction{pager: p}, nil
}

// InTransaction returns true if a transaction is active
func (p *Pager) InTransaction() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.inTransaction
}

// Commit commits the transaction, writing dirty pages to WAL
func (tx *Transaction) Commit() error {
	p := tx.pager
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.inTransaction {
		return ErrNoTransaction
	}

	// Write all dirty pages to WAL
	dirtyCount := 0
	for pageNo := range p.dirtyPages {
		entry, ok := p.cache[pageNo]
		if !ok {
			continue
		}

		if entry.page.IsDirty() {
			dirtyCount++
			isCommit := dirtyCount == len(p.dirtyPages) // Last page is commit
			if err := p.wal.WriteFrame(pageNo, entry.page.Data(), isCommit); err != nil {
				return err
			}
			entry.page.SetDirty(false)
		}
	}

	// If no dirty pages but transaction was started, write a sync point
	if dirtyCount == 0 {
		// Nothing to do
	}

	// Clear transaction state
	p.inTransaction = false
	p.dirtyPages = make(map[uint32][]byte)

	return nil
}

// Rollback aborts the transaction, restoring original page data
func (tx *Transaction) Rollback() {
	p := tx.pager
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.inTransaction {
		return
	}

	// Restore original page data
	for pageNo, originalData := range p.dirtyPages {
		entry, ok := p.cache[pageNo]
		if !ok {
			continue
		}

		// Restore the original data
		copy(entry.page.Data(), originalData)
		entry.page.SetDirty(false)
	}

	// Clear transaction state
	p.inTransaction = false
	p.dirtyPages = make(map[uint32][]byte)
}

// MarkDirty records that a page has been modified in the current transaction
func (p *Pager) MarkDirty(page *Page) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.inTransaction {
		return
	}

	pageNo := page.PageNo()
	if _, exists := p.dirtyPages[pageNo]; !exists {
		// Save original data for potential rollback
		original := make([]byte, p.pageSize)
		copy(original, page.Data())
		p.dirtyPages[pageNo] = original
	}
}

// Free returns a page to the freelist for reuse
func (p *Pager) Free(pageNo uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Cannot free page 0 (header page)
	if pageNo == 0 {
		return errors.New("cannot free page 0 (header page)")
	}

	// Cannot free page beyond current page count
	if pageNo >= p.pageCount {
		return ErrPageNotFound
	}

	// Remove from cache if present
	if entry, ok := p.cache[pageNo]; ok {
		p.lru.Remove(entry.element)
		delete(p.cache, pageNo)
	}

	// Add to freelist and persist
	p.addToFreelistPersistent(pageNo)

	// Update header with new freelist info
	p.writeHeader()

	return nil
}

// addToFreelistPersistent adds a page to the freelist and persists to disk.
// We use a simpler approach: the first freed page becomes a trunk, and
// subsequent freed pages are added as leaf entries in that trunk.
// When the trunk is full, we allocate a new trunk from the freelist itself.
func (p *Pager) addToFreelistPersistent(pageNo uint32) {
	// Get current head trunk
	currentHead := p.freelist.HeadPage()

	if currentHead == 0 {
		// No existing trunk - this page becomes the first trunk
		// A trunk with no leaves still counts as 1 free page (the trunk itself)
		trunk := &FreelistTrunkPage{
			NextTrunk: 0,
			LeafPages: []uint32{},
		}
		// Write trunk to the freed page
		offset := int(pageNo) * p.pageSize
		data := p.mmap.Slice(offset, p.pageSize)
		trunk.Encode(data)

		// Update in-memory freelist - the trunk page itself is a free page
		p.freelist.trunks = []*FreelistTrunkPage{trunk}
		p.freelist.headPage = pageNo
		p.freelist.freeCount = 1
		return
	}

	// We have an existing trunk - add this page as a leaf
	if len(p.freelist.trunks) > 0 {
		trunk := p.freelist.trunks[0]
		if !trunk.IsFull(p.pageSize) {
			// Add as leaf page to current trunk
			trunk.AddLeaf(pageNo)
			p.freelist.freeCount++

			// Write updated trunk to disk
			offset := int(currentHead) * p.pageSize
			data := p.mmap.Slice(offset, p.pageSize)
			trunk.Encode(data)
			return
		}

		// Current trunk is full of leaves
		// The new page becomes a new trunk, and the old trunk becomes a leaf of the new trunk
		// But this is complex - for simplicity, just make the new page a new trunk pointing to old
		newTrunk := &FreelistTrunkPage{
			NextTrunk: currentHead,
			LeafPages: []uint32{},
		}

		// Write new trunk to the freed page
		offset := int(pageNo) * p.pageSize
		data := p.mmap.Slice(offset, p.pageSize)
		newTrunk.Encode(data)

		// Update in-memory freelist
		p.freelist.trunks = append([]*FreelistTrunkPage{newTrunk}, p.freelist.trunks...)
		p.freelist.headPage = pageNo
		p.freelist.freeCount++
	}
}

// FreePageCount returns the number of free pages in the freelist
func (p *Pager) FreePageCount() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.freelist == nil {
		return 0
	}
	return p.freelist.FreeCount()
}

// getPageLocked retrieves a page while already holding the lock.
// Used internally by Allocate when reusing a freed page.
func (p *Pager) getPageLocked(pageNo uint32) (*Page, error) {
	// Check cache first
	if entry, ok := p.cache[pageNo]; ok {
		entry.page.Pin()
		// Move to front of LRU
		p.lru.MoveToFront(entry.element)
		return entry.page, nil
	}

	// Check bounds
	if pageNo >= p.pageCount {
		return nil, ErrPageNotFound
	}

	// Load from mmap
	offset := int(pageNo) * p.pageSize
	data := p.mmap.Slice(offset, p.pageSize)
	if data == nil {
		return nil, ErrPageNotFound
	}

	page := NewPageWithData(pageNo, data)
	page.Pin()

	// Clear the page data (reused pages should be zeroed)
	for i := range data {
		data[i] = 0
	}

	// Add to cache with LRU tracking
	elem := p.lru.PushFront(pageNo)
	p.cache[pageNo] = &cacheEntry{page: page, element: elem}

	// Evict if needed
	p.evictIfNeeded()

	return page, nil
}

// loadFreelist loads the freelist from disk on database open
func (p *Pager) loadFreelist(headPage uint32, freeCount uint32) {
	if headPage == 0 || freeCount == 0 {
		// No freelist to load
		return
	}

	// Load trunk pages directly into freelist structure
	p.freelist.trunks = nil
	p.freelist.headPage = headPage
	p.freelist.freeCount = freeCount

	// Walk the trunk page chain and load all trunks
	currentTrunkPage := headPage

	for currentTrunkPage != 0 {
		// Read trunk page data from mmap
		offset := int(currentTrunkPage) * p.pageSize
		data := p.mmap.Slice(offset, p.pageSize)
		if data == nil {
			break
		}

		// Decode the trunk page
		trunk := DecodeFreelistTrunkPage(data)
		p.freelist.trunks = append(p.freelist.trunks, trunk)

		// Move to next trunk
		currentTrunkPage = trunk.NextTrunk
	}
}

// MemoryBudget returns the memory budget associated with this pager, if any
func (p *Pager) MemoryBudget() *cache.MemoryBudget {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.memoryBudget
}

// trackCacheMemory tracks memory usage for a cached page
func (p *Pager) trackCacheMemory(pageNo uint32, bytes int64) {
	if p.memoryBudget == nil {
		return
	}

	key := fmt.Sprintf("page_%d", pageNo)
	p.memoryBudget.TrackWithPriority("page_cache", key, bytes, cache.PriorityWarm)
}

// releaseCacheMemory releases memory tracking for a cached page
func (p *Pager) releaseCacheMemory(pageNo uint32) {
	if p.memoryBudget == nil {
		return
	}

	key := fmt.Sprintf("page_%d", pageNo)
	p.memoryBudget.ReleaseItem("page_cache", key)
}

// recordCacheAccess records access to a cached page for priority tracking
func (p *Pager) recordCacheAccess(pageNo uint32) {
	if p.memoryBudget == nil {
		return
	}

	key := fmt.Sprintf("page_%d", pageNo)
	p.memoryBudget.RecordAccess("page_cache", key)
}
