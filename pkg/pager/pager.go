// pkg/pager/pager.go
package pager

import (
	"container/list"
	"encoding/binary"
	"errors"
	"sync"

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
}

// Transaction represents an active write transaction
type Transaction struct {
	pager *Pager
}

// Open opens or creates a database file
func Open(path string, opts Options) (*Pager, error) {
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
		mmap:       mf,
		path:       path,
		pageSize:   pageSize,
		cache:      make(map[uint32]*cacheEntry),
		lru:        list.New(),
		cacheSize:  cacheSize,
		dirtyPages: make(map[uint32][]byte),
	}

	// Check if this is a new file or existing database
	header := mf.Slice(0, headerSize)
	if string(header[0:len(magicString)]) == magicString {
		// Existing database - read header
		p.pageSize = int(binary.LittleEndian.Uint32(header[16:20]))
		p.pageCount = binary.LittleEndian.Uint32(header[20:24])
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

	pageNo := p.pageCount
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

	// Add to cache with LRU tracking
	elem := p.lru.PushFront(pageNo)
	p.cache[pageNo] = &cacheEntry{page: page, element: elem}

	// Evict if needed
	p.evictIfNeeded()

	return page, nil
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

	// Evict if needed
	p.evictIfNeeded()

	return page, nil
}

// invalidateCache clears all cached pages after mmap regrowth
// This is necessary because the underlying memory region changes after remap
func (p *Pager) invalidateCache() {
	// Clear LRU list
	p.lru = list.New()
	// Clear cache map
	p.cache = make(map[uint32]*cacheEntry)
}

// evictIfNeeded removes unpinned pages from cache if over capacity
func (p *Pager) evictIfNeeded() {
	for p.lru.Len() > p.cacheSize {
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

		// Remove from cache and LRU
		p.lru.Remove(elem)
		delete(p.cache, pageNo)
	}
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
