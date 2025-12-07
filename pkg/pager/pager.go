// pkg/pager/pager.go
package pager

import (
	"encoding/binary"
	"errors"
	"sync"
)

const (
	// Database header constants
	headerSize      = 100
	magicString     = "TurDB format 1\x00"
	defaultPageSize = 4096
)

var (
	ErrInvalidHeader = errors.New("invalid database header")
	ErrPageNotFound  = errors.New("page not found")
)

// Options configures the pager
type Options struct {
	PageSize  int  // Page size in bytes (default 4096)
	CacheSize int  // Number of pages to cache (default 1000)
	ReadOnly  bool // Open in read-only mode
}

// Pager manages database pages and caching
type Pager struct {
	mu        sync.RWMutex
	mmap      *MmapFile
	pageSize  int
	pageCount uint32
	cache     map[uint32]*Page
	cacheSize int
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
		mmap:      mf,
		pageSize:  pageSize,
		cache:     make(map[uint32]*Page),
		cacheSize: cacheSize,
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
	}

	// Update header with new page count
	p.writeHeader()

	// Create page backed by mmap
	offset := int(pageNo) * p.pageSize
	data := p.mmap.Slice(offset, p.pageSize)
	page := NewPageWithData(pageNo, data)
	page.Pin()

	p.cache[pageNo] = page

	return page, nil
}

// Get retrieves a page by number
func (p *Pager) Get(pageNo uint32) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check cache first
	if page, ok := p.cache[pageNo]; ok {
		page.Pin()
		return page, nil
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

	// Add to cache (simple implementation, no eviction yet)
	p.cache[pageNo] = page

	return page, nil
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

	// Write header before closing
	p.writeHeader()

	// Sync and close mmap
	if err := p.mmap.Sync(); err != nil {
		p.mmap.Close()
		return err
	}

	return p.mmap.Close()
}
