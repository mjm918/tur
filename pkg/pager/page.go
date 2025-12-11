// pkg/pager/page.go
package pager

import "sync"

// PageType identifies the type of data stored in a page
type PageType byte

const (
	PageTypeUnknown       PageType = 0x00
	PageTypeBTreeInterior PageType = 0x01
	PageTypeBTreeLeaf     PageType = 0x02
	PageTypeHNSWNode      PageType = 0x10
	PageTypeHNSWMeta      PageType = 0x11
	PageTypeOverflow      PageType = 0x20
	PageTypeFreeList      PageType = 0x30
)

// Page represents an in-memory database page
type Page struct {
	mu     sync.RWMutex
	pageNo uint32
	data   []byte
	dirty  bool
	pinned int // reference count
}

// NewPage creates a new page with the given page number and size
func NewPage(pageNo uint32, pageSize int) *Page {
	return &Page{
		pageNo: pageNo,
		data:   make([]byte, pageSize),
	}
}

// NewPageWithData creates a page with existing data (for loading from disk)
func NewPageWithData(pageNo uint32, data []byte) *Page {
	return &Page{
		pageNo: pageNo,
		data:   data,
	}
}

// PageNo returns the page number
func (p *Page) PageNo() uint32 {
	return p.pageNo
}

// Data returns the raw page data (caller should hold appropriate lock)
func (p *Page) Data() []byte {
	return p.data
}

// IsDirty returns whether the page has been modified
func (p *Page) IsDirty() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dirty
}

// SetDirty marks the page as dirty (modified)
func (p *Page) SetDirty(dirty bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dirty = dirty
}

// Type returns the page type (stored in first byte)
func (p *Page) Type() PageType {
	if len(p.data) == 0 {
		return PageTypeUnknown
	}
	return PageType(p.data[0])
}

// SetType sets the page type (stored in first byte)
func (p *Page) SetType(t PageType) {
	if len(p.data) > 0 {
		p.data[0] = byte(t)
	}
}

// Pin increments the reference count
func (p *Page) Pin() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pinned++
}

// Unpin decrements the reference count
func (p *Page) Unpin() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pinned > 0 {
		p.pinned--
	}
}

// IsPinned returns whether the page is currently in use
func (p *Page) IsPinned() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pinned > 0
}
