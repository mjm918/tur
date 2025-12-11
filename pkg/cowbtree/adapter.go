// pkg/cowbtree/adapter.go
package cowbtree

import (
	"encoding/binary"
	"errors"
	"sync"

	"tur/pkg/pager"
)

// PersistentCowBTree wraps a CowBTree and provides persistence to disk.
// This allows the CoW B+ tree to be used in place of the page-based btree
// while maintaining compatibility with the existing pager-based storage.
//
// The adapter serializes the in-memory tree to pages on checkpoint/close,
// and deserializes on open. During operation, all reads are lock-free
// through the CoW B+ tree.
type PersistentCowBTree struct {
	mu       sync.RWMutex
	tree     *CowBTree
	pager    *pager.Pager
	rootPage uint32
	dirty    bool
}

// Magic bytes for identifying CoW tree pages
var cowTreeMagic = []byte("COWT")

// Page layout:
// [0:4]   - Magic "COWT"
// [4:8]   - Version (uint32)
// [8:16]  - Key count (uint64)
// [16:24] - Node count (uint64)
// [24:32] - Reserved
// [32:]   - Serialized key-value pairs

const (
	cowTreeHeaderSize = 32
	cowTreeVersion    = 1
)

// CreatePersistent creates a new persistent CoW B+ tree
func CreatePersistent(p *pager.Pager) (*PersistentCowBTree, error) {
	page, err := p.Allocate()
	if err != nil {
		return nil, err
	}

	rootPage := page.PageNo()

	// Initialize header
	data := page.Data()
	copy(data[0:4], cowTreeMagic)
	binary.LittleEndian.PutUint32(data[4:8], cowTreeVersion)
	binary.LittleEndian.PutUint64(data[8:16], 0)  // key count
	binary.LittleEndian.PutUint64(data[16:24], 1) // node count
	page.SetDirty(true)
	p.Release(page)

	return &PersistentCowBTree{
		tree:     NewCowBTree(),
		pager:    p,
		rootPage: rootPage,
		dirty:    false,
	}, nil
}

// CreatePersistentAtPage creates a new persistent CoW B+ tree at a specific page
func CreatePersistentAtPage(p *pager.Pager, pageNo uint32) (*PersistentCowBTree, error) {
	// Allocate pages until we reach the desired page
	for p.PageCount() <= pageNo {
		page, err := p.Allocate()
		if err != nil {
			return nil, err
		}
		if page.PageNo() != pageNo {
			p.Release(page)
		}
	}

	page, err := p.Get(pageNo)
	if err != nil {
		return nil, err
	}
	defer p.Release(page)

	// Initialize header
	data := page.Data()
	copy(data[0:4], cowTreeMagic)
	binary.LittleEndian.PutUint32(data[4:8], cowTreeVersion)
	binary.LittleEndian.PutUint64(data[8:16], 0)
	binary.LittleEndian.PutUint64(data[16:24], 1)
	page.SetDirty(true)

	return &PersistentCowBTree{
		tree:     NewCowBTree(),
		pager:    p,
		rootPage: pageNo,
		dirty:    false,
	}, nil
}

// OpenPersistent opens an existing persistent CoW B+ tree
func OpenPersistent(p *pager.Pager, rootPage uint32) (*PersistentCowBTree, error) {
	pt := &PersistentCowBTree{
		tree:     NewCowBTree(),
		pager:    p,
		rootPage: rootPage,
		dirty:    false,
	}

	// Load data from pages
	if err := pt.load(); err != nil {
		return nil, err
	}

	return pt, nil
}

// load reads the tree data from pages into memory
func (pt *PersistentCowBTree) load() error {
	page, err := pt.pager.Get(pt.rootPage)
	if err != nil {
		return err
	}
	defer pt.pager.Release(page)

	data := page.Data()

	// Check magic
	if string(data[0:4]) != string(cowTreeMagic) {
		// Try to load as legacy btree format or empty
		return nil // Empty tree is fine
	}

	// Check version
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != cowTreeVersion {
		return errors.New("unsupported CoW tree version")
	}

	// Read key count
	keyCount := binary.LittleEndian.Uint64(data[8:16])
	if keyCount == 0 {
		return nil
	}

	// Read key-value pairs from subsequent pages if needed
	// For now, we use a simple approach: all data on root page
	offset := cowTreeHeaderSize
	for i := uint64(0); i < keyCount; i++ {
		if offset+8 > len(data) {
			// Need to load from next page - simplified for now
			break
		}

		keyLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		valLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(keyLen)+int(valLen) > len(data) {
			break
		}

		key := make([]byte, keyLen)
		copy(key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)

		value := make([]byte, valLen)
		copy(value, data[offset:offset+int(valLen)])
		offset += int(valLen)

		pt.tree.Insert(key, value)
	}

	return nil
}

// Insert inserts or updates a key-value pair
func (pt *PersistentCowBTree) Insert(key, value []byte) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	err := pt.tree.Insert(key, value)
	if err != nil {
		return err
	}
	pt.dirty = true
	return nil
}

// Get retrieves the value for a key (lock-free read)
func (pt *PersistentCowBTree) Get(key []byte) ([]byte, error) {
	// No lock needed - CoW tree provides lock-free reads
	return pt.tree.Get(key)
}

// Delete removes a key from the tree
func (pt *PersistentCowBTree) Delete(key []byte) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	err := pt.tree.Delete(key)
	if err != nil {
		return err
	}
	pt.dirty = true
	return nil
}

// Cursor creates a new cursor for iteration
func (pt *PersistentCowBTree) Cursor() *Cursor {
	// Lock-free cursor creation
	return pt.tree.Cursor()
}

// RootPage returns the root page number
func (pt *PersistentCowBTree) RootPage() uint32 {
	return pt.rootPage
}

// Checkpoint persists the current tree state to disk
func (pt *PersistentCowBTree) Checkpoint() error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if !pt.dirty {
		return nil
	}

	return pt.save()
}

// save writes the tree data to pages
func (pt *PersistentCowBTree) save() error {
	page, err := pt.pager.Get(pt.rootPage)
	if err != nil {
		return err
	}
	defer pt.pager.Release(page)

	data := page.Data()
	stats := pt.tree.Stats()

	// Write header
	copy(data[0:4], cowTreeMagic)
	binary.LittleEndian.PutUint32(data[4:8], cowTreeVersion)
	binary.LittleEndian.PutUint64(data[8:16], uint64(stats.KeyCount))
	binary.LittleEndian.PutUint64(data[16:24], uint64(stats.NodeCount))

	// Write key-value pairs
	offset := cowTreeHeaderSize
	pt.tree.ForEach(func(key, value []byte) bool {
		needed := 8 + len(key) + len(value)
		if offset+needed > len(data) {
			// TODO: Allocate overflow pages for large trees
			return false
		}

		binary.LittleEndian.PutUint32(data[offset:], uint32(len(key)))
		offset += 4
		binary.LittleEndian.PutUint32(data[offset:], uint32(len(value)))
		offset += 4
		copy(data[offset:], key)
		offset += len(key)
		copy(data[offset:], value)
		offset += len(value)

		return true
	})

	page.SetDirty(true)
	pt.dirty = false
	return nil
}

// Close checkpoints and releases resources
func (pt *PersistentCowBTree) Close() error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.dirty {
		if err := pt.save(); err != nil {
			return err
		}
	}

	return pt.tree.Close()
}

// Depth returns the depth of the tree
func (pt *PersistentCowBTree) Depth() int {
	return int(pt.tree.Stats().Height)
}

// KeyCount returns the number of keys
func (pt *PersistentCowBTree) KeyCount() int64 {
	return pt.tree.Stats().KeyCount
}

// Snapshot creates a read-only snapshot
func (pt *PersistentCowBTree) Snapshot() *CowBTreeSnapshot {
	return pt.tree.Snapshot()
}

// CollectPages returns all page numbers used by this tree
func (pt *PersistentCowBTree) CollectPages() []uint32 {
	// For CoW tree, we only use the root page for persistence
	// The actual tree structure is in memory
	return []uint32{pt.rootPage}
}

// IsDirty returns true if the tree has uncommitted changes
func (pt *PersistentCowBTree) IsDirty() bool {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return pt.dirty
}

// Stats returns statistics about the tree
func (pt *PersistentCowBTree) Stats() CowBTreeStats {
	return pt.tree.Stats()
}

// Range performs a range scan
func (pt *PersistentCowBTree) Range(startKey, endKey []byte, fn func(key, value []byte) bool) error {
	return pt.tree.Range(startKey, endKey, fn)
}
