// pkg/btree/btree.go
package btree

import (
	"bytes"
	"errors"

	"tur/pkg/pager"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

// BTree represents a B-tree index
type BTree struct {
	pager    *pager.Pager
	rootPage uint32
}

// Create creates a new B-tree, allocating a root page
func Create(p *pager.Pager) (*BTree, error) {
	// Allocate root page
	page, err := p.Allocate()
	if err != nil {
		return nil, err
	}

	// Initialize as empty leaf node
	NewNode(page.Data(), true)
	page.SetDirty(true)
	page.SetType(pager.PageTypeBTreeLeaf)

	rootPage := page.PageNo()
	p.Release(page)

	return &BTree{
		pager:    p,
		rootPage: rootPage,
	}, nil
}

// Open opens an existing B-tree with the given root page
func Open(p *pager.Pager, rootPage uint32) *BTree {
	return &BTree{
		pager:    p,
		rootPage: rootPage,
	}
}

// RootPage returns the root page number
func (bt *BTree) RootPage() uint32 {
	return bt.rootPage
}

// Insert inserts or updates a key-value pair
func (bt *BTree) Insert(key, value []byte) error {
	page, err := bt.pager.Get(bt.rootPage)
	if err != nil {
		return err
	}
	defer bt.pager.Release(page)

	node := LoadNode(page.Data())

	// Find insertion position (sorted order)
	pos := bt.findPosition(node, key)

	// Check if key already exists at this position
	if pos < node.CellCount() {
		existingKey, _ := node.GetCell(pos)
		if bytes.Equal(existingKey, key) {
			// Update: for now, we'll do a simple delete + insert
			// A more sophisticated implementation would update in place
			bt.deleteAt(node, pos)
			pos = bt.findPosition(node, key)
		}
	}

	// Insert at position
	err = node.InsertCell(pos, key, value)
	if err == ErrNodeFull {
		// TODO: implement page splitting
		return errors.New("page split not implemented yet")
	}
	if err != nil {
		return err
	}

	page.SetDirty(true)
	return nil
}

// Get retrieves the value for a key
func (bt *BTree) Get(key []byte) ([]byte, error) {
	page, err := bt.pager.Get(bt.rootPage)
	if err != nil {
		return nil, err
	}
	defer bt.pager.Release(page)

	node := LoadNode(page.Data())

	// Binary search for key
	pos := bt.findPosition(node, key)

	if pos < node.CellCount() {
		foundKey, value := node.GetCell(pos)
		if bytes.Equal(foundKey, key) {
			// Return a copy to avoid issues with mmap
			result := make([]byte, len(value))
			copy(result, value)
			return result, nil
		}
	}

	return nil, ErrKeyNotFound
}

// findPosition returns the position where key should be inserted (binary search)
func (bt *BTree) findPosition(node *Node, key []byte) int {
	count := node.CellCount()
	lo, hi := 0, count

	for lo < hi {
		mid := (lo + hi) / 2
		midKey, _ := node.GetCell(mid)
		cmp := bytes.Compare(midKey, key)
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// deleteAt removes the cell at position i
func (bt *BTree) deleteAt(node *Node, i int) {
	count := node.CellCount()
	if i < 0 || i >= count {
		return
	}

	// Shift cell pointers
	for j := i; j < count-1; j++ {
		node.setCellOffset(j, node.getCellOffset(j+1))
	}

	node.setCellCount(count - 1)
	// Note: we don't reclaim the cell content space in this simple implementation
	// A full implementation would track fragmentation and defragment when needed
}
