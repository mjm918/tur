// pkg/btree/cursor.go
package btree

import (
	"bytes"

	"tur/pkg/pager"
)

// Cursor provides iteration over B-tree entries
type Cursor struct {
	btree *BTree
	page  *pager.Page
	node  *Node
	pos   int
	valid bool
}

// Cursor creates a new cursor for this B-tree
func (bt *BTree) Cursor() *Cursor {
	return &Cursor{
		btree: bt,
		valid: false,
	}
}

// First moves the cursor to the first entry
func (c *Cursor) First() {
	c.release()

	page, err := c.btree.pager.Get(c.btree.rootPage)
	if err != nil {
		c.valid = false
		return
	}

	c.page = page
	c.node = LoadNode(page.Data())
	c.pos = 0
	c.valid = c.node.CellCount() > 0
}

// Last moves the cursor to the last entry
func (c *Cursor) Last() {
	c.release()

	page, err := c.btree.pager.Get(c.btree.rootPage)
	if err != nil {
		c.valid = false
		return
	}

	c.page = page
	c.node = LoadNode(page.Data())
	count := c.node.CellCount()
	if count > 0 {
		c.pos = count - 1
		c.valid = true
	} else {
		c.valid = false
	}
}

// Seek moves the cursor to the first entry >= key
func (c *Cursor) Seek(key []byte) {
	c.release()

	page, err := c.btree.pager.Get(c.btree.rootPage)
	if err != nil {
		c.valid = false
		return
	}

	c.page = page
	c.node = LoadNode(page.Data())

	// Binary search
	count := c.node.CellCount()
	lo, hi := 0, count

	for lo < hi {
		mid := (lo + hi) / 2
		midKey, _ := c.node.GetCell(mid)
		if bytes.Compare(midKey, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	c.pos = lo
	c.valid = lo < count
}

// Next moves the cursor to the next entry
func (c *Cursor) Next() {
	if !c.valid {
		return
	}

	c.pos++
	if c.pos >= c.node.CellCount() {
		c.valid = false
	}
}

// Prev moves the cursor to the previous entry
func (c *Cursor) Prev() {
	if !c.valid {
		return
	}

	c.pos--
	if c.pos < 0 {
		c.valid = false
	}
}

// Valid returns true if the cursor points to a valid entry
func (c *Cursor) Valid() bool {
	return c.valid
}

// Key returns the current key (only valid if Valid() is true)
func (c *Cursor) Key() []byte {
	if !c.valid {
		return nil
	}
	key, _ := c.node.GetCell(c.pos)
	// Return a copy
	result := make([]byte, len(key))
	copy(result, key)
	return result
}

// Value returns the current value (only valid if Valid() is true)
func (c *Cursor) Value() []byte {
	if !c.valid {
		return nil
	}
	_, value := c.node.GetCell(c.pos)
	// Return a copy
	result := make([]byte, len(value))
	copy(result, value)
	return result
}

// Close releases resources held by the cursor
func (c *Cursor) Close() {
	c.release()
}

func (c *Cursor) release() {
	if c.page != nil {
		c.btree.pager.Release(c.page)
		c.page = nil
		c.node = nil
	}
}
