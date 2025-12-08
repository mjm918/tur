// pkg/btree/cursor.go
package btree

import (
	"bytes"

	"tur/pkg/pager"
)

// Cursor provides iteration over B-tree entries
type Cursor struct {
	btree *BTree
	// Stack of pages from root to current leaf
	stack []*cursorFrame
	valid bool
}

// cursorFrame represents one level in the cursor's position stack
type cursorFrame struct {
	page *pager.Page
	node *Node
	pos  int // Current cell position in this node
}

// Cursor creates a new cursor for this B-tree
func (bt *BTree) Cursor() *Cursor {
	return &Cursor{
		btree: bt,
		stack: make([]*cursorFrame, 0, 8), // Pre-allocate for typical tree depth
		valid: false,
	}
}

// First moves the cursor to the first entry
func (c *Cursor) First() {
	c.release()

	// Navigate to the leftmost leaf
	pageNo := c.btree.rootPage
	for {
		page, err := c.btree.pager.Get(pageNo)
		if err != nil {
			c.valid = false
			return
		}

		node := LoadNode(page.Data())
		frame := &cursorFrame{page: page, node: node, pos: 0}
		c.stack = append(c.stack, frame)

		if node.IsLeaf() {
			c.valid = node.CellCount() > 0
			return
		}

		// Go to leftmost child (cell 0's child pointer)
		if node.CellCount() > 0 {
			_, childPtr := node.GetCell(0)
			pageNo = decodePageNo(childPtr)
		} else {
			// Empty interior node, go to right child
			pageNo = node.RightChild()
		}
	}
}

// Last moves the cursor to the last entry
func (c *Cursor) Last() {
	c.release()

	// Navigate to the rightmost leaf
	pageNo := c.btree.rootPage
	for {
		page, err := c.btree.pager.Get(pageNo)
		if err != nil {
			c.valid = false
			return
		}

		node := LoadNode(page.Data())
		// For interior nodes, pos = count means we went to rightChild
		// For leaf nodes, pos will be set to the last cell
		frame := &cursorFrame{page: page, node: node, pos: node.CellCount()}
		c.stack = append(c.stack, frame)

		if node.IsLeaf() {
			c.valid = node.CellCount() > 0
			if c.valid {
				frame.pos = node.CellCount() - 1
			}
			return
		}

		// Go to rightmost child
		pageNo = node.RightChild()
	}
}

// Seek moves the cursor to the first entry >= key
func (c *Cursor) Seek(key []byte) {
	c.release()

	pageNo := c.btree.rootPage
	for {
		page, err := c.btree.pager.Get(pageNo)
		if err != nil {
			c.valid = false
			return
		}

		node := LoadNode(page.Data())

		if node.IsLeaf() {
			// Binary search in leaf
			count := node.CellCount()
			lo, hi := 0, count

			for lo < hi {
				mid := (lo + hi) / 2
				midKey, _ := node.GetCell(mid)
				if bytes.Compare(midKey, key) < 0 {
					lo = mid + 1
				} else {
					hi = mid
				}
			}

			frame := &cursorFrame{page: page, node: node, pos: lo}
			c.stack = append(c.stack, frame)

			if lo < count {
				c.valid = true
			} else {
				// Key is greater than all keys in this leaf
				// Try to move to next leaf
				c.valid = false
				c.moveToNextLeaf()
			}
			return
		}

		// Interior node: find child to descend into
		frame := &cursorFrame{page: page, node: node, pos: 0}
		c.stack = append(c.stack, frame)

		count := node.CellCount()
		for i := 0; i < count; i++ {
			cellKey, cellValue := node.GetCell(i)
			if bytes.Compare(key, cellKey) < 0 {
				frame.pos = i
				pageNo = decodePageNo(cellValue)
				goto descend
			}
		}
		// Key is >= all keys, go to right child
		frame.pos = count
		pageNo = node.RightChild()
	descend:
	}
}

// Next moves the cursor to the next entry
func (c *Cursor) Next() {
	if !c.valid || len(c.stack) == 0 {
		return
	}

	leaf := c.stack[len(c.stack)-1]
	leaf.pos++

	if leaf.pos < leaf.node.CellCount() {
		return
	}

	// Need to move to next leaf
	c.moveToNextLeaf()
}

// moveToNextLeaf navigates to the next leaf in order
func (c *Cursor) moveToNextLeaf() {
	// Pop the current leaf
	if len(c.stack) > 0 {
		c.btree.pager.Release(c.stack[len(c.stack)-1].page)
		c.stack = c.stack[:len(c.stack)-1]
	}

	// Walk up until we find an ancestor where we can go right
	for len(c.stack) > 0 {
		parent := c.stack[len(c.stack)-1]
		parent.pos++

		if parent.pos <= parent.node.CellCount() {
			// Can descend into next child
			var pageNo uint32
			if parent.pos < parent.node.CellCount() {
				_, childPtr := parent.node.GetCell(parent.pos)
				pageNo = decodePageNo(childPtr)
			} else {
				// Go to right child
				pageNo = parent.node.RightChild()
			}

			// Descend to leftmost leaf
			for {
				page, err := c.btree.pager.Get(pageNo)
				if err != nil {
					c.valid = false
					return
				}

				node := LoadNode(page.Data())
				frame := &cursorFrame{page: page, node: node, pos: 0}
				c.stack = append(c.stack, frame)

				if node.IsLeaf() {
					c.valid = node.CellCount() > 0
					return
				}

				// Go to leftmost child
				if node.CellCount() > 0 {
					_, childPtr := node.GetCell(0)
					pageNo = decodePageNo(childPtr)
				} else {
					pageNo = node.RightChild()
				}
			}
		}

		// Release this level and continue up
		c.btree.pager.Release(parent.page)
		c.stack = c.stack[:len(c.stack)-1]
	}

	// No more entries
	c.valid = false
}

// Prev moves the cursor to the previous entry
func (c *Cursor) Prev() {
	if !c.valid || len(c.stack) == 0 {
		return
	}

	leaf := c.stack[len(c.stack)-1]
	leaf.pos--

	if leaf.pos >= 0 {
		return
	}

	// Need to move to previous leaf
	c.moveToPrevLeaf()
}

// moveToPrevLeaf navigates to the previous leaf in order
func (c *Cursor) moveToPrevLeaf() {
	// Pop the current leaf
	if len(c.stack) > 0 {
		c.btree.pager.Release(c.stack[len(c.stack)-1].page)
		c.stack = c.stack[:len(c.stack)-1]
	}

	// Walk up until we find an ancestor where we can go left
	for len(c.stack) > 0 {
		parent := c.stack[len(c.stack)-1]
		parent.pos--

		if parent.pos >= 0 {
			// Can descend into previous child
			_, childPtr := parent.node.GetCell(parent.pos)
			pageNo := decodePageNo(childPtr)

			// Descend to rightmost leaf
			for {
				page, err := c.btree.pager.Get(pageNo)
				if err != nil {
					c.valid = false
					return
				}

				node := LoadNode(page.Data())
				frame := &cursorFrame{page: page, node: node, pos: node.CellCount()}
				c.stack = append(c.stack, frame)

				if node.IsLeaf() {
					frame.pos = node.CellCount() - 1
					c.valid = frame.pos >= 0
					return
				}

				// Go to rightmost child
				pageNo = node.RightChild()
			}
		}

		// Release this level and continue up
		c.btree.pager.Release(parent.page)
		c.stack = c.stack[:len(c.stack)-1]
	}

	// No more entries
	c.valid = false
}

// Valid returns true if the cursor points to a valid entry
func (c *Cursor) Valid() bool {
	return c.valid
}

// Key returns the current key (only valid if Valid() is true)
func (c *Cursor) Key() []byte {
	if !c.valid || len(c.stack) == 0 {
		return nil
	}
	leaf := c.stack[len(c.stack)-1]
	key, _ := leaf.node.GetCell(leaf.pos)
	// Return a copy
	result := make([]byte, len(key))
	copy(result, key)
	return result
}

// Value returns the current value (only valid if Valid() is true)
func (c *Cursor) Value() []byte {
	if !c.valid || len(c.stack) == 0 {
		return nil
	}
	leaf := c.stack[len(c.stack)-1]
	_, value := leaf.node.GetCell(leaf.pos)
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
	for _, frame := range c.stack {
		if frame.page != nil {
			c.btree.pager.Release(frame.page)
		}
	}
	c.stack = c.stack[:0]
	c.valid = false
}
