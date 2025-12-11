// pkg/cowbtree/cursor.go
package cowbtree

import (
	"bytes"
)

// Cursor provides iteration over CoW B+ tree entries.
// This is a lock-free cursor that operates on a snapshot of the tree.
type Cursor struct {
	tree   *CowBTree
	guard  *ReaderGuard // Epoch guard to protect the snapshot
	root   *CowNode     // Root at time of cursor creation
	stack  []*cursorFrame
	valid  bool
	closed bool
}

// cursorFrame represents one level in the cursor's position stack
type cursorFrame struct {
	node *CowNode
	pos  int // Current position in this node
}

// Cursor creates a new cursor for iteration.
// The cursor provides a consistent snapshot of the tree.
func (t *CowBTree) Cursor() *Cursor {
	guard := t.epoch.Enter()
	root := t.getRoot()

	return &Cursor{
		tree:  t,
		guard: guard,
		root:  root,
		stack: make([]*cursorFrame, 0, 8),
		valid: false,
	}
}

// First moves the cursor to the first entry
func (c *Cursor) First() {
	if c.closed {
		return
	}
	c.reset()

	if c.root == nil {
		c.valid = false
		return
	}

	// Navigate to the leftmost leaf
	node := c.root
	for !node.IsLeaf() {
		frame := &cursorFrame{node: node, pos: 0}
		c.stack = append(c.stack, frame)

		child := node.GetChild(0)
		if child == nil {
			c.valid = false
			return
		}
		node = child
	}

	// At leaf
	frame := &cursorFrame{node: node, pos: 0}
	c.stack = append(c.stack, frame)
	c.valid = node.KeyCount() > 0
}

// Last moves the cursor to the last entry
func (c *Cursor) Last() {
	if c.closed {
		return
	}
	c.reset()

	if c.root == nil {
		c.valid = false
		return
	}

	// Navigate to the rightmost leaf
	node := c.root
	for !node.IsLeaf() {
		count := node.KeyCount()
		frame := &cursorFrame{node: node, pos: count}
		c.stack = append(c.stack, frame)

		// Rightmost child is at children[count]
		child := node.GetChild(count)
		if child == nil {
			c.valid = false
			return
		}
		node = child
	}

	// At leaf - position at last entry
	count := node.KeyCount()
	frame := &cursorFrame{node: node, pos: count - 1}
	c.stack = append(c.stack, frame)
	c.valid = count > 0
}

// Seek moves the cursor to the first entry >= key
func (c *Cursor) Seek(key []byte) {
	if c.closed {
		return
	}
	c.reset()

	if c.root == nil {
		c.valid = false
		return
	}

	node := c.root
	for !node.IsLeaf() {
		pos := node.findChildIndex(key)
		frame := &cursorFrame{node: node, pos: pos}
		c.stack = append(c.stack, frame)

		child := node.GetChild(pos)
		if child == nil {
			c.valid = false
			return
		}
		node = child
	}

	// At leaf - binary search for key
	pos := node.findKeyPosition(key)
	frame := &cursorFrame{node: node, pos: pos}
	c.stack = append(c.stack, frame)

	if pos < node.KeyCount() {
		c.valid = true
	} else {
		// Key is greater than all keys in this leaf
		// Try to move to next leaf
		c.valid = false
		c.moveToNextLeaf()
	}
}

// Next moves the cursor to the next entry
func (c *Cursor) Next() {
	if !c.valid || len(c.stack) == 0 || c.closed {
		return
	}

	leaf := c.stack[len(c.stack)-1]
	leaf.pos++

	if leaf.pos < leaf.node.KeyCount() {
		return
	}

	// Need to move to next leaf
	c.moveToNextLeaf()
}

// moveToNextLeaf navigates to the next leaf in order
func (c *Cursor) moveToNextLeaf() {
	// Pop the current leaf
	if len(c.stack) > 0 {
		c.stack = c.stack[:len(c.stack)-1]
	}

	// Walk up until we find an ancestor where we can go right
	for len(c.stack) > 0 {
		parent := c.stack[len(c.stack)-1]
		parent.pos++

		if parent.pos <= parent.node.KeyCount() {
			// Can descend into next child
			child := parent.node.GetChild(parent.pos)
			if child == nil {
				c.valid = false
				return
			}

			// Descend to leftmost leaf
			node := child
			for !node.IsLeaf() {
				frame := &cursorFrame{node: node, pos: 0}
				c.stack = append(c.stack, frame)

				node = node.GetChild(0)
				if node == nil {
					c.valid = false
					return
				}
			}

			frame := &cursorFrame{node: node, pos: 0}
			c.stack = append(c.stack, frame)
			c.valid = node.KeyCount() > 0
			return
		}

		// Pop and continue up
		c.stack = c.stack[:len(c.stack)-1]
	}

	// No more entries
	c.valid = false
}

// Prev moves the cursor to the previous entry
func (c *Cursor) Prev() {
	if !c.valid || len(c.stack) == 0 || c.closed {
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
		c.stack = c.stack[:len(c.stack)-1]
	}

	// Walk up until we find an ancestor where we can go left
	for len(c.stack) > 0 {
		parent := c.stack[len(c.stack)-1]
		parent.pos--

		if parent.pos >= 0 {
			// Can descend into previous child
			child := parent.node.GetChild(parent.pos)
			if child == nil {
				c.valid = false
				return
			}

			// Descend to rightmost leaf
			node := child
			for !node.IsLeaf() {
				count := node.KeyCount()
				frame := &cursorFrame{node: node, pos: count}
				c.stack = append(c.stack, frame)

				node = node.GetChild(count)
				if node == nil {
					c.valid = false
					return
				}
			}

			count := node.KeyCount()
			frame := &cursorFrame{node: node, pos: count - 1}
			c.stack = append(c.stack, frame)
			c.valid = count > 0
			return
		}

		// Pop and continue up
		c.stack = c.stack[:len(c.stack)-1]
	}

	// No more entries
	c.valid = false
}

// Valid returns true if the cursor points to a valid entry
func (c *Cursor) Valid() bool {
	return c.valid && !c.closed
}

// Key returns the current key (only valid if Valid() is true)
func (c *Cursor) Key() []byte {
	if !c.valid || len(c.stack) == 0 || c.closed {
		return nil
	}
	leaf := c.stack[len(c.stack)-1]
	key := leaf.node.GetKey(leaf.pos)
	if key == nil {
		return nil
	}
	// Return a copy
	result := make([]byte, len(key))
	copy(result, key)
	return result
}

// Value returns the current value (only valid if Valid() is true)
func (c *Cursor) Value() []byte {
	if !c.valid || len(c.stack) == 0 || c.closed {
		return nil
	}
	leaf := c.stack[len(c.stack)-1]
	value := leaf.node.GetValue(leaf.pos)
	if value == nil {
		return nil
	}
	// Return a copy
	result := make([]byte, len(value))
	copy(result, value)
	return result
}

// Close releases resources held by the cursor
func (c *Cursor) Close() {
	if c.closed {
		return
	}
	c.closed = true
	c.reset()
	if c.guard != nil {
		c.guard.Leave()
		c.guard = nil
	}
}

// reset clears the cursor state
func (c *Cursor) reset() {
	c.stack = c.stack[:0]
	c.valid = false
}

// SeekExact moves the cursor to the exact key, returning false if not found
func (c *Cursor) SeekExact(key []byte) bool {
	c.Seek(key)
	if !c.valid {
		return false
	}
	return bytes.Equal(c.Key(), key)
}
