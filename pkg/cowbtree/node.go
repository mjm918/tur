// pkg/cowbtree/node.go
package cowbtree

import (
	"bytes"
	"sync/atomic"
	"unsafe"
)

// CowNode represents an immutable B+ tree node in the CoW (Copy-on-Write) tree.
// Once created, nodes are never modified - all changes create new node copies.
// This enables lock-free reads since readers always see a consistent snapshot.
type CowNode struct {
	// keys holds the separator keys for this node
	// For leaf nodes: actual keys stored in the tree
	// For interior nodes: separator keys guiding search
	keys [][]byte

	// values holds the values (leaf nodes only)
	// For interior nodes, this is nil
	values [][]byte

	// children holds child node pointers (interior nodes only)
	// For leaf nodes, this is nil
	// Children are stored as unsafe.Pointer for atomic access
	children []unsafe.Pointer // *CowNode stored as unsafe.Pointer

	// isLeaf indicates whether this is a leaf node
	isLeaf bool

	// next points to the next leaf node (leaf nodes only)
	// Used for efficient range scans
	// Stored as unsafe.Pointer for atomic access
	next unsafe.Pointer // *CowNode

	// prev points to the previous leaf node (leaf nodes only)
	// Used for reverse iteration
	prev unsafe.Pointer // *CowNode

	// version is incremented on each modification (for debugging/testing)
	version uint64
}

// NodeConfig holds configuration for node creation
type NodeConfig struct {
	MaxKeys int // Maximum number of keys per node (branching factor - 1)
}

// DefaultNodeConfig returns the default node configuration
// Using a branching factor optimized for in-memory operations
func DefaultNodeConfig() NodeConfig {
	return NodeConfig{
		MaxKeys: 64, // 64 keys = 65 children for interior nodes
	}
}

// NewLeafNode creates a new empty leaf node
func NewLeafNode() *CowNode {
	return &CowNode{
		keys:    make([][]byte, 0),
		values:  make([][]byte, 0),
		isLeaf:  true,
		version: 1,
	}
}

// NewInteriorNode creates a new empty interior node
func NewInteriorNode() *CowNode {
	return &CowNode{
		keys:     make([][]byte, 0),
		children: make([]unsafe.Pointer, 0),
		isLeaf:   false,
		version:  1,
	}
}

// IsLeaf returns true if this is a leaf node
func (n *CowNode) IsLeaf() bool {
	return n.isLeaf
}

// KeyCount returns the number of keys in this node
func (n *CowNode) KeyCount() int {
	return len(n.keys)
}

// GetKey returns the key at position i
func (n *CowNode) GetKey(i int) []byte {
	if i < 0 || i >= len(n.keys) {
		return nil
	}
	return n.keys[i]
}

// GetValue returns the value at position i (leaf nodes only)
func (n *CowNode) GetValue(i int) []byte {
	if !n.isLeaf || i < 0 || i >= len(n.values) {
		return nil
	}
	return n.values[i]
}

// GetChild returns the child at position i (interior nodes only)
func (n *CowNode) GetChild(i int) *CowNode {
	if n.isLeaf || i < 0 || i >= len(n.children) {
		return nil
	}
	ptr := atomic.LoadPointer(&n.children[i])
	if ptr == nil {
		return nil
	}
	return (*CowNode)(ptr)
}

// GetNextLeaf returns the next leaf node in the chain
func (n *CowNode) GetNextLeaf() *CowNode {
	if !n.isLeaf {
		return nil
	}
	ptr := atomic.LoadPointer(&n.next)
	if ptr == nil {
		return nil
	}
	return (*CowNode)(ptr)
}

// GetPrevLeaf returns the previous leaf node in the chain
func (n *CowNode) GetPrevLeaf() *CowNode {
	if !n.isLeaf {
		return nil
	}
	ptr := atomic.LoadPointer(&n.prev)
	if ptr == nil {
		return nil
	}
	return (*CowNode)(ptr)
}

// findKeyPosition finds the position where key should be inserted (binary search)
// Returns the index where key would be inserted to maintain sorted order
func (n *CowNode) findKeyPosition(key []byte) int {
	lo, hi := 0, len(n.keys)
	for lo < hi {
		mid := (lo + hi) / 2
		cmp := bytes.Compare(n.keys[mid], key)
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// findChildIndex finds which child to descend into for the given key
// For interior nodes: returns the index of the child subtree containing key
// In B+ trees:
//   - children[i] contains keys < keys[i]
//   - children[i+1] contains keys >= keys[i]
// So for a key that equals keys[i], we go to children[i+1]
func (n *CowNode) findChildIndex(key []byte) int {
	// Binary search to find the first key > search key
	// Then descend into the child to the left of that key
	lo, hi := 0, len(n.keys)
	for lo < hi {
		mid := (lo + hi) / 2
		cmp := bytes.Compare(n.keys[mid], key)
		if cmp <= 0 {
			// keys[mid] <= key, so key belongs in a child to the right
			lo = mid + 1
		} else {
			// keys[mid] > key, so key belongs in this child or to the left
			hi = mid
		}
	}
	return lo
}

// Clone creates a mutable copy of this node for modification
func (n *CowNode) Clone() *CowNode {
	clone := &CowNode{
		isLeaf:  n.isLeaf,
		version: n.version + 1,
	}

	// Deep copy keys
	clone.keys = make([][]byte, len(n.keys))
	for i, key := range n.keys {
		clone.keys[i] = make([]byte, len(key))
		copy(clone.keys[i], key)
	}

	if n.isLeaf {
		// Deep copy values for leaf nodes
		clone.values = make([][]byte, len(n.values))
		for i, val := range n.values {
			clone.values[i] = make([]byte, len(val))
			copy(clone.values[i], val)
		}
		// Copy leaf chain pointers
		clone.next = atomic.LoadPointer(&n.next)
		clone.prev = atomic.LoadPointer(&n.prev)
	} else {
		// Shallow copy children pointers for interior nodes
		// Children are immutable, so we can share references
		clone.children = make([]unsafe.Pointer, len(n.children))
		for i := range n.children {
			clone.children[i] = atomic.LoadPointer(&n.children[i])
		}
	}

	return clone
}

// insertKeyValue inserts a key-value pair into a leaf node clone
// Assumes the node has been cloned and is mutable
func (n *CowNode) insertKeyValue(key, value []byte) {
	pos := n.findKeyPosition(key)

	// Check if key already exists at this position
	if pos < len(n.keys) && bytes.Equal(n.keys[pos], key) {
		// Update existing key
		n.values[pos] = copyBytes(value)
		return
	}

	// Insert new key-value
	n.keys = insertAt(n.keys, pos, copyBytes(key))
	n.values = insertAt(n.values, pos, copyBytes(value))
}

// insertChild inserts a separator key and child into an interior node clone
// The child goes to the right of the separator key
func (n *CowNode) insertChild(key []byte, child *CowNode) {
	pos := n.findKeyPosition(key)

	// Insert separator key
	n.keys = insertAt(n.keys, pos, copyBytes(key))

	// Insert child pointer at pos+1 (child is to the right of the separator)
	childPtr := unsafe.Pointer(child)
	n.children = insertPointerAt(n.children, pos+1, childPtr)
}

// deleteKeyValue deletes a key from a leaf node clone
// Returns true if the key was found and deleted
func (n *CowNode) deleteKeyValue(key []byte) bool {
	pos := n.findKeyPosition(key)

	if pos >= len(n.keys) || !bytes.Equal(n.keys[pos], key) {
		return false
	}

	n.keys = deleteAt(n.keys, pos)
	n.values = deleteAt(n.values, pos)
	return true
}

// setChild sets the child at position i
func (n *CowNode) setChild(i int, child *CowNode) {
	if i >= 0 && i < len(n.children) {
		atomic.StorePointer(&n.children[i], unsafe.Pointer(child))
	}
}

// setNextLeaf sets the next leaf pointer
func (n *CowNode) setNextLeaf(next *CowNode) {
	atomic.StorePointer(&n.next, unsafe.Pointer(next))
}

// setPrevLeaf sets the previous leaf pointer
func (n *CowNode) setPrevLeaf(prev *CowNode) {
	atomic.StorePointer(&n.prev, unsafe.Pointer(prev))
}

// IsFull returns true if the node has reached maximum capacity
func (n *CowNode) IsFull(maxKeys int) bool {
	return len(n.keys) >= maxKeys
}

// IsUnderflow returns true if the node has fewer than minimum keys
// Minimum is ceil(maxKeys/2) for non-root nodes
func (n *CowNode) IsUnderflow(maxKeys int, isRoot bool) bool {
	if isRoot {
		// Root can have as few as 1 key (or 0 for empty tree)
		return false
	}
	minKeys := (maxKeys + 1) / 2
	return len(n.keys) < minKeys
}

// split splits a full node into two nodes
// Returns (medianKey, rightNode)
// The original node (after modification) becomes the left node
func (n *CowNode) split() ([]byte, *CowNode) {
	mid := len(n.keys) / 2

	if n.isLeaf {
		// Leaf split: median key is copied up, both halves keep their keys
		right := NewLeafNode()
		right.version = n.version

		// Right node gets keys[mid:]
		right.keys = make([][]byte, len(n.keys)-mid)
		right.values = make([][]byte, len(n.values)-mid)
		for i := mid; i < len(n.keys); i++ {
			right.keys[i-mid] = n.keys[i]
			right.values[i-mid] = n.values[i]
		}

		// Truncate left node
		n.keys = n.keys[:mid]
		n.values = n.values[:mid]

		// Update leaf chain
		oldNext := n.GetNextLeaf()
		n.setNextLeaf(right)
		right.setPrevLeaf(n)
		right.setNextLeaf(oldNext)
		if oldNext != nil {
			oldNext.setPrevLeaf(right)
		}

		// Return first key of right node as separator
		medianKey := copyBytes(right.keys[0])
		return medianKey, right
	}

	// Interior split: median key is promoted, not kept in either node
	right := NewInteriorNode()
	right.version = n.version

	// Right node gets keys[mid+1:] and children[mid+1:]
	right.keys = make([][]byte, len(n.keys)-mid-1)
	for i := mid + 1; i < len(n.keys); i++ {
		right.keys[i-mid-1] = n.keys[i]
	}

	right.children = make([]unsafe.Pointer, len(n.children)-mid-1)
	for i := mid + 1; i < len(n.children); i++ {
		right.children[i-mid-1] = n.children[i]
	}

	// Get median key before truncating
	medianKey := copyBytes(n.keys[mid])

	// Truncate left node
	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]

	return medianKey, right
}

// Helper functions

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func insertAt[T any](slice []T, pos int, value T) []T {
	slice = append(slice, value) // Grow by one
	copy(slice[pos+1:], slice[pos:])
	slice[pos] = value
	return slice
}

func insertPointerAt(slice []unsafe.Pointer, pos int, value unsafe.Pointer) []unsafe.Pointer {
	var zero unsafe.Pointer
	slice = append(slice, zero) // Grow by one
	copy(slice[pos+1:], slice[pos:])
	slice[pos] = value
	return slice
}

func deleteAt[T any](slice []T, pos int) []T {
	copy(slice[pos:], slice[pos+1:])
	return slice[:len(slice)-1]
}
