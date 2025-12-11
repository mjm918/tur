// pkg/cowbtree/cowbtree.go
package cowbtree

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrTreeClosed    = errors.New("tree is closed")
	ErrCASFailed     = errors.New("compare-and-swap failed, concurrent modification")
	ErrInvalidKey    = errors.New("key cannot be nil")
	ErrInvalidValue  = errors.New("value cannot be nil")
)

// CowBTree is a Copy-on-Write B+ tree that provides lock-free reads.
//
// Design principles:
// - Reads are completely lock-free using epoch-based reclamation
// - Writes are serialized with a mutex but use path copying (CoW)
// - The root pointer is atomically swapped after each write
// - Old tree versions are retired and reclaimed when safe
//
// This design is inspired by:
// - LMDB's copy-on-write B+ tree
// - Bw-tree's lock-free architecture
// - Epoch-based reclamation from concurrent data structures research
type CowBTree struct {
	// root is the current root node, accessed atomically
	// Stored as unsafe.Pointer to enable atomic CAS operations
	root unsafe.Pointer // *CowNode

	// writeMu serializes write operations
	// Reads don't acquire this lock - they're lock-free
	writeMu sync.Mutex

	// epoch manages safe memory reclamation
	epoch *EpochManager

	// config holds tree configuration
	config NodeConfig

	// stats tracks tree statistics atomically
	stats CowBTreeStats

	// closed indicates the tree has been shut down
	closed int32 // atomic
}

// CowBTreeStats holds tree statistics
type CowBTreeStats struct {
	KeyCount     int64  // Total number of keys
	NodeCount    int64  // Total number of nodes
	Height       int64  // Tree height
	InsertCount  int64  // Total insert operations
	DeleteCount  int64  // Total delete operations
	GetCount     int64  // Total get operations
	SplitCount   int64  // Total node splits
	MergeCount   int64  // Total node merges
	CowCopyCount int64  // Total CoW node copies
}

// NewCowBTree creates a new CoW B+ tree with default configuration
func NewCowBTree() *CowBTree {
	return NewCowBTreeWithConfig(DefaultNodeConfig())
}

// NewCowBTreeWithConfig creates a new CoW B+ tree with custom configuration
func NewCowBTreeWithConfig(config NodeConfig) *CowBTree {
	tree := &CowBTree{
		epoch:  NewEpochManager(),
		config: config,
	}

	// Initialize with empty leaf root
	root := NewLeafNode()
	atomic.StorePointer(&tree.root, unsafe.Pointer(root))
	atomic.AddInt64(&tree.stats.NodeCount, 1)
	atomic.StoreInt64(&tree.stats.Height, 1)

	return tree
}

// getRoot returns the current root node
func (t *CowBTree) getRoot() *CowNode {
	ptr := atomic.LoadPointer(&t.root)
	if ptr == nil {
		return nil
	}
	return (*CowNode)(ptr)
}

// setRoot atomically sets a new root
func (t *CowBTree) setRoot(newRoot *CowNode) {
	atomic.StorePointer(&t.root, unsafe.Pointer(newRoot))
}

// casRoot attempts to atomically swap the root
// Returns true if successful, false if current root has changed
func (t *CowBTree) casRoot(oldRoot, newRoot *CowNode) bool {
	return atomic.CompareAndSwapPointer(
		&t.root,
		unsafe.Pointer(oldRoot),
		unsafe.Pointer(newRoot),
	)
}

// Get retrieves the value for a key (lock-free read)
func (t *CowBTree) Get(key []byte) ([]byte, error) {
	if atomic.LoadInt32(&t.closed) == 1 {
		return nil, ErrTreeClosed
	}

	if key == nil {
		return nil, ErrInvalidKey
	}

	atomic.AddInt64(&t.stats.GetCount, 1)

	// Enter epoch to protect against reclamation
	guard := t.epoch.Enter()
	defer guard.Leave()

	// Get current root (stable for this read)
	root := t.getRoot()
	if root == nil {
		return nil, ErrKeyNotFound
	}

	// Navigate to leaf
	node := root
	for !node.IsLeaf() {
		childIdx := node.findChildIndex(key)
		child := node.GetChild(childIdx)
		if child == nil {
			return nil, ErrKeyNotFound
		}
		node = child
	}

	// Search in leaf
	pos := node.findKeyPosition(key)
	if pos < node.KeyCount() && bytes.Equal(node.GetKey(pos), key) {
		// Found - return a copy of the value
		return copyBytes(node.GetValue(pos)), nil
	}

	return nil, ErrKeyNotFound
}

// Insert inserts or updates a key-value pair
func (t *CowBTree) Insert(key, value []byte) error {
	if atomic.LoadInt32(&t.closed) == 1 {
		return ErrTreeClosed
	}

	if key == nil {
		return ErrInvalidKey
	}

	if value == nil {
		return ErrInvalidValue
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	atomic.AddInt64(&t.stats.InsertCount, 1)

	// Get current root
	oldRoot := t.getRoot()

	// Perform insertion with path copying
	newRoot, split, increased, err := t.insertRecursive(oldRoot, key, value, true)
	if err != nil {
		return err
	}

	// If root was split, create a new root
	if split != nil {
		newRootNode := NewInteriorNode()
		newRootNode.keys = [][]byte{copyBytes(split.splitKey)}
		newRootNode.children = []unsafe.Pointer{
			unsafe.Pointer(split.left),
			unsafe.Pointer(split.right),
		}
		atomic.AddInt64(&t.stats.NodeCount, 1)
		atomic.AddInt64(&t.stats.Height, 1)
		newRoot = newRootNode
	}

	// Retire old nodes that were replaced
	t.retireOldPath(oldRoot, newRoot)

	// Atomically swap root
	t.setRoot(newRoot)

	// Update key count
	if increased {
		atomic.AddInt64(&t.stats.KeyCount, 1)
	}

	// Advance epoch and try to reclaim old nodes
	t.epoch.Advance()
	t.epoch.TryReclaim()

	return nil
}

// splitInfo holds information about a node split
type splitInfo struct {
	left     *CowNode // Left child (modified original)
	right    *CowNode // Right child (new node)
	splitKey []byte   // Key to promote to parent
}

// insertRecursive performs recursive insertion with path copying
// Returns (newNode, splitInfo, keyCountIncreased, error)
// If split occurred, newNode is the left half and splitInfo contains split details
func (t *CowBTree) insertRecursive(node *CowNode, key, value []byte, isRoot bool) (*CowNode, *splitInfo, bool, error) {
	if node.IsLeaf() {
		return t.insertIntoLeaf(node, key, value)
	}
	return t.insertIntoInterior(node, key, value, isRoot)
}

// insertIntoLeaf inserts into a leaf node, handling splits
func (t *CowBTree) insertIntoLeaf(node *CowNode, key, value []byte) (*CowNode, *splitInfo, bool, error) {
	// Clone the node for modification
	clone := node.Clone()
	atomic.AddInt64(&t.stats.CowCopyCount, 1)

	// Check if key already exists
	pos := clone.findKeyPosition(key)
	keyExists := pos < clone.KeyCount() && bytes.Equal(clone.GetKey(pos), key)

	// Insert/update the key
	clone.insertKeyValue(key, value)

	// Check if we need to split
	if clone.IsFull(t.config.MaxKeys) {
		medianKey, right := clone.split()
		atomic.AddInt64(&t.stats.SplitCount, 1)
		atomic.AddInt64(&t.stats.NodeCount, 1)

		// Return split info - let caller handle creating new root if needed
		return clone, &splitInfo{
			left:     clone,
			right:    right,
			splitKey: medianKey,
		}, !keyExists, nil
	}

	return clone, nil, !keyExists, nil
}

// insertIntoInterior inserts into an interior node
func (t *CowBTree) insertIntoInterior(node *CowNode, key, value []byte, isRoot bool) (*CowNode, *splitInfo, bool, error) {
	// Find child to descend into
	childIdx := node.findChildIndex(key)
	child := node.GetChild(childIdx)
	if child == nil {
		return nil, nil, false, errors.New("invalid tree structure: nil child")
	}

	// Recursively insert into child
	newChild, childSplit, increased, err := t.insertRecursive(child, key, value, false)
	if err != nil {
		return nil, nil, false, err
	}

	// Clone this node
	clone := node.Clone()
	atomic.AddInt64(&t.stats.CowCopyCount, 1)

	// Update child pointer
	clone.setChild(childIdx, newChild)

	// If child was split, absorb the split into this node
	if childSplit != nil {
		// Insert the separator key and right child
		clone.insertChild(childSplit.splitKey, childSplit.right)

		// Check if this node now needs to split
		if clone.IsFull(t.config.MaxKeys) {
			medianKey, right := clone.split()
			atomic.AddInt64(&t.stats.SplitCount, 1)
			atomic.AddInt64(&t.stats.NodeCount, 1)

			// Return split info
			return clone, &splitInfo{
				left:     clone,
				right:    right,
				splitKey: medianKey,
			}, increased, nil
		}
	}

	return clone, nil, increased, nil
}

// Delete removes a key from the tree
func (t *CowBTree) Delete(key []byte) error {
	if atomic.LoadInt32(&t.closed) == 1 {
		return ErrTreeClosed
	}

	if key == nil {
		return ErrInvalidKey
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	atomic.AddInt64(&t.stats.DeleteCount, 1)

	// Get current root
	oldRoot := t.getRoot()

	// Perform deletion with path copying
	newRoot, found, err := t.deleteRecursive(oldRoot, key, true)
	if err != nil {
		return err
	}

	if !found {
		return ErrKeyNotFound
	}

	// Retire old nodes
	t.retireOldPath(oldRoot, newRoot)

	// Handle root becoming empty
	if newRoot != nil && !newRoot.IsLeaf() && newRoot.KeyCount() == 0 {
		// Root is an interior node with no keys - use its only child as new root
		if len(newRoot.children) > 0 {
			newRoot = newRoot.GetChild(0)
			atomic.AddInt64(&t.stats.Height, -1)
		}
	}

	// Atomically swap root
	t.setRoot(newRoot)

	// Update key count
	atomic.AddInt64(&t.stats.KeyCount, -1)

	// Advance epoch and reclaim
	t.epoch.Advance()
	t.epoch.TryReclaim()

	return nil
}

// deleteRecursive performs recursive deletion with path copying
// Returns (newNode, keyFound, error)
func (t *CowBTree) deleteRecursive(node *CowNode, key []byte, isRoot bool) (*CowNode, bool, error) {
	if node.IsLeaf() {
		return t.deleteFromLeaf(node, key)
	}
	return t.deleteFromInterior(node, key, isRoot)
}

// deleteFromLeaf deletes from a leaf node
func (t *CowBTree) deleteFromLeaf(node *CowNode, key []byte) (*CowNode, bool, error) {
	// Check if key exists
	pos := node.findKeyPosition(key)
	if pos >= node.KeyCount() || !bytes.Equal(node.GetKey(pos), key) {
		return node, false, nil
	}

	// Clone and delete
	clone := node.Clone()
	atomic.AddInt64(&t.stats.CowCopyCount, 1)
	clone.deleteKeyValue(key)

	return clone, true, nil
}

// deleteFromInterior handles deletion in interior nodes
func (t *CowBTree) deleteFromInterior(node *CowNode, key []byte, isRoot bool) (*CowNode, bool, error) {
	// Find child containing the key
	childIdx := node.findChildIndex(key)
	child := node.GetChild(childIdx)
	if child == nil {
		return node, false, nil
	}

	// Recursively delete from child
	newChild, found, err := t.deleteRecursive(child, key, false)
	if err != nil {
		return nil, false, err
	}

	if !found {
		return node, false, nil
	}

	// Clone this node
	clone := node.Clone()
	atomic.AddInt64(&t.stats.CowCopyCount, 1)

	// Update child pointer
	clone.setChild(childIdx, newChild)

	// In a full implementation, we'd handle underflow and rebalancing here
	// For simplicity, we use lazy delete (like SQLite) and tolerate underflow

	return clone, true, nil
}

// retireOldPath retires nodes that were replaced during a modification
func (t *CowBTree) retireOldPath(oldRoot, newRoot *CowNode) {
	if oldRoot == nil || oldRoot == newRoot {
		return
	}

	// Simple approach: retire the old root
	// In practice, we'd walk the old path and retire all replaced nodes
	t.epoch.Retire(oldRoot)
}

// Range performs a range scan from startKey to endKey (inclusive)
// This is a lock-free read operation
func (t *CowBTree) Range(startKey, endKey []byte, fn func(key, value []byte) bool) error {
	if atomic.LoadInt32(&t.closed) == 1 {
		return ErrTreeClosed
	}

	// Enter epoch for the entire scan
	guard := t.epoch.Enter()
	defer guard.Leave()

	root := t.getRoot()
	if root == nil {
		return nil
	}

	// Find the leaf containing startKey
	node := root
	for !node.IsLeaf() {
		childIdx := node.findChildIndex(startKey)
		child := node.GetChild(childIdx)
		if child == nil {
			return nil
		}
		node = child
	}

	// Scan leaves until we pass endKey
	for node != nil {
		for i := 0; i < node.KeyCount(); i++ {
			key := node.GetKey(i)

			// Skip keys before startKey
			if startKey != nil && bytes.Compare(key, startKey) < 0 {
				continue
			}

			// Stop if we've passed endKey
			if endKey != nil && bytes.Compare(key, endKey) > 0 {
				return nil
			}

			// Call the callback
			if !fn(key, node.GetValue(i)) {
				return nil
			}
		}

		// Move to next leaf
		node = node.GetNextLeaf()
	}

	return nil
}

// RangeScan is an alias for Range with a more descriptive name
func (t *CowBTree) RangeScan(startKey, endKey []byte, fn func(key, value []byte) bool) error {
	return t.Range(startKey, endKey, fn)
}

// ForEach iterates over all key-value pairs in order
func (t *CowBTree) ForEach(fn func(key, value []byte) bool) error {
	return t.Range(nil, nil, fn)
}

// Stats returns a snapshot of the tree statistics
func (t *CowBTree) Stats() CowBTreeStats {
	return CowBTreeStats{
		KeyCount:     atomic.LoadInt64(&t.stats.KeyCount),
		NodeCount:    atomic.LoadInt64(&t.stats.NodeCount),
		Height:       atomic.LoadInt64(&t.stats.Height),
		InsertCount:  atomic.LoadInt64(&t.stats.InsertCount),
		DeleteCount:  atomic.LoadInt64(&t.stats.DeleteCount),
		GetCount:     atomic.LoadInt64(&t.stats.GetCount),
		SplitCount:   atomic.LoadInt64(&t.stats.SplitCount),
		MergeCount:   atomic.LoadInt64(&t.stats.MergeCount),
		CowCopyCount: atomic.LoadInt64(&t.stats.CowCopyCount),
	}
}

// KeyCount returns the current number of keys in the tree
func (t *CowBTree) KeyCount() int64 {
	return atomic.LoadInt64(&t.stats.KeyCount)
}

// Close shuts down the tree and reclaims all memory
func (t *CowBTree) Close() error {
	if !atomic.CompareAndSwapInt32(&t.closed, 0, 1) {
		return ErrTreeClosed
	}

	// Wait for readers to finish and reclaim all nodes
	for t.epoch.ActiveReaderCount() > 0 {
		t.epoch.Advance()
		t.epoch.TryReclaim()
	}

	return nil
}

// Snapshot creates a read-only snapshot of the current tree state
// The snapshot provides a consistent view even as the tree is modified
func (t *CowBTree) Snapshot() *CowBTreeSnapshot {
	guard := t.epoch.Enter()
	root := t.getRoot()

	return &CowBTreeSnapshot{
		root:   root,
		guard:  guard,
		tree:   t,
		config: t.config,
	}
}

// CowBTreeSnapshot represents a consistent read-only view of the tree
type CowBTreeSnapshot struct {
	root   *CowNode
	guard  *ReaderGuard
	tree   *CowBTree
	config NodeConfig
}

// Get retrieves a value from the snapshot
func (s *CowBTreeSnapshot) Get(key []byte) ([]byte, error) {
	if s.root == nil {
		return nil, ErrKeyNotFound
	}

	node := s.root
	for !node.IsLeaf() {
		childIdx := node.findChildIndex(key)
		child := node.GetChild(childIdx)
		if child == nil {
			return nil, ErrKeyNotFound
		}
		node = child
	}

	pos := node.findKeyPosition(key)
	if pos < node.KeyCount() && bytes.Equal(node.GetKey(pos), key) {
		return copyBytes(node.GetValue(pos)), nil
	}

	return nil, ErrKeyNotFound
}

// Range performs a range scan on the snapshot
func (s *CowBTreeSnapshot) Range(startKey, endKey []byte, fn func(key, value []byte) bool) error {
	if s.root == nil {
		return nil
	}

	// Find starting leaf
	node := s.root
	for !node.IsLeaf() {
		childIdx := node.findChildIndex(startKey)
		child := node.GetChild(childIdx)
		if child == nil {
			return nil
		}
		node = child
	}

	// Scan
	for node != nil {
		for i := 0; i < node.KeyCount(); i++ {
			key := node.GetKey(i)
			if startKey != nil && bytes.Compare(key, startKey) < 0 {
				continue
			}
			if endKey != nil && bytes.Compare(key, endKey) > 0 {
				return nil
			}
			if !fn(key, node.GetValue(i)) {
				return nil
			}
		}
		node = node.GetNextLeaf()
	}

	return nil
}

// Release releases the snapshot, allowing old nodes to be reclaimed
func (s *CowBTreeSnapshot) Release() {
	if s.guard != nil {
		s.guard.Leave()
		s.guard = nil
	}
}
