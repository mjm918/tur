// pkg/btree/btree.go
package btree

import (
	"bytes"
	"encoding/binary"
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
	// Note: Don't use SetType here - the node header already contains type info in data[0]

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

// CreateAtPage creates a new B-tree at the specified page number.
// If pages need to be allocated to reach pageNo, they will be allocated.
// The page will be initialized as an empty leaf node.
func CreateAtPage(p *pager.Pager, pageNo uint32) (*BTree, error) {
	var page *pager.Page
	var err error

	// Allocate pages until we reach the desired page number
	for p.PageCount() <= pageNo {
		page, err = p.Allocate()
		if err != nil {
			return nil, err
		}
		if page.PageNo() != pageNo {
			p.Release(page)
		}
	}

	// Get the specific page
	page, err = p.Get(pageNo)
	if err != nil {
		return nil, err
	}
	defer p.Release(page)

	// Initialize as empty leaf node
	NewNode(page.Data(), true)
	page.SetDirty(true)

	return &BTree{
		pager:    p,
		rootPage: pageNo,
	}, nil
}

// RootPage returns the root page number
func (bt *BTree) RootPage() uint32 {
	return bt.rootPage
}

// Insert inserts or updates a key-value pair
func (bt *BTree) Insert(key, value []byte) error {
	// Use insertRecursive which handles splits
	_, newRootPage, err := bt.insertRecursive(bt.rootPage, key, value)
	if err != nil {
		return err
	}

	// If root was split, we got a new root page number
	if newRootPage != 0 {
		bt.rootPage = newRootPage
	}

	return nil
}

// splitResult is returned when a node split occurs during insertion
type splitResult struct {
	promotedKey []byte // Key to promote to parent
	rightPageNo uint32 // Page number of the new right sibling
}

// insertRecursive inserts into subtree, returns split info if split occurred
func (bt *BTree) insertRecursive(pageNo uint32, key, value []byte) (*splitResult, uint32, error) {
	page, err := bt.pager.Get(pageNo)
	if err != nil {
		return nil, 0, err
	}
	defer bt.pager.Release(page)

	node := LoadNode(page.Data())

	if node.IsLeaf() {
		return bt.insertIntoLeaf(page, node, key, value)
	}
	return bt.insertIntoInterior(page, node, key, value)
}

// insertIntoLeaf inserts into a leaf node, handling splits if necessary
func (bt *BTree) insertIntoLeaf(page *pager.Page, node *Node, key, value []byte) (*splitResult, uint32, error) {
	pos := bt.findPosition(node, key)

	// Check for update (key exists)
	if pos < node.CellCount() {
		existingKey, _ := node.GetCell(pos)
		if bytes.Equal(existingKey, key) {
			node.DeleteCell(pos)
			pos = bt.findPosition(node, key)
		}
	}

	// Try to insert
	err := node.InsertCell(pos, key, value)
	if err == nil {
		page.SetDirty(true)
		return nil, 0, nil
	}

	if err != ErrNodeFull {
		return nil, 0, err
	}

	// Need to split
	return bt.splitLeaf(page, node, key, value, pos)
}

// splitLeaf splits a full leaf node
func (bt *BTree) splitLeaf(page *pager.Page, node *Node, key, value []byte, pos int) (*splitResult, uint32, error) {
	// Allocate new page for right sibling
	rightPage, err := bt.pager.Allocate()
	if err != nil {
		return nil, 0, err
	}
	defer bt.pager.Release(rightPage)

	// Split the node
	medianKey, rightNode := node.Split(rightPage.Data())
	rightPage.SetDirty(true)
	// Note: Don't use SetType here - the node header already contains type info

	// Insert the new key into appropriate side
	if bytes.Compare(key, medianKey) < 0 {
		node.InsertCell(bt.findPosition(node, key), key, value)
	} else {
		rightNode.InsertCell(bt.findPosition(rightNode, key), key, value)
	}
	page.SetDirty(true)

	// Create split result
	split := &splitResult{
		promotedKey: medianKey,
		rightPageNo: rightPage.PageNo(),
	}

	// If this was the root, create a new root
	if page.PageNo() == bt.rootPage {
		newRoot, err := bt.createNewRoot(page.PageNo(), medianKey, rightPage.PageNo())
		if err != nil {
			return nil, 0, err
		}
		return nil, newRoot, nil
	}

	return split, 0, nil
}

// insertIntoInterior inserts into an interior node by descending to the correct child
func (bt *BTree) insertIntoInterior(page *pager.Page, node *Node, key, value []byte) (*splitResult, uint32, error) {
	// Find which child to descend into and track if it's the rightChild
	childPageNo, childIdx := bt.findChildPageWithIndex(node, key)

	// Recursively insert
	split, newRootPage, err := bt.insertRecursive(childPageNo, key, value)
	if err != nil {
		return nil, 0, err
	}

	// If a new root was created below us, propagate it up
	if newRootPage != 0 {
		return nil, newRootPage, nil
	}

	// If no split occurred in child, we're done
	if split == nil {
		return nil, 0, nil
	}

	// Child was split. The original child (childPageNo) now has keys < promotedKey,
	// and split.rightPageNo has keys >= promotedKey.
	//
	// We need to insert a cell pointing to the LEFT child (original node),
	// and update the pointer that was pointing to the original node to now point to the RIGHT child.

	if childIdx == -1 {
		// The split happened in rightChild
		// Insert cell: (promotedKey, originalChild) - "keys < promotedKey go to originalChild"
		// Update rightChild to point to newRightPage
		pos := bt.findPosition(node, split.promotedKey)
		leftChildPtr := encodePageNo(childPageNo)

		err = node.InsertCell(pos, split.promotedKey, leftChildPtr)
		if err == nil {
			node.SetRightChild(split.rightPageNo)
			page.SetDirty(true)
			return nil, 0, nil
		}

		if err != ErrNodeFull {
			return nil, 0, err
		}

		// This interior node is also full, need to split it
		return bt.splitInteriorFromRightChild(page, node, split.promotedKey, childPageNo, split.rightPageNo)
	}

	// The split happened in cell[childIdx].ptr
	// Insert cell: (promotedKey, originalChild) before the existing cell
	// Update cell[childIdx].ptr to point to newRightPage
	pos := bt.findPosition(node, split.promotedKey)
	leftChildPtr := encodePageNo(childPageNo)

	err = node.InsertCell(pos, split.promotedKey, leftChildPtr)
	if err == nil {
		// Update the cell that was pointing to the original node
		// Since we inserted before it, it's now at pos+1
		node.UpdateCellValue(pos+1, encodePageNo(split.rightPageNo))
		page.SetDirty(true)
		return nil, 0, nil
	}

	if err != ErrNodeFull {
		return nil, 0, err
	}

	// This interior node is also full, need to split it
	return bt.splitInteriorFromCell(page, node, split.promotedKey, childPageNo, split.rightPageNo, childIdx)
}

// splitInteriorFromRightChild handles splitting an interior node when the split originated from rightChild
func (bt *BTree) splitInteriorFromRightChild(page *pager.Page, node *Node, promotedKey []byte, leftChild, rightChild uint32) (*splitResult, uint32, error) {
	// Allocate new page for right sibling
	newRightPage, err := bt.pager.Allocate()
	if err != nil {
		return nil, 0, err
	}
	defer bt.pager.Release(newRightPage)

	// Split the interior node
	medianKey, newRightNode := node.Split(newRightPage.Data())
	newRightPage.SetDirty(true)

	// Now insert the promoted key into the appropriate side
	// The promoted key points to leftChild, and rightChild becomes the new rightChild
	if bytes.Compare(promotedKey, medianKey) < 0 {
		// Insert into left (original) node
		pos := bt.findPosition(node, promotedKey)
		node.InsertCell(pos, promotedKey, encodePageNo(leftChild))
		// The original rightChild was moved during split, update it
		node.SetRightChild(rightChild)
	} else {
		// Insert into right (new) node
		pos := bt.findPosition(newRightNode, promotedKey)
		newRightNode.InsertCell(pos, promotedKey, encodePageNo(leftChild))
		newRightNode.SetRightChild(rightChild)
	}
	page.SetDirty(true)

	// If this was the root, create a new root
	if page.PageNo() == bt.rootPage {
		newRoot, err := bt.createNewRoot(page.PageNo(), medianKey, newRightPage.PageNo())
		if err != nil {
			return nil, 0, err
		}
		return nil, newRoot, nil
	}

	// Return split result for parent to handle
	return &splitResult{
		promotedKey: medianKey,
		rightPageNo: newRightPage.PageNo(),
	}, 0, nil
}

// splitInteriorFromCell handles splitting an interior node when the split originated from a cell pointer
func (bt *BTree) splitInteriorFromCell(page *pager.Page, node *Node, promotedKey []byte, leftChild, rightChild uint32, origCellIdx int) (*splitResult, uint32, error) {
	// Allocate new page for right sibling
	newRightPage, err := bt.pager.Allocate()
	if err != nil {
		return nil, 0, err
	}
	defer bt.pager.Release(newRightPage)

	// Split the interior node
	medianKey, newRightNode := node.Split(newRightPage.Data())
	newRightPage.SetDirty(true)

	// Insert the promoted key into the appropriate side
	if bytes.Compare(promotedKey, medianKey) < 0 {
		// Insert into left (original) node
		pos := bt.findPosition(node, promotedKey)
		node.InsertCell(pos, promotedKey, encodePageNo(leftChild))
		// Update the next cell to point to rightChild
		if pos+1 < node.CellCount() {
			node.UpdateCellValue(pos+1, encodePageNo(rightChild))
		} else {
			node.SetRightChild(rightChild)
		}
	} else {
		// Insert into right (new) node
		pos := bt.findPosition(newRightNode, promotedKey)
		newRightNode.InsertCell(pos, promotedKey, encodePageNo(leftChild))
		// Update the next cell to point to rightChild
		if pos+1 < newRightNode.CellCount() {
			newRightNode.UpdateCellValue(pos+1, encodePageNo(rightChild))
		} else {
			newRightNode.SetRightChild(rightChild)
		}
	}
	page.SetDirty(true)

	// If this was the root, create a new root
	if page.PageNo() == bt.rootPage {
		newRoot, err := bt.createNewRoot(page.PageNo(), medianKey, newRightPage.PageNo())
		if err != nil {
			return nil, 0, err
		}
		return nil, newRoot, nil
	}

	// Return split result for parent to handle
	return &splitResult{
		promotedKey: medianKey,
		rightPageNo: newRightPage.PageNo(),
	}, 0, nil
}

// createNewRoot creates a new root node with the given left child, key, and right child
func (bt *BTree) createNewRoot(leftPage uint32, key []byte, rightPage uint32) (uint32, error) {
	newRootPage, err := bt.pager.Allocate()
	if err != nil {
		return 0, err
	}
	defer bt.pager.Release(newRootPage)

	// Initialize as interior node
	newRoot := NewNode(newRootPage.Data(), false)
	newRoot.InsertCell(0, key, encodePageNo(leftPage))
	newRoot.SetRightChild(rightPage)

	newRootPage.SetDirty(true)
	// Note: Don't use SetType here - the node header already contains type info

	return newRootPage.PageNo(), nil
}

// findChildPage finds which child page to descend into for the given key
func (bt *BTree) findChildPage(node *Node, key []byte) uint32 {
	pageNo, _ := bt.findChildPageWithIndex(node, key)
	return pageNo
}

// findChildPageWithIndex finds which child page to descend into and returns the cell index
// Returns (pageNo, cellIndex) where cellIndex is -1 if rightChild was returned
func (bt *BTree) findChildPageWithIndex(node *Node, key []byte) (uint32, int) {
	count := node.CellCount()
	for i := 0; i < count; i++ {
		cellKey, cellValue := node.GetCell(i)
		if bytes.Compare(key, cellKey) < 0 {
			// Key is less than this cell's key, go to the child pointer in value
			return decodePageNo(cellValue), i
		}
	}
	// Key is >= all keys, go to right child
	return node.RightChild(), -1
}

func encodePageNo(pageNo uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, pageNo)
	return buf
}

func decodePageNo(data []byte) uint32 {
	if len(data) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(data)
}

// Get retrieves the value for a key
func (bt *BTree) Get(key []byte) ([]byte, error) {
	return bt.getRecursive(bt.rootPage, key)
}

// getRecursive searches for a key recursively through the tree
func (bt *BTree) getRecursive(pageNo uint32, key []byte) ([]byte, error) {
	page, err := bt.pager.Get(pageNo)
	if err != nil {
		return nil, err
	}
	defer bt.pager.Release(page)

	node := LoadNode(page.Data())

	if node.IsLeaf() {
		// Binary search for key in leaf
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

	// Interior node: find child to descend into
	childPage := bt.findChildPage(node, key)
	return bt.getRecursive(childPage, key)
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

// Depth returns the depth of the tree (1 = just root)
func (bt *BTree) Depth() int {
	return bt.depthRecursive(bt.rootPage)
}

func (bt *BTree) depthRecursive(pageNo uint32) int {
	page, err := bt.pager.Get(pageNo)
	if err != nil {
		return 0
	}
	defer bt.pager.Release(page)

	node := LoadNode(page.Data())
	if node.IsLeaf() {
		return 1
	}

	// Get first child
	childPage := bt.findChildPage(node, nil)
	return 1 + bt.depthRecursive(childPage)
}

// Delete removes a key from the B-tree.
// This implementation uses a simple approach that allows underflow without
// aggressive rebalancing, similar to SQLite's lazy delete strategy.
// The tree remains valid and searchable, though it may become less balanced.
func (bt *BTree) Delete(key []byte) error {
	return bt.deleteSimple(bt.rootPage, key)
}

// CollectPages returns all page numbers used by this B-tree.
// This is used for freeing pages when dropping a table or index.
func (bt *BTree) CollectPages() []uint32 {
	var pages []uint32
	bt.collectPagesRecursive(bt.rootPage, &pages)
	return pages
}

// collectPagesRecursive recursively collects all page numbers in the B-tree
func (bt *BTree) collectPagesRecursive(pageNo uint32, pages *[]uint32) {
	page, err := bt.pager.Get(pageNo)
	if err != nil {
		return
	}
	defer bt.pager.Release(page)

	*pages = append(*pages, pageNo)

	node := LoadNode(page.Data())
	if !node.IsLeaf() {
		// Interior nodes store child pointers:
		// - Each cell's value contains a child page pointer (left child of that key)
		// - RightChild contains the rightmost child page
		count := node.CellCount()
		for i := 0; i < count; i++ {
			_, cellValue := node.GetCell(i)
			childPage := decodePageNo(cellValue)
			if childPage != 0 {
				bt.collectPagesRecursive(childPage, pages)
			}
		}
		// Don't forget the right child
		rightChild := node.RightChild()
		if rightChild != 0 {
			bt.collectPagesRecursive(rightChild, pages)
		}
	}
}

// deleteSimple performs a straightforward delete without complex rebalancing.
// This approach is used by many production B-tree implementations including SQLite.
func (bt *BTree) deleteSimple(pageNo uint32, key []byte) error {
	page, err := bt.pager.Get(pageNo)
	if err != nil {
		return err
	}
	defer bt.pager.Release(page)

	node := LoadNode(page.Data())

	if node.IsLeaf() {
		pos := bt.findPosition(node, key)
		if pos >= node.CellCount() {
			return ErrKeyNotFound
		}
		foundKey, _ := node.GetCell(pos)
		if !bytes.Equal(foundKey, key) {
			return ErrKeyNotFound
		}
		node.DeleteCell(pos)
		page.SetDirty(true)
		return nil
	}

	// Interior node: find child and recurse
	childPage := bt.findChildPage(node, key)
	return bt.deleteSimple(childPage, key)
}
