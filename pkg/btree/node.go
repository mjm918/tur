// pkg/btree/node.go
package btree

import (
	"encoding/binary"
	"errors"

	"tur/internal/encoding"
)

/*
Node Page Layout (SQLite-inspired):
+------------------+
| Header (12 bytes)|
|   - flags (1)    |
|   - cell count(2)|
|   - free start(2)|
|   - free end (2) |
|   - frag bytes(1)|
|   - right child(4)| (interior only)
+------------------+
| Cell Pointers    |
| (2 bytes each)   |
+------------------+
| Free Space       |
+------------------+
| Cell Content     |
| (grows upward)   |
+------------------+
*/

const (
	nodeHeaderSize       = 12
	cellPointerSize      = 2
	flagLeaf        byte = 0x01
)

var (
	ErrNodeFull     = errors.New("node is full")
	ErrCellNotFound = errors.New("cell not found")
)

// Node represents a B-tree node backed by a page
type Node struct {
	data []byte
}

// NewNode creates a new node, initializing the header
func NewNode(data []byte, isLeaf bool) *Node {
	n := &Node{data: data}

	// Initialize header
	if isLeaf {
		data[0] = flagLeaf
	} else {
		data[0] = 0
	}

	// Cell count = 0
	binary.LittleEndian.PutUint16(data[1:3], 0)

	// Free space starts after header
	binary.LittleEndian.PutUint16(data[3:5], nodeHeaderSize)

	// Free space ends at page end
	binary.LittleEndian.PutUint16(data[5:7], uint16(len(data)))

	// Fragmented bytes = 0
	data[7] = 0

	return n
}

// LoadNode loads an existing node from page data
func LoadNode(data []byte) *Node {
	return &Node{data: data}
}

// IsLeaf returns true if this is a leaf node
func (n *Node) IsLeaf() bool {
	return n.data[0]&flagLeaf != 0
}

// CellCount returns the number of cells in this node
func (n *Node) CellCount() int {
	return int(binary.LittleEndian.Uint16(n.data[1:3]))
}

func (n *Node) setCellCount(count int) {
	binary.LittleEndian.PutUint16(n.data[1:3], uint16(count))
}

// freeStart returns the offset where cell pointers end
func (n *Node) freeStart() int {
	return int(binary.LittleEndian.Uint16(n.data[3:5]))
}

func (n *Node) setFreeStart(offset int) {
	binary.LittleEndian.PutUint16(n.data[3:5], uint16(offset))
}

// freeEnd returns the offset where cell content starts
func (n *Node) freeEnd() int {
	return int(binary.LittleEndian.Uint16(n.data[5:7]))
}

func (n *Node) setFreeEnd(offset int) {
	binary.LittleEndian.PutUint16(n.data[5:7], uint16(offset))
}

// FreeSpace returns the amount of free space available
func (n *Node) FreeSpace() int {
	return n.freeEnd() - n.freeStart()
}

// cellPointer returns the offset of cell i's pointer
func (n *Node) cellPointerOffset(i int) int {
	return nodeHeaderSize + i*cellPointerSize
}

// getCellOffset returns the offset of cell i's content
func (n *Node) getCellOffset(i int) int {
	ptrOffset := n.cellPointerOffset(i)
	return int(binary.LittleEndian.Uint16(n.data[ptrOffset:]))
}

func (n *Node) setCellOffset(i, offset int) {
	ptrOffset := n.cellPointerOffset(i)
	binary.LittleEndian.PutUint16(n.data[ptrOffset:], uint16(offset))
}

// InsertCell inserts a key-value cell at position i
func (n *Node) InsertCell(i int, key, value []byte) error {
	// Calculate cell size: key_len(varint) + key + value_len(varint) + value
	cellSize := encoding.VarintLen(uint64(len(key))) + len(key) +
		encoding.VarintLen(uint64(len(value))) + len(value)

	// Check if we have enough space
	spaceNeeded := cellSize + cellPointerSize
	if n.FreeSpace() < spaceNeeded {
		return ErrNodeFull
	}

	count := n.CellCount()

	// Shift cell pointers to make room at position i
	for j := count; j > i; j-- {
		n.setCellOffset(j, n.getCellOffset(j-1))
	}

	// Allocate space for cell content (grows from end of page backward)
	newFreeEnd := n.freeEnd() - cellSize
	n.setFreeEnd(newFreeEnd)

	// Write cell content
	offset := newFreeEnd
	offset += encoding.PutVarint(n.data[offset:], uint64(len(key)))
	copy(n.data[offset:], key)
	offset += len(key)
	offset += encoding.PutVarint(n.data[offset:], uint64(len(value)))
	copy(n.data[offset:], value)

	// Set cell pointer
	n.setCellOffset(i, newFreeEnd)

	// Update cell count and free start
	n.setCellCount(count + 1)
	n.setFreeStart(n.freeStart() + cellPointerSize)

	return nil
}

// GetCell returns the key and value at position i
func (n *Node) GetCell(i int) (key, value []byte) {
	if i < 0 || i >= n.CellCount() {
		return nil, nil
	}

	offset := n.getCellOffset(i)

	// Read key
	keyLen, sz := encoding.GetVarint(n.data[offset:])
	offset += sz
	key = n.data[offset : offset+int(keyLen)]
	offset += int(keyLen)

	// Read value
	valueLen, sz := encoding.GetVarint(n.data[offset:])
	offset += sz
	value = n.data[offset : offset+int(valueLen)]

	return key, value
}

// SetRightChild sets the right child page number (interior nodes only)
func (n *Node) SetRightChild(pageNo uint32) {
	binary.LittleEndian.PutUint32(n.data[8:12], pageNo)
}

// RightChild returns the right child page number
func (n *Node) RightChild() uint32 {
	return binary.LittleEndian.Uint32(n.data[8:12])
}

// Split splits the node at midpoint, moving upper half to rightNode
// Returns the median key that should be promoted to the parent
func (n *Node) Split(rightData []byte) ([]byte, *Node) {
	count := n.CellCount()
	mid := count / 2

	// Create right node with same type (leaf or interior)
	right := NewNode(rightData, n.IsLeaf())

	// Copy cells [mid, count) to right node for leaf nodes
	// For interior nodes, we promote the median key and copy [mid+1, count)
	startIdx := mid
	if !n.IsLeaf() {
		startIdx = mid + 1
	}

	for i := startIdx; i < count; i++ {
		key, value := n.GetCell(i)
		right.InsertCell(right.CellCount(), key, value)
	}

	// Get median key (will be promoted to parent)
	medianKey, medianValue := n.GetCell(mid)
	medianKeyCopy := make([]byte, len(medianKey))
	copy(medianKeyCopy, medianKey)

	// For interior nodes, the right child of the median becomes the leftmost child of right
	if !n.IsLeaf() {
		// The median's value is a child pointer - this becomes right's implicit left child
		// We need to handle this in the parent promotion
		right.SetRightChild(n.RightChild())
	}

	// Truncate left node
	if n.IsLeaf() {
		// Leaf: keep [0, mid), median goes to right
		n.truncateTo(mid)
	} else {
		// Interior: keep [0, mid), median is promoted
		n.truncateTo(mid)
		// Set the right child to what was the median's child pointer
		if len(medianValue) >= 4 {
			childPage := binary.LittleEndian.Uint32(medianValue)
			n.SetRightChild(childPage)
		}
	}

	return medianKeyCopy, right
}

// truncateTo keeps only the first n cells, resetting free space
func (n *Node) truncateTo(count int) {
	if count >= n.CellCount() {
		return
	}

	// We need to rebuild the node with only the first count cells
	// This is inefficient but correct - a production implementation
	// would track cell boundaries more carefully

	// Save cells we want to keep
	cells := make([]struct{ key, value []byte }, count)
	for i := 0; i < count; i++ {
		k, v := n.GetCell(i)
		cells[i].key = make([]byte, len(k))
		cells[i].value = make([]byte, len(v))
		copy(cells[i].key, k)
		copy(cells[i].value, v)
	}

	// Save node properties
	isLeaf := n.IsLeaf()
	rightChild := n.RightChild()
	pageSize := len(n.data)

	// Reinitialize node
	if isLeaf {
		n.data[0] = flagLeaf
	} else {
		n.data[0] = 0
	}
	binary.LittleEndian.PutUint16(n.data[1:3], 0)
	binary.LittleEndian.PutUint16(n.data[3:5], nodeHeaderSize)
	binary.LittleEndian.PutUint16(n.data[5:7], uint16(pageSize))
	n.data[7] = 0
	n.SetRightChild(rightChild)

	// Re-insert cells
	for i, cell := range cells {
		n.InsertCell(i, cell.key, cell.value)
	}
}

// DeleteCell removes the cell at position i
func (n *Node) DeleteCell(i int) {
	count := n.CellCount()
	if i < 0 || i >= count {
		return
	}

	// Shift cell pointers left
	for j := i; j < count-1; j++ {
		n.setCellOffset(j, n.getCellOffset(j+1))
	}

	n.setCellCount(count - 1)
	n.setFreeStart(n.freeStart() - cellPointerSize)
	// Note: cell content space is not reclaimed (fragmentation)
}

// UpdateCellValue updates the value of the cell at position i
// This only works if the new value is the same size as the old value
// (used for updating child pointers in interior nodes)
func (n *Node) UpdateCellValue(i int, newValue []byte) {
	if i < 0 || i >= n.CellCount() {
		return
	}

	offset := n.getCellOffset(i)

	// Skip key
	keyLen, sz := encoding.GetVarint(n.data[offset:])
	offset += sz + int(keyLen)

	// Read old value length
	oldValueLen, sz := encoding.GetVarint(n.data[offset:])
	offset += sz

	// Only update if same size (for child pointers, this is always 4 bytes)
	if int(oldValueLen) == len(newValue) {
		copy(n.data[offset:offset+len(newValue)], newValue)
	}
}
