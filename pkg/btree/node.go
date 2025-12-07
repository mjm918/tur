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
