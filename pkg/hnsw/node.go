// pkg/hnsw/node.go
package hnsw

import (
	"tur/pkg/types"
)

// HNSWNode represents a node in the HNSW graph
type HNSWNode struct {
	id        uint64
	rowID     int64          // Foreign key to B-tree row
	vector    *types.Vector
	level     int            // Maximum level this node exists at
	neighbors [][]uint64     // neighbors[level] = list of neighbor IDs
}

// NewHNSWNode creates a new HNSW node
func NewHNSWNode(id uint64, rowID int64, vector *types.Vector, level int) *HNSWNode {
	neighbors := make([][]uint64, level+1)
	for i := range neighbors {
		neighbors[i] = make([]uint64, 0)
	}

	return &HNSWNode{
		id:        id,
		rowID:     rowID,
		vector:    vector,
		level:     level,
		neighbors: neighbors,
	}
}

// ID returns the node's unique identifier
func (n *HNSWNode) ID() uint64 {
	return n.id
}

// RowID returns the associated B-tree row ID
func (n *HNSWNode) RowID() int64 {
	return n.rowID
}

// Vector returns the node's vector
func (n *HNSWNode) Vector() *types.Vector {
	return n.vector
}

// Level returns the maximum level this node exists at
func (n *HNSWNode) Level() int {
	return n.level
}

// Neighbors returns the neighbor IDs at the given level
func (n *HNSWNode) Neighbors(level int) []uint64 {
	if level < 0 || level > n.level {
		return nil
	}
	return n.neighbors[level]
}

// AddNeighbor adds a neighbor at the given level
func (n *HNSWNode) AddNeighbor(level int, neighborID uint64) {
	if level < 0 || level > n.level {
		return
	}
	n.neighbors[level] = append(n.neighbors[level], neighborID)
}

// SetNeighbors sets all neighbors at a given level
func (n *HNSWNode) SetNeighbors(level int, neighborIDs []uint64) {
	if level < 0 || level > n.level {
		return
	}
	n.neighbors[level] = make([]uint64, len(neighborIDs))
	copy(n.neighbors[level], neighborIDs)
}

// RemoveNeighbor removes a neighbor at the given level
func (n *HNSWNode) RemoveNeighbor(level int, neighborID uint64) {
	if level < 0 || level > n.level {
		return
	}
	neighbors := n.neighbors[level]
	for i, id := range neighbors {
		if id == neighborID {
			n.neighbors[level] = append(neighbors[:i], neighbors[i+1:]...)
			return
		}
	}
}
