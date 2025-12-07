// pkg/hnsw/index.go
package hnsw

import (
	"errors"
	"math/rand"
	"sync"

	"tur/pkg/types"
)

var (
	ErrDimensionMismatch = errors.New("vector dimension mismatch")
)

// Index is an HNSW index for approximate nearest neighbor search
type Index struct {
	mu         sync.RWMutex
	config     Config
	nodes      map[uint64]*HNSWNode // nodeID -> node
	entryPoint uint64               // entry point node ID
	maxLevel   int                  // current maximum level
	nextID     uint64               // next node ID to assign
}

// NewIndex creates a new empty HNSW index
func NewIndex(config Config) *Index {
	return &Index{
		config: config,
		nodes:  make(map[uint64]*HNSWNode),
	}
}

// Len returns the number of nodes in the index
func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.nodes)
}

// Dimension returns the vector dimension
func (idx *Index) Dimension() int {
	return idx.config.Dimension
}

// Config returns the index configuration
func (idx *Index) Config() Config {
	return idx.config
}

// randomLevel generates a random level for a new node
func (idx *Index) randomLevel() int {
	level := 0
	for rand.Float64() < idx.config.ML && level < 32 {
		level++
	}
	return level
}

// Insert adds a vector to the index
func (idx *Index) Insert(rowID int64, vector *types.Vector) error {
	if vector.Dimension() != idx.config.Dimension {
		return ErrDimensionMismatch
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Assign node ID
	nodeID := idx.nextID
	idx.nextID++

	// Generate random level for this node
	level := idx.randomLevel()

	// Create node
	node := NewHNSWNode(nodeID, rowID, vector, level)

	// If this is the first node, it becomes the entry point
	if len(idx.nodes) == 0 {
		idx.nodes[nodeID] = node
		idx.entryPoint = nodeID
		idx.maxLevel = level
		return nil
	}

	// Find entry point and insert
	ep := idx.entryPoint
	currentLevel := idx.maxLevel

	// Phase 1: Traverse from top to node's level, finding closest node at each level
	for l := currentLevel; l > level; l-- {
		ep = idx.searchLayerClosest(vector, ep, l)
	}

	// Phase 2: Insert at each level from node's level down to 0
	for l := min(level, currentLevel); l >= 0; l-- {
		// Find neighbors at this level
		neighbors := idx.searchLayer(vector, ep, idx.config.EfConstruction, l)

		// Select M best neighbors
		maxNeighbors := idx.config.M
		if l == 0 {
			maxNeighbors = idx.config.MMax0
		}
		selectedNeighbors := idx.selectNeighbors(vector, neighbors, maxNeighbors)

		// Connect node to neighbors bidirectionally
		node.SetNeighbors(l, selectedNeighbors)
		for _, neighborID := range selectedNeighbors {
			neighbor := idx.nodes[neighborID]
			neighbor.AddNeighbor(l, nodeID)

			// Prune neighbor's connections if needed
			idx.pruneConnections(neighbor, l, maxNeighbors)
		}

		// Use closest neighbor as entry point for next level
		if len(selectedNeighbors) > 0 {
			ep = selectedNeighbors[0]
		}
	}

	// Store node
	idx.nodes[nodeID] = node

	// Update entry point if this node has higher level
	if level > idx.maxLevel {
		idx.entryPoint = nodeID
		idx.maxLevel = level
	}

	return nil
}

// searchLayerClosest finds the closest node to query at the given level
func (idx *Index) searchLayerClosest(query *types.Vector, ep uint64, level int) uint64 {
	current := ep
	currentDist := query.CosineDistance(idx.nodes[current].Vector())

	for {
		improved := false
		for _, neighborID := range idx.nodes[current].Neighbors(level) {
			dist := query.CosineDistance(idx.nodes[neighborID].Vector())
			if dist < currentDist {
				current = neighborID
				currentDist = dist
				improved = true
			}
		}
		if !improved {
			break
		}
	}

	return current
}

// searchLayer finds ef closest nodes to query at the given level
func (idx *Index) searchLayer(query *types.Vector, ep uint64, ef int, level int) []uint64 {
	visited := make(map[uint64]bool)
	visited[ep] = true

	// candidates: nodes to explore (sorted by distance, closest first)
	// results: current best results (sorted by distance, furthest first for easy removal)
	candidates := []distNode{{id: ep, dist: query.CosineDistance(idx.nodes[ep].Vector())}}
	results := []distNode{{id: ep, dist: candidates[0].dist}}

	for len(candidates) > 0 {
		// Get closest candidate
		closest := candidates[0]
		candidates = candidates[1:]

		// If closest candidate is further than furthest result, we're done
		if len(results) >= ef && closest.dist > results[len(results)-1].dist {
			break
		}

		// Explore neighbors
		for _, neighborID := range idx.nodes[closest.id].Neighbors(level) {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			dist := query.CosineDistance(idx.nodes[neighborID].Vector())

			// Add to results if better than worst result or not enough results yet
			if len(results) < ef || dist < results[len(results)-1].dist {
				results = insertSorted(results, distNode{id: neighborID, dist: dist})
				if len(results) > ef {
					results = results[:ef]
				}
				candidates = insertSorted(candidates, distNode{id: neighborID, dist: dist})
			}
		}
	}

	// Extract IDs from results
	ids := make([]uint64, len(results))
	for i, r := range results {
		ids[i] = r.id
	}
	return ids
}

// selectNeighbors selects the M best neighbors (simple heuristic)
func (idx *Index) selectNeighbors(query *types.Vector, candidates []uint64, m int) []uint64 {
	if len(candidates) <= m {
		return candidates
	}
	return candidates[:m]
}

// pruneConnections ensures a node doesn't exceed max connections
func (idx *Index) pruneConnections(node *HNSWNode, level int, maxConnections int) {
	neighbors := node.Neighbors(level)
	if len(neighbors) <= maxConnections {
		return
	}

	// Keep only the closest maxConnections neighbors
	type nd struct {
		id   uint64
		dist float32
	}
	nds := make([]nd, 0, len(neighbors))
	for _, nid := range neighbors {
		// Skip if node doesn't exist yet (happens during insertion)
		neighborNode := idx.nodes[nid]
		if neighborNode == nil {
			continue
		}
		nds = append(nds, nd{id: nid, dist: node.Vector().CosineDistance(neighborNode.Vector())})
	}

	// Sort by distance
	for i := 0; i < len(nds)-1; i++ {
		for j := i + 1; j < len(nds); j++ {
			if nds[j].dist < nds[i].dist {
				nds[i], nds[j] = nds[j], nds[i]
			}
		}
	}

	// Keep only maxConnections
	numToKeep := maxConnections
	if len(nds) < numToKeep {
		numToKeep = len(nds)
	}
	selected := make([]uint64, numToKeep)
	for i := 0; i < numToKeep; i++ {
		selected[i] = nds[i].id
	}
	node.SetNeighbors(level, selected)
}

// distNode pairs a node ID with its distance
type distNode struct {
	id   uint64
	dist float32
}

// insertSorted inserts a distNode into a sorted slice (by distance, ascending)
func insertSorted(slice []distNode, node distNode) []distNode {
	i := 0
	for i < len(slice) && slice[i].dist < node.dist {
		i++
	}
	slice = append(slice, distNode{})
	copy(slice[i+1:], slice[i:])
	slice[i] = node
	return slice
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
