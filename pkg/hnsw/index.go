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

// distance computes the distance between two vectors using the configured metric
func (idx *Index) distance(a, b *types.Vector) float32 {
	return a.Distance(b, idx.config.DistanceMetric)
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
	currentNode := idx.nodes[current]
	if currentNode == nil {
		return ep
	}
	currentDist := idx.distance(query, currentNode.Vector())

	for {
		improved := false
		node := idx.nodes[current]
		if node == nil {
			break
		}
		for _, neighborID := range node.Neighbors(level) {
			neighborNode := idx.nodes[neighborID]
			if neighborNode == nil {
				continue
			}
			dist := idx.distance(query, neighborNode.Vector())
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
	// Validate entry point exists
	epNode := idx.nodes[ep]
	if epNode == nil {
		return nil
	}

	visited := make(map[uint64]bool)
	visited[ep] = true

	// candidates: nodes to explore (sorted by distance, closest first)
	// results: current best results (sorted by distance, furthest first for easy removal)
	candidates := []distNode{{id: ep, dist: idx.distance(query, epNode.Vector())}}
	results := []distNode{{id: ep, dist: candidates[0].dist}}

	for len(candidates) > 0 {
		// Get closest candidate
		closest := candidates[0]
		candidates = candidates[1:]

		// If closest candidate is further than furthest result, we're done
		if len(results) >= ef && closest.dist > results[len(results)-1].dist {
			break
		}

		// Get the current node
		currentNode := idx.nodes[closest.id]
		if currentNode == nil {
			continue
		}

		// Explore neighbors
		for _, neighborID := range currentNode.Neighbors(level) {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			// Skip if neighbor node doesn't exist (was deleted)
			neighborNode := idx.nodes[neighborID]
			if neighborNode == nil {
				continue
			}

			dist := idx.distance(query, neighborNode.Vector())

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

// selectNeighbors selects the M best neighbors using either simple or heuristic selection
// The heuristic considers both distance and diversity to create a better graph structure
func (idx *Index) selectNeighbors(query *types.Vector, candidates []uint64, m int) []uint64 {
	if len(candidates) <= m {
		return candidates
	}

	// Use heuristic selection if enabled in config
	if idx.config.UseHeuristic {
		return idx.selectNeighborsHeuristic(query, candidates, m, idx.config.ExtendCandidates)
	}

	return candidates[:m]
}

// selectNeighborsHeuristic implements the heuristic neighbor selection from the HNSW paper
// This provides better graph connectivity and recall compared to simple selection
// extendCandidates: if true, extend candidates with neighbors of candidates
func (idx *Index) selectNeighborsHeuristic(query *types.Vector, candidates []uint64, m int, extendCandidates bool) []uint64 {
	if len(candidates) == 0 {
		return nil
	}

	// Optionally extend candidates with their neighbors
	candidateSet := make(map[uint64]bool)
	for _, c := range candidates {
		candidateSet[c] = true
	}

	if extendCandidates {
		for _, c := range candidates {
			node := idx.nodes[c]
			if node == nil {
				continue
			}
			// Add neighbors at level 0
			for _, n := range node.Neighbors(0) {
				if !candidateSet[n] {
					candidateSet[n] = true
				}
			}
		}
	}

	// Build distance list for all candidates
	type candDist struct {
		id   uint64
		dist float32
	}
	workQueue := make([]candDist, 0, len(candidateSet))
	for id := range candidateSet {
		node := idx.nodes[id]
		if node == nil {
			continue
		}
		dist := idx.distance(query, node.Vector())
		workQueue = append(workQueue, candDist{id: id, dist: dist})
	}

	// Sort by distance (closest first)
	for i := 0; i < len(workQueue)-1; i++ {
		for j := i + 1; j < len(workQueue); j++ {
			if workQueue[j].dist < workQueue[i].dist {
				workQueue[i], workQueue[j] = workQueue[j], workQueue[i]
			}
		}
	}

	// Heuristic selection: keep candidates that are closer to query than to any selected neighbor
	selected := make([]uint64, 0, m)

	for _, cand := range workQueue {
		if len(selected) >= m {
			break
		}

		candNode := idx.nodes[cand.id]
		if candNode == nil {
			continue
		}

		// Check if this candidate is good (not too close to already selected neighbors)
		isGood := true
		for _, selID := range selected {
			selNode := idx.nodes[selID]
			if selNode == nil {
				continue
			}
			// Distance from candidate to selected neighbor
			distToNeighbor := idx.distance(candNode.Vector(), selNode.Vector())

			// If candidate is closer to an already selected neighbor than to query,
			// skip it to maintain diversity
			if distToNeighbor < cand.dist {
				isGood = false
				break
			}
		}

		if isGood {
			selected = append(selected, cand.id)
		}
	}

	// If we couldn't fill up to m using the heuristic, add remaining closest candidates
	if len(selected) < m {
		for _, cand := range workQueue {
			if len(selected) >= m {
				break
			}
			// Check if already selected
			alreadySelected := false
			for _, s := range selected {
				if s == cand.id {
					alreadySelected = true
					break
				}
			}
			if !alreadySelected {
				selected = append(selected, cand.id)
			}
		}
	}

	return selected
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
		nds = append(nds, nd{id: nid, dist: idx.distance(node.Vector(), neighborNode.Vector())})
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

// Delete removes a node from the index by its rowID
// Returns true if the node was found and deleted, false otherwise
func (idx *Index) Delete(rowID int64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Find the node with matching rowID
	var nodeToDelete *HNSWNode
	var nodeIDToDelete uint64
	for id, node := range idx.nodes {
		if node.rowID == rowID {
			nodeToDelete = node
			nodeIDToDelete = id
			break
		}
	}

	if nodeToDelete == nil {
		return false
	}

	// For each level the node exists at, rewire neighbors
	for level := 0; level <= nodeToDelete.level; level++ {
		neighbors := nodeToDelete.Neighbors(level)

		// Remove the deleted node from all its neighbors' lists
		for _, neighborID := range neighbors {
			neighbor := idx.nodes[neighborID]
			if neighbor == nil {
				continue
			}
			neighbor.RemoveNeighbor(level, nodeIDToDelete)

			// Optionally: reconnect orphaned neighbors to maintain graph connectivity
			// This is a simplified approach - a more sophisticated version would
			// use the heuristic neighbor selection
			idx.repairNeighborConnections(neighbor, level)
		}
	}

	// Remove the node
	delete(idx.nodes, nodeIDToDelete)

	// If we deleted the entry point, pick a new one
	if idx.entryPoint == nodeIDToDelete {
		idx.updateEntryPoint()
	}

	return true
}

// DeleteByNodeID removes a node from the index by its internal nodeID
// Returns true if the node was found and deleted, false otherwise
func (idx *Index) DeleteByNodeID(nodeID uint64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	nodeToDelete := idx.nodes[nodeID]
	if nodeToDelete == nil {
		return false
	}

	// For each level the node exists at, rewire neighbors
	for level := 0; level <= nodeToDelete.level; level++ {
		neighbors := nodeToDelete.Neighbors(level)

		// Remove the deleted node from all its neighbors' lists
		for _, neighborID := range neighbors {
			neighbor := idx.nodes[neighborID]
			if neighbor == nil {
				continue
			}
			neighbor.RemoveNeighbor(level, nodeID)

			// Repair connections for the neighbor
			idx.repairNeighborConnections(neighbor, level)
		}
	}

	// Remove the node
	delete(idx.nodes, nodeID)

	// If we deleted the entry point, pick a new one
	if idx.entryPoint == nodeID {
		idx.updateEntryPoint()
	}

	return true
}

// repairNeighborConnections attempts to maintain graph connectivity
// after a node is deleted by potentially adding new connections
func (idx *Index) repairNeighborConnections(node *HNSWNode, level int) {
	// If the node still has enough connections, no repair needed
	maxNeighbors := idx.config.M
	if level == 0 {
		maxNeighbors = idx.config.MMax0
	}

	currentNeighbors := node.Neighbors(level)
	if len(currentNeighbors) >= maxNeighbors/2 {
		return // Still has enough connections
	}

	// Try to add connections from neighbors' neighbors
	candidateSet := make(map[uint64]bool)
	for _, nid := range currentNeighbors {
		candidateSet[nid] = true
		neighbor := idx.nodes[nid]
		if neighbor == nil {
			continue
		}
		for _, nnid := range neighbor.Neighbors(level) {
			if nnid != node.id {
				candidateSet[nnid] = true
			}
		}
	}

	// Remove already connected nodes
	for _, nid := range currentNeighbors {
		delete(candidateSet, nid)
	}

	// Add new connections up to max
	candidates := make([]distNode, 0, len(candidateSet))
	for cid := range candidateSet {
		cnode := idx.nodes[cid]
		if cnode == nil || cnode.level < level {
			continue
		}
		dist := idx.distance(node.vector, cnode.vector)
		candidates = append(candidates, distNode{id: cid, dist: dist})
	}

	// Sort by distance
	for i := 0; i < len(candidates)-1; i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].dist < candidates[i].dist {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	// Add best candidates
	for _, c := range candidates {
		if len(node.Neighbors(level)) >= maxNeighbors {
			break
		}
		node.AddNeighbor(level, c.id)
		// Also add reverse connection
		cnode := idx.nodes[c.id]
		if cnode != nil {
			cnode.AddNeighbor(level, node.id)
		}
	}
}

// updateEntryPoint finds a new entry point after the current one is deleted
func (idx *Index) updateEntryPoint() {
	if len(idx.nodes) == 0 {
		idx.entryPoint = 0
		idx.maxLevel = 0
		return
	}

	// Find the node with the highest level
	maxLevel := -1
	var newEntryPoint uint64
	for id, node := range idx.nodes {
		if node.level > maxLevel {
			maxLevel = node.level
			newEntryPoint = id
		}
	}

	idx.entryPoint = newEntryPoint
	idx.maxLevel = maxLevel
}

// Update updates the vector for an existing rowID
// This is implemented as delete + insert for simplicity
// Returns true if the rowID was found and updated, false otherwise
func (idx *Index) Update(rowID int64, newVector *types.Vector) (bool, error) {
	if newVector.Dimension() != idx.config.Dimension {
		return false, ErrDimensionMismatch
	}

	// First delete the old entry
	if !idx.Delete(rowID) {
		return false, nil
	}

	// Then insert the new one
	if err := idx.Insert(rowID, newVector); err != nil {
		return false, err
	}

	return true, nil
}

// GetByRowID retrieves the vector for a given rowID
// Returns nil if not found
func (idx *Index) GetByRowID(rowID int64) *types.Vector {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	for _, node := range idx.nodes {
		if node.rowID == rowID {
			return node.vector
		}
	}
	return nil
}

// Contains checks if a rowID exists in the index
func (idx *Index) Contains(rowID int64) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	for _, node := range idx.nodes {
		if node.rowID == rowID {
			return true
		}
	}
	return false
}
