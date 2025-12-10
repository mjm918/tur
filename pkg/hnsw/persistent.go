// pkg/hnsw/persistent.go
package hnsw

import (
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"sync"

	"tur/pkg/pager"
	"tur/pkg/types"
)

var (
	ErrInvalidMetaPage = errors.New("invalid HNSW meta page")
	ErrNodeNotFound    = errors.New("HNSW node not found")
)

// PersistentIndex is an HNSW index backed by the pager for disk persistence
type PersistentIndex struct {
	mu       sync.RWMutex
	pager    *pager.Pager
	metaPage uint32 // Page number of the metadata page

	// Cached metadata (loaded from disk)
	config     Config
	entryPoint uint64
	maxLevel   int
	nextID     uint64
	nodeCount  uint64

	// nodeID -> pageNo mapping (loaded lazily or kept in memory)
	nodePages map[uint64]uint32

	// In-memory cache of recently accessed nodes
	nodeCache map[uint64]*HNSWNode
}

// Meta page layout (stored on PageTypeHNSWMeta page):
// [0]     PageType (1 byte) = 0x11
// [1-4]   Magic (4 bytes)
// [5-8]   Version (4 bytes)
// [9-12]  M (4 bytes)
// [13-16] MMax0 (4 bytes)
// [17-20] EfConstruction (4 bytes)
// [21-24] EfSearch (4 bytes)
// [25-28] Dimension (4 bytes)
// [29-36] ML (8 bytes, float64)
// [37-44] EntryPoint (8 bytes)
// [45-48] MaxLevel (4 bytes)
// [49-56] NextID (8 bytes)
// [57-64] NodeCount (8 bytes)
// [65]    Flags (1 byte)
// [66-71] Reserved
// [72...] Node page directory (nodeID -> pageNo mappings)

const (
	metaHeaderSize    = 72
	nodePageEntrySize = 12 // 8 bytes nodeID + 4 bytes pageNo
)

// CreatePersistent creates a new persistent HNSW index
func CreatePersistent(p *pager.Pager, config Config) (*PersistentIndex, error) {
	// Allocate meta page
	metaPage, err := p.Allocate()
	if err != nil {
		return nil, err
	}
	defer p.Release(metaPage)

	metaPage.SetType(pager.PageTypeHNSWMeta)

	idx := &PersistentIndex{
		pager:     p,
		metaPage:  metaPage.PageNo(),
		config:    config,
		nodePages: make(map[uint64]uint32),
		nodeCache: make(map[uint64]*HNSWNode),
	}

	// Write initial metadata
	if err := idx.writeMeta(); err != nil {
		return nil, err
	}

	return idx, nil
}

// OpenPersistent opens an existing persistent HNSW index
func OpenPersistent(p *pager.Pager, metaPageNo uint32) (*PersistentIndex, error) {
	idx := &PersistentIndex{
		pager:     p,
		metaPage:  metaPageNo,
		nodePages: make(map[uint64]uint32),
		nodeCache: make(map[uint64]*HNSWNode),
	}

	// Load metadata
	if err := idx.loadMeta(); err != nil {
		return nil, err
	}

	return idx, nil
}

// MetaPage returns the meta page number (useful for reopening)
func (idx *PersistentIndex) MetaPage() uint32 {
	return idx.metaPage
}

// distance computes the distance between two vectors using the configured metric
func (idx *PersistentIndex) distance(a, b *types.Vector) float32 {
	return a.Distance(b, idx.config.DistanceMetric)
}

// writeMeta writes the metadata to the meta page
func (idx *PersistentIndex) writeMeta() error {
	page, err := idx.pager.Get(idx.metaPage)
	if err != nil {
		return err
	}
	defer idx.pager.Release(page)

	data := page.Data()

	// Write header
	data[0] = byte(pager.PageTypeHNSWMeta)
	binary.LittleEndian.PutUint32(data[1:5], hnswMagic)
	binary.LittleEndian.PutUint32(data[5:9], hnswVersion)
	binary.LittleEndian.PutUint32(data[9:13], uint32(idx.config.M))
	binary.LittleEndian.PutUint32(data[13:17], uint32(idx.config.MMax0))
	binary.LittleEndian.PutUint32(data[17:21], uint32(idx.config.EfConstruction))
	binary.LittleEndian.PutUint32(data[21:25], uint32(idx.config.EfSearch))
	binary.LittleEndian.PutUint32(data[25:29], uint32(idx.config.Dimension))
	binary.LittleEndian.PutUint64(data[29:37], math.Float64bits(idx.config.ML))
	binary.LittleEndian.PutUint64(data[37:45], idx.entryPoint)
	binary.LittleEndian.PutUint32(data[45:49], uint32(idx.maxLevel))
	binary.LittleEndian.PutUint64(data[49:57], idx.nextID)
	binary.LittleEndian.PutUint64(data[57:65], idx.nodeCount)

	// Flags
	var flags byte
	if idx.config.UseHeuristic {
		flags |= 0x01
	}
	if idx.config.ExtendCandidates {
		flags |= 0x02
	}
	data[65] = flags

	// Write node page directory
	offset := metaHeaderSize
	pageSize := idx.pager.PageSize()
	for nodeID, pageNo := range idx.nodePages {
		if offset+nodePageEntrySize > pageSize {
			// TODO: Implement overflow pages for large indexes
			break
		}
		binary.LittleEndian.PutUint64(data[offset:offset+8], nodeID)
		binary.LittleEndian.PutUint32(data[offset+8:offset+12], pageNo)
		offset += nodePageEntrySize
	}

	page.SetDirty(true)
	return nil
}

// loadMeta reads the metadata from the meta page
func (idx *PersistentIndex) loadMeta() error {
	page, err := idx.pager.Get(idx.metaPage)
	if err != nil {
		return err
	}
	defer idx.pager.Release(page)

	data := page.Data()

	// Validate page type
	if pager.PageType(data[0]) != pager.PageTypeHNSWMeta {
		return ErrInvalidMetaPage
	}

	// Validate magic
	magic := binary.LittleEndian.Uint32(data[1:5])
	if magic != hnswMagic {
		return ErrInvalidMagic
	}

	// Read config
	flags := data[65]
	idx.config = Config{
		M:                int(binary.LittleEndian.Uint32(data[9:13])),
		MMax0:            int(binary.LittleEndian.Uint32(data[13:17])),
		EfConstruction:   int(binary.LittleEndian.Uint32(data[17:21])),
		EfSearch:         int(binary.LittleEndian.Uint32(data[21:25])),
		Dimension:        int(binary.LittleEndian.Uint32(data[25:29])),
		ML:               math.Float64frombits(binary.LittleEndian.Uint64(data[29:37])),
		UseHeuristic:     flags&0x01 != 0,
		ExtendCandidates: flags&0x02 != 0,
	}

	idx.entryPoint = binary.LittleEndian.Uint64(data[37:45])
	idx.maxLevel = int(binary.LittleEndian.Uint32(data[45:49]))
	idx.nextID = binary.LittleEndian.Uint64(data[49:57])
	idx.nodeCount = binary.LittleEndian.Uint64(data[57:65])

	// Read node page directory
	offset := metaHeaderSize
	pageSize := idx.pager.PageSize()
	for i := uint64(0); i < idx.nodeCount && offset+nodePageEntrySize <= pageSize; i++ {
		nodeID := binary.LittleEndian.Uint64(data[offset : offset+8])
		pageNo := binary.LittleEndian.Uint32(data[offset+8 : offset+12])
		idx.nodePages[nodeID] = pageNo
		offset += nodePageEntrySize
	}

	return nil
}

// Len returns the number of nodes
func (idx *PersistentIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return int(idx.nodeCount)
}

// Dimension returns the vector dimension
func (idx *PersistentIndex) Dimension() int {
	return idx.config.Dimension
}

// Config returns the index configuration
func (idx *PersistentIndex) Config() Config {
	return idx.config
}

// Insert adds a vector to the persistent index
func (idx *PersistentIndex) Insert(rowID int64, vector *types.Vector) error {
	if vector.Dimension() != idx.config.Dimension {
		return ErrDimensionMismatch
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Assign node ID
	nodeID := idx.nextID
	idx.nextID++

	// Generate random level
	level := idx.randomLevel()

	// Create node
	node := NewHNSWNode(nodeID, rowID, vector, level)

	// Allocate page for this node
	// NOTE: Allocation may trigger mmap regrowth which invalidates cached pages
	nodePage, err := idx.pager.Allocate()
	if err != nil {
		return err
	}
	nodePageNo := nodePage.PageNo()
	idx.pager.Release(nodePage)

	// Clear node cache since allocation may have triggered mmap regrowth
	// This ensures we re-read nodes from fresh page data
	idx.nodeCache = make(map[uint64]*HNSWNode)

	// Store mapping
	idx.nodePages[nodeID] = nodePageNo
	idx.nodeCache[nodeID] = node

	// If this is the first node
	if idx.nodeCount == 0 {
		idx.entryPoint = nodeID
		idx.maxLevel = level
		idx.nodeCount = 1
		if err := idx.writeNode(node, nodePageNo); err != nil {
			return err
		}
		return idx.writeMeta()
	}

	// Find entry point and insert using HNSW algorithm
	ep := idx.entryPoint
	currentLevel := idx.maxLevel

	// Phase 1: Traverse from top to node's level
	for l := currentLevel; l > level; l-- {
		ep = idx.searchLayerClosest(vector, ep, l)
	}

	// Phase 2: Insert at each level
	for l := min(level, currentLevel); l >= 0; l-- {
		neighbors := idx.searchLayer(vector, ep, idx.config.EfConstruction, l)

		maxNeighbors := idx.config.M
		if l == 0 {
			maxNeighbors = idx.config.MMax0
		}
		selectedNeighbors := idx.selectNeighbors(vector, neighbors, maxNeighbors)

		node.SetNeighbors(l, selectedNeighbors)
		for _, neighborID := range selectedNeighbors {
			neighbor := idx.getNode(neighborID)
			if neighbor != nil {
				neighbor.AddNeighbor(l, nodeID)
				idx.pruneConnections(neighbor, l, maxNeighbors)
				// Mark neighbor as dirty (needs to be written)
				if neighborPageNo, ok := idx.nodePages[neighborID]; ok {
					idx.writeNode(neighbor, neighborPageNo)
				}
			}
		}

		if len(selectedNeighbors) > 0 {
			ep = selectedNeighbors[0]
		}
	}

	idx.nodeCount++

	// Update entry point if needed
	if level > idx.maxLevel {
		idx.entryPoint = nodeID
		idx.maxLevel = level
	}

	// Write node to disk
	if err := idx.writeNode(node, nodePageNo); err != nil {
		return err
	}

	// Update metadata
	return idx.writeMeta()
}

// writeNode writes a node to its page
func (idx *PersistentIndex) writeNode(node *HNSWNode, pageNo uint32) error {
	page, err := idx.pager.Get(pageNo)
	if err != nil {
		return err
	}
	defer idx.pager.Release(page)

	data := page.Data()
	pageSize := len(data)
	data[0] = byte(pager.PageTypeHNSWNode)

	offset := 1

	// Check we have enough space for header (1 + 8 + 8 + 4 + 4 = 25 bytes minimum)
	if offset+24 > pageSize {
		return errors.New("page too small for node header")
	}

	// Write node header
	binary.LittleEndian.PutUint64(data[offset:offset+8], node.id)
	offset += 8
	binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(node.rowID))
	offset += 8
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(node.level))
	offset += 4

	// Write vector
	vecBytes := node.vector.ToBytes()
	if offset+4+len(vecBytes) > pageSize {
		return errors.New("page too small for vector data")
	}
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(vecBytes)))
	offset += 4
	copy(data[offset:offset+len(vecBytes)], vecBytes)
	offset += len(vecBytes)

	// Write neighbors for each level
	for l := 0; l <= node.level; l++ {
		neighbors := node.Neighbors(l)
		if offset+4 > pageSize {
			return errors.New("page too small for neighbor count")
		}
		binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(neighbors)))
		offset += 4
		for _, nid := range neighbors {
			if offset+8 > pageSize {
				return errors.New("page too small for neighbor data")
			}
			binary.LittleEndian.PutUint64(data[offset:offset+8], nid)
			offset += 8
		}
	}

	page.SetDirty(true)
	return nil
}

// readNode reads a node from its page
func (idx *PersistentIndex) readNode(pageNo uint32) (*HNSWNode, error) {
	page, err := idx.pager.Get(pageNo)
	if err != nil {
		return nil, err
	}
	defer idx.pager.Release(page)

	data := page.Data()

	// Validate page type
	if pager.PageType(data[0]) != pager.PageTypeHNSWNode {
		return nil, ErrNodeNotFound
	}

	offset := 1

	// Read node header
	nodeID := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8
	rowID := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8
	level := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Read vector
	vecSize := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	vector, err := types.VectorFromBytes(data[offset : offset+int(vecSize)])
	if err != nil {
		return nil, err
	}
	offset += int(vecSize)

	// Create node
	node := &HNSWNode{
		id:        nodeID,
		rowID:     rowID,
		vector:    vector,
		level:     level,
		neighbors: make([][]uint64, level+1),
	}

	// Read neighbors
	for l := 0; l <= level; l++ {
		count := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		neighbors := make([]uint64, count)
		for i := uint32(0); i < count; i++ {
			neighbors[i] = binary.LittleEndian.Uint64(data[offset : offset+8])
			offset += 8
		}
		node.neighbors[l] = neighbors
	}

	return node, nil
}

// getNode gets a node, first checking cache, then disk
func (idx *PersistentIndex) getNode(nodeID uint64) *HNSWNode {
	// Check cache first
	if node, ok := idx.nodeCache[nodeID]; ok {
		return node
	}

	// Load from disk
	pageNo, ok := idx.nodePages[nodeID]
	if !ok {
		return nil
	}

	node, err := idx.readNode(pageNo)
	if err != nil {
		return nil
	}

	// Cache it
	idx.nodeCache[nodeID] = node
	return node
}

// randomLevel generates a random level for a new node
func (idx *PersistentIndex) randomLevel() int {
	level := 0
	for randFloat() < idx.config.ML && level < 32 {
		level++
	}
	return level
}

// searchLayerClosest finds the closest node at the given level
func (idx *PersistentIndex) searchLayerClosest(query *types.Vector, ep uint64, level int) uint64 {
	current := ep
	currentNode := idx.getNode(current)
	if currentNode == nil {
		return ep
	}
	currentDist := idx.distance(query, currentNode.Vector())

	for {
		improved := false
		node := idx.getNode(current)
		if node == nil {
			break
		}
		for _, neighborID := range node.Neighbors(level) {
			neighborNode := idx.getNode(neighborID)
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

// searchLayer finds ef closest nodes at the given level
func (idx *PersistentIndex) searchLayer(query *types.Vector, ep uint64, ef int, level int) []uint64 {
	epNode := idx.getNode(ep)
	if epNode == nil {
		return nil
	}

	visited := make(map[uint64]bool)
	visited[ep] = true

	candidates := []distNode{{id: ep, dist: idx.distance(query, epNode.Vector())}}
	results := []distNode{{id: ep, dist: candidates[0].dist}}

	for len(candidates) > 0 {
		closest := candidates[0]
		candidates = candidates[1:]

		if len(results) >= ef && closest.dist > results[len(results)-1].dist {
			break
		}

		currentNode := idx.getNode(closest.id)
		if currentNode == nil {
			continue
		}

		for _, neighborID := range currentNode.Neighbors(level) {
			if visited[neighborID] {
				continue
			}
			visited[neighborID] = true

			neighborNode := idx.getNode(neighborID)
			if neighborNode == nil {
				continue
			}

			dist := idx.distance(query, neighborNode.Vector())

			if len(results) < ef || dist < results[len(results)-1].dist {
				results = insertSorted(results, distNode{id: neighborID, dist: dist})
				if len(results) > ef {
					results = results[:ef]
				}
				candidates = insertSorted(candidates, distNode{id: neighborID, dist: dist})
			}
		}
	}

	ids := make([]uint64, len(results))
	for i, r := range results {
		ids[i] = r.id
	}
	return ids
}

// selectNeighbors selects the M best neighbors
func (idx *PersistentIndex) selectNeighbors(query *types.Vector, candidates []uint64, m int) []uint64 {
	if len(candidates) <= m {
		return candidates
	}

	if idx.config.UseHeuristic {
		return idx.selectNeighborsHeuristic(query, candidates, m, idx.config.ExtendCandidates)
	}

	return candidates[:m]
}

// selectNeighborsHeuristic implements heuristic neighbor selection
func (idx *PersistentIndex) selectNeighborsHeuristic(query *types.Vector, candidates []uint64, m int, extendCandidates bool) []uint64 {
	if len(candidates) == 0 {
		return nil
	}

	candidateSet := make(map[uint64]bool)
	for _, c := range candidates {
		candidateSet[c] = true
	}

	if extendCandidates {
		for _, c := range candidates {
			node := idx.getNode(c)
			if node == nil {
				continue
			}
			for _, n := range node.Neighbors(0) {
				candidateSet[n] = true
			}
		}
	}

	type candDist struct {
		id   uint64
		dist float32
	}
	workQueue := make([]candDist, 0, len(candidateSet))
	for id := range candidateSet {
		node := idx.getNode(id)
		if node == nil {
			continue
		}
		dist := idx.distance(query, node.Vector())
		workQueue = append(workQueue, candDist{id: id, dist: dist})
	}

	// Sort by distance
	for i := 0; i < len(workQueue)-1; i++ {
		for j := i + 1; j < len(workQueue); j++ {
			if workQueue[j].dist < workQueue[i].dist {
				workQueue[i], workQueue[j] = workQueue[j], workQueue[i]
			}
		}
	}

	selected := make([]uint64, 0, m)

	for _, cand := range workQueue {
		if len(selected) >= m {
			break
		}

		candNode := idx.getNode(cand.id)
		if candNode == nil {
			continue
		}

		isGood := true
		for _, selID := range selected {
			selNode := idx.getNode(selID)
			if selNode == nil {
				continue
			}
			distToNeighbor := idx.distance(candNode.Vector(), selNode.Vector())
			if distToNeighbor < cand.dist {
				isGood = false
				break
			}
		}

		if isGood {
			selected = append(selected, cand.id)
		}
	}

	if len(selected) < m {
		for _, cand := range workQueue {
			if len(selected) >= m {
				break
			}
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
func (idx *PersistentIndex) pruneConnections(node *HNSWNode, level int, maxConnections int) {
	neighbors := node.Neighbors(level)
	if len(neighbors) <= maxConnections {
		return
	}

	type nd struct {
		id   uint64
		dist float32
	}
	nds := make([]nd, 0, len(neighbors))
	for _, nid := range neighbors {
		neighborNode := idx.getNode(nid)
		if neighborNode == nil {
			continue
		}
		nds = append(nds, nd{id: nid, dist: idx.distance(node.Vector(), neighborNode.Vector())})
	}

	for i := 0; i < len(nds)-1; i++ {
		for j := i + 1; j < len(nds); j++ {
			if nds[j].dist < nds[i].dist {
				nds[i], nds[j] = nds[j], nds[i]
			}
		}
	}

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

// SearchKNN finds the k nearest neighbors
func (idx *PersistentIndex) SearchKNN(query *types.Vector, k int) ([]SearchResult, error) {
	return idx.SearchKNNWithEf(query, k, idx.config.EfSearch)
}

// SearchKNNWithEf finds the k nearest neighbors with custom ef
func (idx *PersistentIndex) SearchKNNWithEf(query *types.Vector, k int, ef int) ([]SearchResult, error) {
	if query.Dimension() != idx.config.Dimension {
		return nil, ErrDimensionMismatch
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.nodeCount == 0 {
		return []SearchResult{}, nil
	}

	ep := idx.entryPoint

	for l := idx.maxLevel; l > 0; l-- {
		ep = idx.searchLayerClosest(query, ep, l)
	}

	candidates := idx.searchLayer(query, ep, ef, 0)

	if len(candidates) > k {
		candidates = candidates[:k]
	}

	results := make([]SearchResult, 0, len(candidates))
	for _, nodeID := range candidates {
		node := idx.getNode(nodeID)
		if node == nil {
			continue
		}
		results = append(results, SearchResult{
			RowID:    node.RowID(),
			Distance: idx.distance(query, node.Vector()),
		})
	}

	// Sort by distance
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Distance < results[i].Distance {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	return results, nil
}

// Delete removes a node by rowID
func (idx *PersistentIndex) Delete(rowID int64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Find node with matching rowID
	var nodeToDelete *HNSWNode
	var nodeIDToDelete uint64
	for nodeID := range idx.nodePages {
		node := idx.getNode(nodeID)
		if node != nil && node.rowID == rowID {
			nodeToDelete = node
			nodeIDToDelete = nodeID
			break
		}
	}

	if nodeToDelete == nil {
		return false
	}

	// Remove from neighbors
	for level := 0; level <= nodeToDelete.level; level++ {
		neighbors := nodeToDelete.Neighbors(level)
		for _, neighborID := range neighbors {
			neighbor := idx.getNode(neighborID)
			if neighbor == nil {
				continue
			}
			neighbor.RemoveNeighbor(level, nodeIDToDelete)
			if pageNo, ok := idx.nodePages[neighborID]; ok {
				idx.writeNode(neighbor, pageNo)
			}
		}
	}

	// Remove from maps
	delete(idx.nodePages, nodeIDToDelete)
	delete(idx.nodeCache, nodeIDToDelete)
	idx.nodeCount--

	// Update entry point if needed
	if idx.entryPoint == nodeIDToDelete {
		idx.updateEntryPoint()
	}

	// TODO: Mark the node page as free for reuse

	return idx.writeMeta() == nil
}

// updateEntryPoint finds a new entry point after deletion
func (idx *PersistentIndex) updateEntryPoint() {
	if idx.nodeCount == 0 {
		idx.entryPoint = 0
		idx.maxLevel = 0
		return
	}

	maxLevel := -1
	var newEntryPoint uint64
	for nodeID := range idx.nodePages {
		node := idx.getNode(nodeID)
		if node != nil && node.level > maxLevel {
			maxLevel = node.level
			newEntryPoint = nodeID
		}
	}

	idx.entryPoint = newEntryPoint
	idx.maxLevel = maxLevel
}

// Sync flushes changes to disk
func (idx *PersistentIndex) Sync() error {
	return idx.pager.Sync()
}

// Update updates the vector for an existing rowID
// Returns true if the rowID was found and updated, false otherwise
func (idx *PersistentIndex) Update(rowID int64, newVector *types.Vector) (bool, error) {
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
func (idx *PersistentIndex) GetByRowID(rowID int64) *types.Vector {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	for nodeID := range idx.nodePages {
		node := idx.getNode(nodeID)
		if node != nil && node.rowID == rowID {
			return node.vector
		}
	}
	return nil
}

// Contains checks if a rowID exists in the index
func (idx *PersistentIndex) Contains(rowID int64) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	for nodeID := range idx.nodePages {
		node := idx.getNode(nodeID)
		if node != nil && node.rowID == rowID {
			return true
		}
	}
	return false
}

// randFloat returns a random float64 between 0 and 1
func randFloat() float64 {
	return rand.Float64()
}
