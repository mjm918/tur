// pkg/hnsw/incremental.go
package hnsw

import (
	"sync"
	"time"

	"tur/pkg/types"
)

// OpType represents the type of operation in the change log
type OpType int

const (
	OpInsert OpType = iota
	OpDelete
	OpUpdate
)

// Operation represents a single operation in the change log
type Operation struct {
	Seq       uint64        // Sequence number (monotonically increasing)
	Type      OpType        // Operation type
	NodeID    uint64        // Node ID affected
	RowID     int64         // Row ID affected
	Vector    *types.Vector // Vector for insert/update (new vector for update)
	OldVector *types.Vector // Old vector for update operations
}

// ChangeLog tracks modifications to an HNSW index
// It maintains a log of all insert, delete, and update operations
// that can be applied incrementally or merged into the main index
type ChangeLog struct {
	mu         sync.RWMutex
	operations []Operation
	nextSeq    uint64
}

// NewChangeLog creates a new empty change log
func NewChangeLog() *ChangeLog {
	return &ChangeLog{
		operations: make([]Operation, 0),
		nextSeq:    1,
	}
}

// RecordInsert records an insert operation
func (cl *ChangeLog) RecordInsert(nodeID uint64, rowID int64, vector *types.Vector) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	op := Operation{
		Seq:    cl.nextSeq,
		Type:   OpInsert,
		NodeID: nodeID,
		RowID:  rowID,
		Vector: vector,
	}
	cl.operations = append(cl.operations, op)
	cl.nextSeq++
}

// RecordDelete records a delete operation
func (cl *ChangeLog) RecordDelete(nodeID uint64, rowID int64) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	op := Operation{
		Seq:    cl.nextSeq,
		Type:   OpDelete,
		NodeID: nodeID,
		RowID:  rowID,
	}
	cl.operations = append(cl.operations, op)
	cl.nextSeq++
}

// RecordUpdate records an update operation
func (cl *ChangeLog) RecordUpdate(nodeID uint64, rowID int64, oldVector, newVector *types.Vector) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	op := Operation{
		Seq:       cl.nextSeq,
		Type:      OpUpdate,
		NodeID:    nodeID,
		RowID:     rowID,
		Vector:    newVector,
		OldVector: oldVector,
	}
	cl.operations = append(cl.operations, op)
	cl.nextSeq++
}

// Operations returns all recorded operations
func (cl *ChangeLog) Operations() []Operation {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	// Return a copy to prevent external modification
	result := make([]Operation, len(cl.operations))
	copy(result, cl.operations)
	return result
}

// Size returns the number of operations in the log
func (cl *ChangeLog) Size() int {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return len(cl.operations)
}

// Clear removes all operations from the log
func (cl *ChangeLog) Clear() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.operations = cl.operations[:0]
}

// LastSeq returns the sequence number of the last operation
func (cl *ChangeLog) LastSeq() uint64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	if len(cl.operations) == 0 {
		return 0
	}
	return cl.operations[len(cl.operations)-1].Seq
}

// OperationsSince returns all operations with sequence number > seq
func (cl *ChangeLog) OperationsSince(seq uint64) []Operation {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	var result []Operation
	for _, op := range cl.operations {
		if op.Seq > seq {
			result = append(result, op)
		}
	}
	return result
}

// rowState tracks the net state of operations on a single row
type rowState struct {
	exists      bool          // Does the row exist after all operations?
	finalVector *types.Vector // The final vector value (if exists)
	rowID       int64
	nodeID      uint64
}

// Compact removes redundant operations by computing net changes per row
// This reduces memory usage by eliminating insert-delete pairs and
// collapsing multiple updates into single operations
func (cl *ChangeLog) Compact() {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if len(cl.operations) == 0 {
		return
	}

	// Track net state per rowID
	states := make(map[int64]*rowState)

	for _, op := range cl.operations {
		state, exists := states[op.RowID]
		if !exists {
			state = &rowState{
				rowID:  op.RowID,
				nodeID: op.NodeID,
			}
			states[op.RowID] = state
		}

		switch op.Type {
		case OpInsert:
			state.exists = true
			state.finalVector = op.Vector
			state.nodeID = op.NodeID
		case OpDelete:
			state.exists = false
			state.finalVector = nil
		case OpUpdate:
			if state.exists {
				state.finalVector = op.Vector
			}
		}
	}

	// Build compacted operations list
	compacted := make([]Operation, 0)
	for _, state := range states {
		if state.exists && state.finalVector != nil {
			compacted = append(compacted, Operation{
				Seq:    cl.nextSeq,
				Type:   OpInsert,
				NodeID: state.nodeID,
				RowID:  state.rowID,
				Vector: state.finalVector,
			})
			cl.nextSeq++
		}
	}

	cl.operations = compacted
}

// EstimateMemoryUsage returns an estimate of memory used by the change log in bytes
func (cl *ChangeLog) EstimateMemoryUsage() int64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	// Base struct size
	var total int64 = 64 // approximate base overhead

	// Size per operation (without vectors)
	const opBaseSize = 48 // Seq(8) + Type(8) + NodeID(8) + RowID(8) + 2 pointers(16)

	for _, op := range cl.operations {
		total += opBaseSize
		if op.Vector != nil {
			total += int64(op.Vector.Dimension() * 4) // float32 = 4 bytes
		}
		if op.OldVector != nil {
			total += int64(op.OldVector.Dimension() * 4)
		}
	}

	return total
}

// TruncateOlderThan removes operations with sequence number <= seq
// This is useful after checkpoint persistence to free memory
func (cl *ChangeLog) TruncateOlderThan(seq uint64) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	var remaining []Operation
	for _, op := range cl.operations {
		if op.Seq > seq {
			remaining = append(remaining, op)
		}
	}
	cl.operations = remaining
}

// IndexSnapshot represents a point-in-time snapshot of the index state
type IndexSnapshot struct {
	Version   uint64 // Version at time of snapshot (based on last seq)
	NodeCount int    // Number of nodes at snapshot time
}

// Checkpoint represents a named point-in-time state that can be referenced
type Checkpoint struct {
	Version   uint64 // Version at time of checkpoint
	Timestamp int64  // Unix timestamp when checkpoint was created
	NodeCount int    // Number of nodes at checkpoint time
}

// IncrementalIndex wraps an HNSW Index with change tracking for incremental updates
// It maintains a change log that can be used to merge deltas into the main index
// without requiring a full rebuild
type IncrementalIndex struct {
	*Index                   // Embedded Index for all HNSW operations
	changeLog   *ChangeLog   // Log of all modifications
	version     uint64       // Current version (incremented on each change)
	checkpoints []Checkpoint // History of checkpoints
}

// NewIncrementalIndex creates a new incremental index with change tracking
func NewIncrementalIndex(config Config) *IncrementalIndex {
	return &IncrementalIndex{
		Index:       NewIndex(config),
		changeLog:   NewChangeLog(),
		version:     0,
		checkpoints: make([]Checkpoint, 0),
	}
}

// Insert adds a vector to the index and records the operation
func (idx *IncrementalIndex) Insert(rowID int64, vector *types.Vector) error {
	idx.mu.Lock()
	// Get the node ID that will be assigned
	nodeID := idx.nextID
	idx.mu.Unlock()

	// Perform the actual insert
	if err := idx.Index.Insert(rowID, vector); err != nil {
		return err
	}

	// Record the operation in the change log
	idx.changeLog.RecordInsert(nodeID, rowID, vector)
	idx.version++

	return nil
}

// Delete removes a vector from the index and records the operation
func (idx *IncrementalIndex) Delete(rowID int64) bool {
	// Find the node ID before deletion
	idx.mu.RLock()
	var nodeID uint64
	for id, node := range idx.nodes {
		if node.rowID == rowID {
			nodeID = id
			break
		}
	}
	idx.mu.RUnlock()

	// Perform the actual delete
	if !idx.Index.Delete(rowID) {
		return false
	}

	// Record the operation in the change log
	idx.changeLog.RecordDelete(nodeID, rowID)
	idx.version++

	return true
}

// Update updates a vector in the index and records the operation
func (idx *IncrementalIndex) Update(rowID int64, newVector *types.Vector) (bool, error) {
	// Get old vector before update
	oldVector := idx.Index.GetByRowID(rowID)
	if oldVector == nil {
		return false, nil
	}

	// Find the node ID
	idx.mu.RLock()
	var nodeID uint64
	for id, node := range idx.nodes {
		if node.rowID == rowID {
			nodeID = id
			break
		}
	}
	idx.mu.RUnlock()

	// Perform the actual update
	updated, err := idx.Index.Update(rowID, newVector)
	if err != nil {
		return false, err
	}
	if !updated {
		return false, nil
	}

	// Record the operation in the change log
	idx.changeLog.RecordUpdate(nodeID, rowID, oldVector, newVector)
	idx.version++

	return true, nil
}

// PendingChanges returns the number of unmerged changes
func (idx *IncrementalIndex) PendingChanges() int {
	return idx.changeLog.Size()
}

// LastSeq returns the last sequence number from the change log
func (idx *IncrementalIndex) LastSeq() uint64 {
	return idx.changeLog.LastSeq()
}

// OperationsSince returns all operations since the given sequence number
func (idx *IncrementalIndex) OperationsSince(seq uint64) []Operation {
	return idx.changeLog.OperationsSince(seq)
}

// Snapshot creates a point-in-time snapshot of the index state
func (idx *IncrementalIndex) Snapshot() IndexSnapshot {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return IndexSnapshot{
		Version:   idx.changeLog.LastSeq(),
		NodeCount: len(idx.nodes),
	}
}

// ApplyDelta applies a set of operations to the index
// This is used to merge changes from another index or replay a log
func (idx *IncrementalIndex) ApplyDelta(operations []Operation) error {
	for _, op := range operations {
		switch op.Type {
		case OpInsert:
			if op.Vector == nil {
				continue
			}
			if err := idx.Index.Insert(op.RowID, op.Vector); err != nil {
				return err
			}
		case OpDelete:
			idx.Index.Delete(op.RowID)
		case OpUpdate:
			if op.Vector == nil {
				continue
			}
			idx.Index.Update(op.RowID, op.Vector)
		}
	}
	return nil
}

// ClearPendingChanges clears the change log (typically after merging)
func (idx *IncrementalIndex) ClearPendingChanges() {
	idx.changeLog.Clear()
}

// SearchKNN finds the k nearest neighbors
func (idx *IncrementalIndex) SearchKNN(query *types.Vector, k int) ([]SearchResult, error) {
	return idx.Index.SearchKNN(query, k)
}

// Version returns the current version of the index
func (idx *IncrementalIndex) Version() uint64 {
	return idx.version
}

// CreateCheckpoint creates a new checkpoint at the current state
func (idx *IncrementalIndex) CreateCheckpoint() Checkpoint {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	cp := Checkpoint{
		Version:   idx.changeLog.LastSeq(),
		Timestamp: time.Now().Unix(),
		NodeCount: len(idx.nodes),
	}

	idx.checkpoints = append(idx.checkpoints, cp)
	return cp
}

// CheckpointHistory returns all checkpoints in creation order
func (idx *IncrementalIndex) CheckpointHistory() []Checkpoint {
	result := make([]Checkpoint, len(idx.checkpoints))
	copy(result, idx.checkpoints)
	return result
}

// OperationsBetween returns operations with sequence numbers in range (startSeq, endSeq]
func (idx *IncrementalIndex) OperationsBetween(startSeq, endSeq uint64) []Operation {
	allOps := idx.changeLog.Operations()
	var result []Operation
	for _, op := range allOps {
		if op.Seq > startSeq && op.Seq <= endSeq {
			result = append(result, op)
		}
	}
	return result
}

// CompactChangeLog compresses the change log by computing net changes
// This is useful for reducing memory usage during long-running operations
func (idx *IncrementalIndex) CompactChangeLog() {
	idx.changeLog.Compact()
}

// EstimateMemoryUsage returns estimated memory usage of the change log
func (idx *IncrementalIndex) EstimateMemoryUsage() int64 {
	return idx.changeLog.EstimateMemoryUsage()
}

// TruncateChangeLogOlderThan removes operations older than the given sequence
func (idx *IncrementalIndex) TruncateChangeLogOlderThan(seq uint64) {
	idx.changeLog.TruncateOlderThan(seq)
}
