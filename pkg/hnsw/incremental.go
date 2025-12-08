// pkg/hnsw/incremental.go
package hnsw

import (
	"sync"

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
