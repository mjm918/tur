package mvcc

import (
	"fmt"
	"sync"
)

// UndoOpType represents the type of operation to undo
type UndoOpType int

const (
	// UndoInsert - undo by DELETE (remove the inserted row)
	UndoInsert UndoOpType = iota
	// UndoUpdate - undo by restoring old data
	UndoUpdate
	// UndoDelete - undo by re-INSERT (restore the deleted row)
	UndoDelete
	// UndoIndexInsert - undo by removing from index
	UndoIndexInsert
	// UndoIndexDelete - undo by re-adding to index
	UndoIndexDelete
)

// UndoOperation represents a single undoable operation
type UndoOperation struct {
	Type      UndoOpType
	TableName string
	Key       []byte // B-tree key (rowid encoded as 8 bytes)
	OldData   []byte // For UPDATE/DELETE: original encoded record data

	// For index operations
	IndexName string
	IndexKey  []byte
	IndexVal  []byte // rowid as bytes
}

// UndoLog tracks all undoable operations for a transaction
type UndoLog struct {
	mu         sync.Mutex
	operations []UndoOperation
	savepoints map[string]int // savepoint name -> index in operations slice
}

// NewUndoLog creates a new empty undo log
func NewUndoLog() *UndoLog {
	return &UndoLog{
		operations: make([]UndoOperation, 0),
		savepoints: make(map[string]int),
	}
}

// Add appends an operation to the undo log
func (u *UndoLog) Add(op UndoOperation) {
	u.mu.Lock()
	defer u.mu.Unlock()

	// Make copies of byte slices to avoid aliasing issues
	opCopy := UndoOperation{
		Type:      op.Type,
		TableName: op.TableName,
		IndexName: op.IndexName,
	}

	if op.Key != nil {
		opCopy.Key = make([]byte, len(op.Key))
		copy(opCopy.Key, op.Key)
	}
	if op.OldData != nil {
		opCopy.OldData = make([]byte, len(op.OldData))
		copy(opCopy.OldData, op.OldData)
	}
	if op.IndexKey != nil {
		opCopy.IndexKey = make([]byte, len(op.IndexKey))
		copy(opCopy.IndexKey, op.IndexKey)
	}
	if op.IndexVal != nil {
		opCopy.IndexVal = make([]byte, len(op.IndexVal))
		copy(opCopy.IndexVal, op.IndexVal)
	}

	u.operations = append(u.operations, opCopy)
}

// GetAllOperations returns a copy of all operations in the log
func (u *UndoLog) GetAllOperations() []UndoOperation {
	u.mu.Lock()
	defer u.mu.Unlock()

	result := make([]UndoOperation, len(u.operations))
	copy(result, u.operations)
	return result
}

// Clear removes all operations and savepoints from the log
func (u *UndoLog) Clear() {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.operations = make([]UndoOperation, 0)
	u.savepoints = make(map[string]int)
}

// CreateSavepoint records the current position in the undo log
func (u *UndoLog) CreateSavepoint(name string) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// Record current position (index of next operation)
	u.savepoints[name] = len(u.operations)
	return nil
}

// RollbackToSavepoint returns operations to undo (in reverse order) and truncates the log
func (u *UndoLog) RollbackToSavepoint(name string) ([]UndoOperation, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	idx, exists := u.savepoints[name]
	if !exists {
		return nil, fmt.Errorf("savepoint %s does not exist", name)
	}

	// Get operations after the savepoint
	opsAfterSavepoint := u.operations[idx:]

	// Reverse them for LIFO undo order
	opsToUndo := make([]UndoOperation, len(opsAfterSavepoint))
	for i, op := range opsAfterSavepoint {
		opsToUndo[len(opsAfterSavepoint)-1-i] = op
	}

	// Truncate log to savepoint position
	u.operations = u.operations[:idx]

	// Remove savepoints that were after this position
	for spName, spIdx := range u.savepoints {
		if spIdx > idx {
			delete(u.savepoints, spName)
		}
	}

	return opsToUndo, nil
}

// ReleaseSavepoint removes a savepoint but keeps all operations
func (u *UndoLog) ReleaseSavepoint(name string) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if _, exists := u.savepoints[name]; !exists {
		return fmt.Errorf("savepoint %s does not exist", name)
	}

	delete(u.savepoints, name)
	return nil
}

// Len returns the number of operations in the log
func (u *UndoLog) Len() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.operations)
}

// HasSavepoint checks if a savepoint exists
func (u *UndoLog) HasSavepoint(name string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	_, exists := u.savepoints[name]
	return exists
}
