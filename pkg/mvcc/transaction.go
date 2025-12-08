// pkg/mvcc/transaction.go
package mvcc

import (
	"errors"
	"sync"
)

// Errors
var (
	ErrTxNotActive       = errors.New("transaction is not active")
	ErrSavepointNotFound = errors.New("savepoint not found")
)

// TxState represents the state of a transaction
type TxState int

const (
	TxStateActive TxState = iota
	TxStateCommitted
	TxStateAborted
)

// String returns a string representation of the transaction state
func (s TxState) String() string {
	switch s {
	case TxStateActive:
		return "Active"
	case TxStateCommitted:
		return "Committed"
	case TxStateAborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

// Savepoint represents a savepoint within a transaction
type Savepoint struct {
	Name          string // Unique name for the savepoint
	ModStartIndex int    // Index in modifications slice when savepoint was created
}

// Transaction represents a database transaction for MVCC
type Transaction struct {
	mu            sync.RWMutex
	id            uint64       // Unique transaction ID
	startTS       uint64       // Snapshot timestamp - reads see versions committed before this
	commitTS      uint64       // Commit timestamp - 0 if uncommitted
	state         TxState      // Current state of the transaction
	savepoints    []Savepoint  // Stack of savepoints (newest at end)
	modifications []string     // Keys modified in this transaction
}

// NewTransaction creates a new transaction with the given ID and start timestamp
func NewTransaction(id, startTS uint64) *Transaction {
	return &Transaction{
		id:            id,
		startTS:       startTS,
		commitTS:      0,
		state:         TxStateActive,
		savepoints:    nil,
		modifications: nil,
	}
}

// ID returns the transaction ID
func (tx *Transaction) ID() uint64 {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.id
}

// StartTS returns the start timestamp
func (tx *Transaction) StartTS() uint64 {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.startTS
}

// CommitTS returns the commit timestamp (0 if uncommitted)
func (tx *Transaction) CommitTS() uint64 {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.commitTS
}

// State returns the current transaction state
func (tx *Transaction) State() TxState {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.state
}

// IsActive returns true if the transaction is still active
func (tx *Transaction) IsActive() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.state == TxStateActive
}

// IsCommitted returns true if the transaction has been committed
func (tx *Transaction) IsCommitted() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.state == TxStateCommitted
}

// IsAborted returns true if the transaction has been aborted
func (tx *Transaction) IsAborted() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.state == TxStateAborted
}

// Commit commits the transaction with the given commit timestamp
func (tx *Transaction) Commit(commitTS uint64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return ErrTxNotActive
	}

	tx.commitTS = commitTS
	tx.state = TxStateCommitted
	tx.savepoints = nil // Clear all savepoints on commit
	return nil
}

// Abort aborts the transaction
func (tx *Transaction) Abort() {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state == TxStateActive {
		tx.state = TxStateAborted
		tx.savepoints = nil // Clear all savepoints on abort
	}
}

// SavepointCount returns the number of active savepoints
func (tx *Transaction) SavepointCount() int {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return len(tx.savepoints)
}

// Savepoint creates a new savepoint with the given name
func (tx *Transaction) Savepoint(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return ErrTxNotActive
	}

	tx.savepoints = append(tx.savepoints, Savepoint{
		Name:          name,
		ModStartIndex: len(tx.modifications),
	})
	return nil
}

// RollbackTo rolls back to the specified savepoint, removing it and all later savepoints
func (tx *Transaction) RollbackTo(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return ErrTxNotActive
	}

	// Find the savepoint (search from newest to oldest)
	idx := -1
	for i := len(tx.savepoints) - 1; i >= 0; i-- {
		if tx.savepoints[i].Name == name {
			idx = i
			break
		}
	}

	if idx == -1 {
		return ErrSavepointNotFound
	}

	// Truncate modifications to the savepoint's start index
	modIdx := tx.savepoints[idx].ModStartIndex
	tx.modifications = tx.modifications[:modIdx]

	// Remove the savepoint and all later ones
	tx.savepoints = tx.savepoints[:idx]
	return nil
}

// Release releases the specified savepoint and all later savepoints
func (tx *Transaction) Release(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return ErrTxNotActive
	}

	// Find the savepoint (search from newest to oldest)
	idx := -1
	for i := len(tx.savepoints) - 1; i >= 0; i-- {
		if tx.savepoints[i].Name == name {
			idx = i
			break
		}
	}

	if idx == -1 {
		return ErrSavepointNotFound
	}

	// Remove the savepoint and all later ones
	tx.savepoints = tx.savepoints[:idx]
	return nil
}

// RecordModification records a key that was modified in this transaction
func (tx *Transaction) RecordModification(key string) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.modifications = append(tx.modifications, key)
}

// GetModificationsSince returns all keys modified since the specified savepoint
func (tx *Transaction) GetModificationsSince(savepointName string) []string {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	// Find the savepoint
	for i := len(tx.savepoints) - 1; i >= 0; i-- {
		if tx.savepoints[i].Name == savepointName {
			startIdx := tx.savepoints[i].ModStartIndex
			if startIdx >= len(tx.modifications) {
				return nil
			}
			result := make([]string, len(tx.modifications)-startIdx)
			copy(result, tx.modifications[startIdx:])
			return result
		}
	}
	return nil
}

// GetAllModifications returns all keys modified in this transaction
func (tx *Transaction) GetAllModifications() []string {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	result := make([]string, len(tx.modifications))
	copy(result, tx.modifications)
	return result
}
