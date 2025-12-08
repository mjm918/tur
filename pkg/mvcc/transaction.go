// pkg/mvcc/transaction.go
package mvcc

import (
	"errors"
	"sync"
)

// Errors
var (
	ErrTxNotActive = errors.New("transaction is not active")
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

// Transaction represents a database transaction for MVCC
type Transaction struct {
	mu       sync.RWMutex
	id       uint64  // Unique transaction ID
	startTS  uint64  // Snapshot timestamp - reads see versions committed before this
	commitTS uint64  // Commit timestamp - 0 if uncommitted
	state    TxState // Current state of the transaction
}

// NewTransaction creates a new transaction with the given ID and start timestamp
func NewTransaction(id, startTS uint64) *Transaction {
	return &Transaction{
		id:       id,
		startTS:  startTS,
		commitTS: 0,
		state:    TxStateActive,
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
	return nil
}

// Abort aborts the transaction
func (tx *Transaction) Abort() {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state == TxStateActive {
		tx.state = TxStateAborted
	}
}
