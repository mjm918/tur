// pkg/mvcc/manager.go
package mvcc

import (
	"sync"
	"sync/atomic"
)

// TransactionManager manages all transactions in the database
type TransactionManager struct {
	mu           sync.RWMutex
	transactions map[uint64]*Transaction // All transactions by ID
	nextTxID     uint64                  // Next transaction ID (atomic)
	timestamp    uint64                  // Current logical timestamp (atomic)
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		transactions: make(map[uint64]*Transaction),
		nextTxID:     1,
		timestamp:    1,
	}
}

// Begin starts a new transaction and returns it
func (m *TransactionManager) Begin() *Transaction {
	// Atomically get next transaction ID and timestamp
	txID := atomic.AddUint64(&m.nextTxID, 1) - 1
	startTS := atomic.AddUint64(&m.timestamp, 1) - 1

	tx := NewTransaction(txID, startTS)

	m.mu.Lock()
	m.transactions[txID] = tx
	m.mu.Unlock()

	return tx
}

// Commit commits a transaction
func (m *TransactionManager) Commit(tx *Transaction) error {
	if !tx.IsActive() {
		return ErrTxNotActive
	}

	// Get commit timestamp
	commitTS := atomic.AddUint64(&m.timestamp, 1) - 1

	return tx.Commit(commitTS)
}

// Rollback aborts a transaction
func (m *TransactionManager) Rollback(tx *Transaction) error {
	if !tx.IsActive() {
		return ErrTxNotActive
	}

	tx.Abort()
	return nil
}

// GetTransaction returns a transaction by ID
func (m *TransactionManager) GetTransaction(txID uint64) *Transaction {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.transactions[txID]
}

// ActiveTransactions returns all currently active transactions
func (m *TransactionManager) ActiveTransactions() []*Transaction {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var active []*Transaction
	for _, tx := range m.transactions {
		if tx.IsActive() {
			active = append(active, tx)
		}
	}
	return active
}

// CurrentTimestamp returns the current logical timestamp
func (m *TransactionManager) CurrentTimestamp() uint64 {
	return atomic.LoadUint64(&m.timestamp)
}

// MinActiveTimestamp returns the minimum start timestamp of all active transactions
// This is used for garbage collection of old versions
func (m *TransactionManager) MinActiveTimestamp() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	minTS := ^uint64(0) // Max uint64

	for _, tx := range m.transactions {
		if tx.IsActive() {
			startTS := tx.StartTS()
			if startTS < minTS {
				minTS = startTS
			}
		}
	}

	return minTS
}

// CleanupOldTransactions removes transactions that are no longer needed
// This should be called periodically to free memory
func (m *TransactionManager) CleanupOldTransactions(minTS uint64) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for txID, tx := range m.transactions {
		// Only cleanup committed/aborted transactions older than minTS
		if !tx.IsActive() && tx.CommitTS() < minTS {
			delete(m.transactions, txID)
			count++
		}
	}
	return count
}
