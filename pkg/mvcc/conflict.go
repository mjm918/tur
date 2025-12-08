// pkg/mvcc/conflict.go
package mvcc

import (
	"errors"
	"sync"
)

var (
	ErrWriteConflict = errors.New("write-write conflict detected")
)

// WriteSet tracks keys that a transaction has written to
type WriteSet struct {
	mu   sync.RWMutex
	keys map[string]struct{}
}

// NewWriteSet creates a new write set
func NewWriteSet() *WriteSet {
	return &WriteSet{
		keys: make(map[string]struct{}),
	}
}

// Add adds a key to the write set
func (ws *WriteSet) Add(key []byte) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.keys[string(key)] = struct{}{}
}

// Contains returns true if the key is in the write set
func (ws *WriteSet) Contains(key []byte) bool {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	_, ok := ws.keys[string(key)]
	return ok
}

// Keys returns all keys in the write set
func (ws *WriteSet) Keys() [][]byte {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	result := make([][]byte, 0, len(ws.keys))
	for k := range ws.keys {
		result = append(result, []byte(k))
	}
	return result
}

// ReadSet tracks keys that a transaction has read
type ReadSet struct {
	mu   sync.RWMutex
	keys map[string]struct{}
}

// NewReadSet creates a new read set
func NewReadSet() *ReadSet {
	return &ReadSet{
		keys: make(map[string]struct{}),
	}
}

// Add adds a key to the read set
func (rs *ReadSet) Add(key []byte) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.keys[string(key)] = struct{}{}
}

// Contains returns true if the key is in the read set
func (rs *ReadSet) Contains(key []byte) bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	_, ok := rs.keys[string(key)]
	return ok
}

// Keys returns all keys in the read set
func (rs *ReadSet) Keys() [][]byte {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	result := make([][]byte, 0, len(rs.keys))
	for k := range rs.keys {
		result = append(result, []byte(k))
	}
	return result
}

// txWriteEntry tracks the write set for an active transaction
type txWriteEntry struct {
	tx     *Transaction
	writes *WriteSet
}

// ConflictDetector detects write-write conflicts between transactions
type ConflictDetector struct {
	mu sync.RWMutex
	// Map from key to the transaction that is currently writing it
	keyLocks map[string]*txWriteEntry
	// Map from transaction ID to its write entry
	txWrites map[uint64]*txWriteEntry
}

// NewConflictDetector creates a new conflict detector
func NewConflictDetector() *ConflictDetector {
	return &ConflictDetector{
		keyLocks: make(map[string]*txWriteEntry),
		txWrites: make(map[uint64]*txWriteEntry),
	}
}

// RegisterWrites registers a transaction's write set for conflict detection
func (cd *ConflictDetector) RegisterWrites(tx *Transaction, ws *WriteSet) {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	entry := &txWriteEntry{
		tx:     tx,
		writes: ws,
	}

	// Record this transaction's writes
	cd.txWrites[tx.ID()] = entry

	// Lock each key
	for _, key := range ws.Keys() {
		keyStr := string(key)
		cd.keyLocks[keyStr] = entry
	}
}

// CheckConflict checks if a transaction's writes conflict with any active transaction
func (cd *ConflictDetector) CheckConflict(tx *Transaction, ws *WriteSet) error {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	txID := tx.ID()

	for _, key := range ws.Keys() {
		keyStr := string(key)
		if entry, ok := cd.keyLocks[keyStr]; ok {
			// Check if it's a different transaction
			if entry.tx.ID() != txID {
				// Check if that transaction is still active
				if entry.tx.IsActive() {
					return ErrWriteConflict
				}
			}
		}
	}

	return nil
}

// FindConflictingTransaction finds the transaction that conflicts with the given writes
func (cd *ConflictDetector) FindConflictingTransaction(tx *Transaction, ws *WriteSet) *Transaction {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	txID := tx.ID()

	for _, key := range ws.Keys() {
		keyStr := string(key)
		if entry, ok := cd.keyLocks[keyStr]; ok {
			if entry.tx.ID() != txID && entry.tx.IsActive() {
				return entry.tx
			}
		}
	}

	return nil
}

// OnCommit cleans up after a transaction commits
func (cd *ConflictDetector) OnCommit(tx *Transaction) {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	cd.cleanupTransaction(tx)
}

// OnAbort cleans up after a transaction aborts
func (cd *ConflictDetector) OnAbort(tx *Transaction) {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	cd.cleanupTransaction(tx)
}

// cleanupTransaction removes a transaction's locks
func (cd *ConflictDetector) cleanupTransaction(tx *Transaction) {
	txID := tx.ID()

	entry, ok := cd.txWrites[txID]
	if !ok {
		return
	}

	// Remove key locks
	for _, key := range entry.writes.Keys() {
		keyStr := string(key)
		if locked, exists := cd.keyLocks[keyStr]; exists && locked.tx.ID() == txID {
			delete(cd.keyLocks, keyStr)
		}
	}

	// Remove transaction entry
	delete(cd.txWrites, txID)
}

// ActiveConflicts returns the number of active key locks
func (cd *ConflictDetector) ActiveConflicts() int {
	cd.mu.RLock()
	defer cd.mu.RUnlock()
	return len(cd.keyLocks)
}
