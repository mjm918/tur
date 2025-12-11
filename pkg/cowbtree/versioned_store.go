// pkg/cowbtree/versioned_store.go
package cowbtree

import (
	"errors"
	"sync"

	"tur/pkg/mvcc"
)

var (
	ErrTxNotActive    = errors.New("transaction is not active")
	ErrWriteConflict  = errors.New("write-write conflict detected")
	ErrStoreNotFound  = errors.New("key not found in store")
)

// CowVersionedStore combines a CoW B+ tree with MVCC for full transactional support.
// It provides:
// - Lock-free reads through CoW B+ tree snapshots
// - Snapshot isolation through MVCC version chains
// - Write-write conflict detection
// - Efficient garbage collection of old versions
//
// Architecture:
// - The CoW B+ tree stores the latest committed state (key -> latest version chain head)
// - Version chains provide historical versions for snapshot isolation
// - Readers see a consistent snapshot without any locks
// - Writers are serialized but use path copying to minimize blocking
type CowVersionedStore struct {
	// tree is the underlying CoW B+ tree
	tree *CowBTree

	// txManager manages transactions and timestamps
	txManager *mvcc.TransactionManager

	// versionChains stores version chains for each key
	// The CoW tree stores pointers to these chains
	versionMu     sync.RWMutex
	versionChains map[string]*mvcc.VersionChain

	// conflictDetector tracks write sets for conflict detection
	conflictMu   sync.Mutex
	writeSets    map[uint64]map[string]struct{} // txID -> set of written keys

	// gcInterval controls how often garbage collection runs
	gcCounter int
	gcEvery   int
}

// NewCowVersionedStore creates a new versioned store backed by a CoW B+ tree
func NewCowVersionedStore() *CowVersionedStore {
	return NewCowVersionedStoreWithConfig(DefaultNodeConfig())
}

// NewCowVersionedStoreWithConfig creates a versioned store with custom configuration
func NewCowVersionedStoreWithConfig(config NodeConfig) *CowVersionedStore {
	return &CowVersionedStore{
		tree:          NewCowBTreeWithConfig(config),
		txManager:     mvcc.NewTransactionManager(),
		versionChains: make(map[string]*mvcc.VersionChain),
		writeSets:     make(map[uint64]map[string]struct{}),
		gcEvery:       1000, // Run GC every 1000 operations
	}
}

// Begin starts a new transaction
func (s *CowVersionedStore) Begin() *mvcc.Transaction {
	return s.txManager.Begin()
}

// Get retrieves the value for a key visible to the transaction.
// This is a lock-free read operation that uses snapshot isolation.
func (s *CowVersionedStore) Get(tx *mvcc.Transaction, key []byte) ([]byte, error) {
	if tx == nil || !tx.IsActive() {
		return nil, ErrTxNotActive
	}

	// First check if the transaction has its own uncommitted write
	s.versionMu.RLock()
	chain := s.versionChains[string(key)]
	s.versionMu.RUnlock()

	if chain != nil {
		// Find the version visible to this transaction
		version := mvcc.FindVisibleVersion(chain, tx, s.txManager)
		if version != nil {
			return version.Data(), nil
		}
	}

	return nil, ErrStoreNotFound
}

// GetSnapshot retrieves a value using a tree snapshot for even better read performance.
// The snapshot must be released after use.
func (s *CowVersionedStore) GetSnapshot(tx *mvcc.Transaction, key []byte) ([]byte, *CowBTreeSnapshot, error) {
	if tx == nil || !tx.IsActive() {
		return nil, nil, ErrTxNotActive
	}

	// Create a snapshot of the tree
	snapshot := s.tree.Snapshot()

	// Look up the key in the snapshot
	s.versionMu.RLock()
	chain := s.versionChains[string(key)]
	s.versionMu.RUnlock()

	if chain != nil {
		version := mvcc.FindVisibleVersion(chain, tx, s.txManager)
		if version != nil {
			return version.Data(), snapshot, nil
		}
	}

	return nil, snapshot, ErrStoreNotFound
}

// Put stores a key-value pair, creating a new version
func (s *CowVersionedStore) Put(tx *mvcc.Transaction, key, value []byte) error {
	if tx == nil || !tx.IsActive() {
		return ErrTxNotActive
	}

	// Check for write-write conflict
	if err := s.checkWriteConflict(tx, key); err != nil {
		return err
	}

	// Register this write
	s.registerWrite(tx, key)

	// Create new version
	s.versionMu.Lock()

	keyStr := string(key)
	chain := s.versionChains[keyStr]
	if chain == nil {
		chain = mvcc.NewVersionChain(key)
		s.versionChains[keyStr] = chain

		// Also insert into the CoW tree (stores key -> exists marker)
		// The actual value is in the version chain
		s.tree.Insert(key, []byte{1}) // marker value
	}

	// Add new version to chain
	version := mvcc.NewRowVersion(value, tx.ID())
	chain.AddVersion(version)

	s.versionMu.Unlock()

	// Periodic garbage collection
	s.maybeRunGC()

	return nil
}

// Delete removes a key by marking the visible version as deleted
func (s *CowVersionedStore) Delete(tx *mvcc.Transaction, key []byte) error {
	if tx == nil || !tx.IsActive() {
		return ErrTxNotActive
	}

	// Check for write-write conflict
	if err := s.checkWriteConflict(tx, key); err != nil {
		return err
	}

	// Register this write
	s.registerWrite(tx, key)

	s.versionMu.Lock()
	defer s.versionMu.Unlock()

	keyStr := string(key)
	chain := s.versionChains[keyStr]
	if chain == nil {
		// Key doesn't exist - nothing to delete
		return nil
	}

	// Find and mark the visible version as deleted
	version := mvcc.FindVisibleVersion(chain, tx, s.txManager)
	if version != nil {
		version.MarkDeleted(tx.ID())
	}

	return nil
}

// Commit commits a transaction
func (s *CowVersionedStore) Commit(tx *mvcc.Transaction) error {
	if tx == nil || !tx.IsActive() {
		return ErrTxNotActive
	}

	// Commit the transaction
	err := s.txManager.Commit(tx)
	if err != nil {
		return err
	}

	// Clean up write set
	s.conflictMu.Lock()
	delete(s.writeSets, tx.ID())
	s.conflictMu.Unlock()

	return nil
}

// Rollback aborts a transaction and discards its changes
func (s *CowVersionedStore) Rollback(tx *mvcc.Transaction) error {
	if tx == nil || !tx.IsActive() {
		return ErrTxNotActive
	}

	// Mark versions created by this transaction as deleted
	s.rollbackVersions(tx)

	// Abort the transaction
	s.txManager.Rollback(tx)

	// Clean up write set
	s.conflictMu.Lock()
	delete(s.writeSets, tx.ID())
	s.conflictMu.Unlock()

	return nil
}

// rollbackVersions marks all versions created by the transaction as invalid
func (s *CowVersionedStore) rollbackVersions(tx *mvcc.Transaction) {
	s.versionMu.Lock()
	defer s.versionMu.Unlock()

	txID := tx.ID()

	for _, chain := range s.versionChains {
		head := chain.Head()
		if head != nil && head.CreatedBy() == txID {
			head.MarkDeleted(txID)
		}
	}
}

// checkWriteConflict checks if another active transaction has written to this key
func (s *CowVersionedStore) checkWriteConflict(tx *mvcc.Transaction, key []byte) error {
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()

	keyStr := string(key)
	txID := tx.ID()

	for otherTxID, writeSet := range s.writeSets {
		if otherTxID == txID {
			continue
		}

		// Check if the other transaction has written to this key
		if _, exists := writeSet[keyStr]; exists {
			// Check if the other transaction is still active
			otherTx := s.txManager.GetTransaction(otherTxID)
			if otherTx != nil && otherTx.IsActive() {
				return ErrWriteConflict
			}
		}
	}

	return nil
}

// registerWrite records that a transaction has written to a key
func (s *CowVersionedStore) registerWrite(tx *mvcc.Transaction, key []byte) {
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()

	txID := tx.ID()
	if s.writeSets[txID] == nil {
		s.writeSets[txID] = make(map[string]struct{})
	}
	s.writeSets[txID][string(key)] = struct{}{}
}

// maybeRunGC periodically runs garbage collection
func (s *CowVersionedStore) maybeRunGC() {
	s.gcCounter++
	if s.gcCounter >= s.gcEvery {
		s.gcCounter = 0
		go s.GarbageCollect()
	}
}

// GarbageCollect removes old versions that are no longer visible to any transaction
func (s *CowVersionedStore) GarbageCollect() {
	minTS := s.txManager.MinActiveTimestamp()

	s.versionMu.Lock()
	defer s.versionMu.Unlock()

	for _, chain := range s.versionChains {
		chain.PruneOldVersions(s.txManager, minTS)
	}

	// Also cleanup old transaction metadata
	s.txManager.CleanupOldTransactions(minTS)
}

// Range performs a range scan with snapshot isolation
func (s *CowVersionedStore) Range(tx *mvcc.Transaction, startKey, endKey []byte, fn func(key, value []byte) bool) error {
	if tx == nil || !tx.IsActive() {
		return ErrTxNotActive
	}

	// Use the CoW tree's range scan to find keys
	return s.tree.Range(startKey, endKey, func(key, _ []byte) bool {
		// For each key, get the visible version
		s.versionMu.RLock()
		chain := s.versionChains[string(key)]
		s.versionMu.RUnlock()

		if chain != nil {
			version := mvcc.FindVisibleVersion(chain, tx, s.txManager)
			if version != nil && !version.IsDeleted() {
				return fn(key, version.Data())
			}
		}
		return true
	})
}

// CreateSnapshot creates a read-only snapshot for efficient repeated reads
func (s *CowVersionedStore) CreateSnapshot(tx *mvcc.Transaction) (*VersionedSnapshot, error) {
	if tx == nil || !tx.IsActive() {
		return nil, ErrTxNotActive
	}

	return &VersionedSnapshot{
		store:        s,
		tx:           tx,
		treeSnapshot: s.tree.Snapshot(),
	}, nil
}

// VersionedSnapshot represents a consistent read-only view of the versioned store
type VersionedSnapshot struct {
	store        *CowVersionedStore
	tx           *mvcc.Transaction
	treeSnapshot *CowBTreeSnapshot
}

// Get retrieves a value from the snapshot
func (vs *VersionedSnapshot) Get(key []byte) ([]byte, error) {
	vs.store.versionMu.RLock()
	chain := vs.store.versionChains[string(key)]
	vs.store.versionMu.RUnlock()

	if chain != nil {
		version := mvcc.FindVisibleVersion(chain, vs.tx, vs.store.txManager)
		if version != nil {
			return version.Data(), nil
		}
	}

	return nil, ErrStoreNotFound
}

// Range performs a range scan on the snapshot
func (vs *VersionedSnapshot) Range(startKey, endKey []byte, fn func(key, value []byte) bool) error {
	return vs.treeSnapshot.Range(startKey, endKey, func(key, _ []byte) bool {
		vs.store.versionMu.RLock()
		chain := vs.store.versionChains[string(key)]
		vs.store.versionMu.RUnlock()

		if chain != nil {
			version := mvcc.FindVisibleVersion(chain, vs.tx, vs.store.txManager)
			if version != nil && !version.IsDeleted() {
				return fn(key, version.Data())
			}
		}
		return true
	})
}

// Release releases the snapshot
func (vs *VersionedSnapshot) Release() {
	if vs.treeSnapshot != nil {
		vs.treeSnapshot.Release()
		vs.treeSnapshot = nil
	}
}

// Stats returns statistics about the store
type StoreStats struct {
	TreeStats          CowBTreeStats
	ActiveTransactions int
	TotalVersionChains int
	TotalVersions      int
}

func (s *CowVersionedStore) Stats() StoreStats {
	s.versionMu.RLock()
	chainCount := len(s.versionChains)
	totalVersions := 0
	for _, chain := range s.versionChains {
		totalVersions += chain.Length()
	}
	s.versionMu.RUnlock()

	return StoreStats{
		TreeStats:          s.tree.Stats(),
		ActiveTransactions: len(s.txManager.ActiveTransactions()),
		TotalVersionChains: chainCount,
		TotalVersions:      totalVersions,
	}
}

// Close shuts down the store
func (s *CowVersionedStore) Close() error {
	return s.tree.Close()
}
