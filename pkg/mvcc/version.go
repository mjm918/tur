// pkg/mvcc/version.go
package mvcc

import "sync"

// RowVersion represents a single version of a row in MVCC
type RowVersion struct {
	data      []byte      // The row data for this version
	createdBy uint64      // Transaction ID that created this version
	deletedBy uint64      // Transaction ID that deleted this version (0 = not deleted)
	next      *RowVersion // Pointer to the next (older) version
}

// NewRowVersion creates a new row version with the given data and creating transaction
func NewRowVersion(data []byte, createdBy uint64) *RowVersion {
	// Copy data to avoid external mutation
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	return &RowVersion{
		data:      dataCopy,
		createdBy: createdBy,
		deletedBy: 0,
		next:      nil,
	}
}

// Data returns a copy of the row data
func (v *RowVersion) Data() []byte {
	if v.data == nil {
		return nil
	}
	copied := make([]byte, len(v.data))
	copy(copied, v.data)
	return copied
}

// CreatedBy returns the transaction ID that created this version
func (v *RowVersion) CreatedBy() uint64 {
	return v.createdBy
}

// DeletedBy returns the transaction ID that deleted this version (0 if not deleted)
func (v *RowVersion) DeletedBy() uint64 {
	return v.deletedBy
}

// Next returns the next (older) version in the chain
func (v *RowVersion) Next() *RowVersion {
	return v.next
}

// SetNext sets the next version pointer
func (v *RowVersion) SetNext(next *RowVersion) {
	v.next = next
}

// IsDeleted returns true if this version has been marked as deleted
func (v *RowVersion) IsDeleted() bool {
	return v.deletedBy != 0
}

// MarkDeleted marks this version as deleted by the given transaction
func (v *RowVersion) MarkDeleted(txID uint64) {
	v.deletedBy = txID
}

// VersionChain manages a chain of versions for a single key
type VersionChain struct {
	mu   sync.RWMutex
	key  []byte      // The key this chain belongs to
	head *RowVersion // Most recent version (head of the chain)
}

// NewVersionChain creates a new version chain for the given key
func NewVersionChain(key []byte) *VersionChain {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	return &VersionChain{
		key:  keyCopy,
		head: nil,
	}
}

// Key returns the key for this version chain
func (c *VersionChain) Key() []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keyCopy := make([]byte, len(c.key))
	copy(keyCopy, c.key)
	return keyCopy
}

// Head returns the most recent version
func (c *VersionChain) Head() *RowVersion {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.head
}

// AddVersion adds a new version to the head of the chain
func (c *VersionChain) AddVersion(v *RowVersion) {
	c.mu.Lock()
	defer c.mu.Unlock()

	v.SetNext(c.head)
	c.head = v
}

// FindVersionByCreator finds a version created by the given transaction
func (c *VersionChain) FindVersionByCreator(txID uint64) *RowVersion {
	c.mu.RLock()
	defer c.mu.RUnlock()

	current := c.head
	for current != nil {
		if current.CreatedBy() == txID {
			return current
		}
		current = current.Next()
	}
	return nil
}

// Length returns the number of versions in the chain
func (c *VersionChain) Length() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	count := 0
	current := c.head
	for current != nil {
		count++
		current = current.Next()
	}
	return count
}

// PruneOldVersions removes versions that are no longer needed
// A version can be pruned if:
// 1. It has been deleted by a committed transaction
// 2. No active transaction can possibly see it
// Returns the number of versions pruned
func (c *VersionChain) PruneOldVersions(mgr *TransactionManager, minActiveTS uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.head == nil {
		return 0
	}

	pruned := 0

	// Find the last version that must be kept
	// We need to keep at least one committed version visible to any active transaction
	var prev *RowVersion
	current := c.head

	for current != nil {
		next := current.Next()

		// Check if this version can be pruned
		// A version can be pruned if:
		// 1. Its creator has committed before minActiveTS
		// 2. It has been deleted by a transaction committed before minActiveTS
		// 3. There's a newer version that satisfies active transactions

		creatorTx := mgr.GetTransaction(current.CreatedBy())
		canPrune := false

		if creatorTx != nil && creatorTx.IsCommitted() {
			// Creator committed - check if deletion also committed
			if current.IsDeleted() {
				deleterTx := mgr.GetTransaction(current.DeletedBy())
				if deleterTx != nil && deleterTx.IsCommitted() && deleterTx.CommitTS() < minActiveTS {
					// This version was deleted before any active transaction started
					canPrune = true
				}
			} else if creatorTx.CommitTS() < minActiveTS && prev != nil {
				// There's a newer version and this one is old enough
				canPrune = true
			}
		}

		if canPrune {
			// Remove current from chain
			if prev != nil {
				prev.SetNext(next)
			} else {
				c.head = next
			}
			pruned++
		} else {
			prev = current
		}

		current = next
	}

	return pruned
}
