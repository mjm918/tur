// pkg/mvcc/visibility.go
package mvcc

// IsVersionVisible determines if a version is visible to a transaction
// according to MVCC snapshot isolation rules:
//
// A version V is visible to transaction T if:
//  1. V was created by T itself, OR
//  2. V.CreatedBy is committed AND V.CreatedBy.CommitTS < T.StartTS
//     AND (V.DeletedBy == 0 OR V.DeletedBy is not committed at T.StartTS)
//
// Special cases:
// - T can always see its own uncommitted writes
// - T cannot see versions deleted by itself
func IsVersionVisible(v *RowVersion, tx *Transaction, mgr *TransactionManager) bool {
	if v == nil || tx == nil {
		return false
	}

	txID := tx.ID()
	txStartTS := tx.StartTS()

	creatorID := v.CreatedBy()
	deleterID := v.DeletedBy()

	// Special case: Transaction sees its own uncommitted writes
	if creatorID == txID {
		// But not if it also deleted it
		return deleterID == 0 || deleterID != txID
	}

	// Check if creator is visible
	creatorTx := mgr.GetTransaction(creatorID)
	if creatorTx == nil {
		// Creator transaction doesn't exist - shouldn't happen in practice
		return false
	}

	// Creator must be committed
	if !creatorTx.IsCommitted() {
		return false
	}

	// Creator must have committed before this transaction started
	if creatorTx.CommitTS() >= txStartTS {
		return false
	}

	// Check deletion status
	if deleterID == 0 {
		// Not deleted - visible
		return true
	}

	// If current transaction is the deleter, it should not see this version
	if deleterID == txID {
		return false
	}

	// Check if deleter has committed
	deleterTx := mgr.GetTransaction(deleterID)
	if deleterTx == nil {
		// Deleter doesn't exist - treat as not deleted
		return true
	}

	// If deleter is not committed, version is still visible
	if !deleterTx.IsCommitted() {
		return true
	}

	// If deleter committed after our snapshot, version is visible
	if deleterTx.CommitTS() >= txStartTS {
		return true
	}

	// Version was deleted before our snapshot started
	return false
}

// FindVisibleVersion finds the appropriate version in a chain that is visible
// to the given transaction. Returns nil if no visible version exists.
func FindVisibleVersion(chain *VersionChain, tx *Transaction, mgr *TransactionManager) *RowVersion {
	if chain == nil || tx == nil {
		return nil
	}

	current := chain.Head()
	for current != nil {
		if IsVersionVisible(current, tx, mgr) {
			return current
		}
		current = current.Next()
	}

	return nil
}

// GetVisibleData returns the data from the visible version for a transaction.
// Returns nil if no visible version exists.
func GetVisibleData(chain *VersionChain, tx *Transaction, mgr *TransactionManager) []byte {
	v := FindVisibleVersion(chain, tx, mgr)
	if v == nil {
		return nil
	}
	return v.Data()
}
