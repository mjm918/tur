// pkg/mvcc/transaction_test.go
package mvcc

import "testing"

func TestTxStateString(t *testing.T) {
	tests := []struct {
		state    TxState
		expected string
	}{
		{TxStateActive, "Active"},
		{TxStateCommitted, "Committed"},
		{TxStateAborted, "Aborted"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("TxState(%d).String() = %q, want %q", tt.state, got, tt.expected)
		}
	}
}

func TestTransactionCreate(t *testing.T) {
	tx := NewTransaction(1, 100)

	if tx.ID() != 1 {
		t.Errorf("expected ID 1, got %d", tx.ID())
	}

	if tx.StartTS() != 100 {
		t.Errorf("expected StartTS 100, got %d", tx.StartTS())
	}

	if tx.CommitTS() != 0 {
		t.Errorf("expected CommitTS 0 for uncommitted tx, got %d", tx.CommitTS())
	}

	if tx.State() != TxStateActive {
		t.Errorf("expected state Active, got %v", tx.State())
	}
}

func TestTransactionCommit(t *testing.T) {
	tx := NewTransaction(1, 100)

	err := tx.Commit(200)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tx.State() != TxStateCommitted {
		t.Errorf("expected state Committed, got %v", tx.State())
	}

	if tx.CommitTS() != 200 {
		t.Errorf("expected CommitTS 200, got %d", tx.CommitTS())
	}
}

func TestTransactionAbort(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.Abort()

	if tx.State() != TxStateAborted {
		t.Errorf("expected state Aborted, got %v", tx.State())
	}
}

func TestTransactionDoubleCommit(t *testing.T) {
	tx := NewTransaction(1, 100)
	tx.Commit(200)

	err := tx.Commit(300)
	if err != ErrTxNotActive {
		t.Errorf("expected ErrTxNotActive, got %v", err)
	}
}

func TestTransactionCommitAfterAbort(t *testing.T) {
	tx := NewTransaction(1, 100)
	tx.Abort()

	err := tx.Commit(200)
	if err != ErrTxNotActive {
		t.Errorf("expected ErrTxNotActive, got %v", err)
	}
}

func TestTransactionIsActive(t *testing.T) {
	tx := NewTransaction(1, 100)

	if !tx.IsActive() {
		t.Error("expected IsActive to return true")
	}

	tx.Commit(200)

	if tx.IsActive() {
		t.Error("expected IsActive to return false after commit")
	}
}

func TestTransactionIsCommitted(t *testing.T) {
	tx := NewTransaction(1, 100)

	if tx.IsCommitted() {
		t.Error("expected IsCommitted to return false")
	}

	tx.Commit(200)

	if !tx.IsCommitted() {
		t.Error("expected IsCommitted to return true after commit")
	}
}

func TestTransactionIsAborted(t *testing.T) {
	tx := NewTransaction(1, 100)

	if tx.IsAborted() {
		t.Error("expected IsAborted to return false")
	}

	tx.Abort()

	if !tx.IsAborted() {
		t.Error("expected IsAborted to return true after abort")
	}
}

// ============ Savepoint Tests ============

// Test 1: Transaction starts with empty savepoint stack
func TestTransaction_SavepointStackEmpty(t *testing.T) {
	tx := NewTransaction(1, 100)

	// New transaction should have no savepoints
	if tx.SavepointCount() != 0 {
		t.Errorf("expected 0 savepoints, got %d", tx.SavepointCount())
	}
}

// Test 2: Create a savepoint with a unique name
func TestTransaction_CreateSavepoint(t *testing.T) {
	tx := NewTransaction(1, 100)

	err := tx.Savepoint("sp1")
	if err != nil {
		t.Fatalf("unexpected error creating savepoint: %v", err)
	}

	if tx.SavepointCount() != 1 {
		t.Errorf("expected 1 savepoint, got %d", tx.SavepointCount())
	}
}

// Test 3: Create multiple nested savepoints
func TestTransaction_NestedSavepoints(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.Savepoint("sp1")
	tx.Savepoint("sp2")
	tx.Savepoint("sp3")

	if tx.SavepointCount() != 3 {
		t.Errorf("expected 3 savepoints, got %d", tx.SavepointCount())
	}
}

// Test 4: Duplicate savepoint name should be allowed (creates new one)
func TestTransaction_DuplicateSavepointName(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.Savepoint("sp1")
	tx.Savepoint("sp1") // Same name, should still work

	if tx.SavepointCount() != 2 {
		t.Errorf("expected 2 savepoints, got %d", tx.SavepointCount())
	}
}

// Test 5: Savepoint on non-active transaction fails
func TestTransaction_SavepointOnNonActive(t *testing.T) {
	tx := NewTransaction(1, 100)
	tx.Abort()

	err := tx.Savepoint("sp1")
	if err != ErrTxNotActive {
		t.Errorf("expected ErrTxNotActive, got %v", err)
	}
}

// Test 6: Rollback to a savepoint removes later savepoints
func TestTransaction_RollbackToSavepoint(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.Savepoint("sp1")
	tx.Savepoint("sp2")
	tx.Savepoint("sp3")

	err := tx.RollbackTo("sp2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// sp2 and sp3 should be removed, only sp1 remains
	if tx.SavepointCount() != 1 {
		t.Errorf("expected 1 savepoint after rollback, got %d", tx.SavepointCount())
	}
}

// Test 7: Rollback to non-existent savepoint fails
func TestTransaction_RollbackToNonExistent(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.Savepoint("sp1")

	err := tx.RollbackTo("nonexistent")
	if err == nil {
		t.Error("expected error when rolling back to non-existent savepoint")
	}
}

// Test 8: Release savepoint removes it and later ones
func TestTransaction_ReleaseSavepoint(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.Savepoint("sp1")
	tx.Savepoint("sp2")
	tx.Savepoint("sp3")

	err := tx.Release("sp2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// sp2 and sp3 should be released, only sp1 remains
	if tx.SavepointCount() != 1 {
		t.Errorf("expected 1 savepoint after release, got %d", tx.SavepointCount())
	}
}

// Test 9: Release non-existent savepoint fails
func TestTransaction_ReleaseNonExistent(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.Savepoint("sp1")

	err := tx.Release("nonexistent")
	if err == nil {
		t.Error("expected error when releasing non-existent savepoint")
	}
}

// Test 10: All savepoints cleared on commit
func TestTransaction_SavepointsClearedOnCommit(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.Savepoint("sp1")
	tx.Savepoint("sp2")

	tx.Commit(200)

	if tx.SavepointCount() != 0 {
		t.Errorf("expected 0 savepoints after commit, got %d", tx.SavepointCount())
	}
}

// Test 11: All savepoints cleared on abort
func TestTransaction_SavepointsClearedOnAbort(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.Savepoint("sp1")
	tx.Savepoint("sp2")

	tx.Abort()

	if tx.SavepointCount() != 0 {
		t.Errorf("expected 0 savepoints after abort, got %d", tx.SavepointCount())
	}
}

// Test 12: Track modified keys at savepoint level
func TestTransaction_SavepointTracksModifiedKeys(t *testing.T) {
	tx := NewTransaction(1, 100)

	// Record some modifications before savepoint
	tx.RecordModification("key1")

	tx.Savepoint("sp1")

	// Record more modifications after savepoint
	tx.RecordModification("key2")
	tx.RecordModification("key3")

	// GetModificationsSince should return keys modified after sp1
	mods := tx.GetModificationsSince("sp1")
	if len(mods) != 2 {
		t.Errorf("expected 2 modifications since sp1, got %d", len(mods))
	}
}

// Test 13: Rollback clears modifications since savepoint
func TestTransaction_RollbackClearsModifications(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.RecordModification("key1")
	tx.Savepoint("sp1")
	tx.RecordModification("key2")
	tx.RecordModification("key3")

	err := tx.RollbackTo("sp1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After rollback, only key1 should remain
	allMods := tx.GetAllModifications()
	if len(allMods) != 1 {
		t.Errorf("expected 1 modification after rollback, got %d", len(allMods))
	}
}

// Test 14: Nested savepoints track modifications correctly
func TestTransaction_NestedSavepointModifications(t *testing.T) {
	tx := NewTransaction(1, 100)

	tx.RecordModification("key1")
	tx.Savepoint("sp1")
	tx.RecordModification("key2")
	tx.Savepoint("sp2")
	tx.RecordModification("key3")

	// Rollback to sp1 should remove key2 and key3
	err := tx.RollbackTo("sp1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	allMods := tx.GetAllModifications()
	if len(allMods) != 1 {
		t.Errorf("expected 1 modification after rollback to sp1, got %d", len(allMods))
	}
}
