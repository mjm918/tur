// pkg/mvcc/manager_test.go
package mvcc

import (
	"sync"
	"testing"
)

func TestTransactionManagerBegin(t *testing.T) {
	mgr := NewTransactionManager()

	tx := mgr.Begin()
	if tx == nil {
		t.Fatal("expected non-nil transaction")
	}

	if tx.ID() == 0 {
		t.Error("expected non-zero transaction ID")
	}

	if tx.StartTS() == 0 {
		t.Error("expected non-zero start timestamp")
	}

	if tx.State() != TxStateActive {
		t.Errorf("expected Active state, got %v", tx.State())
	}
}

func TestTransactionManagerMultipleTransactions(t *testing.T) {
	mgr := NewTransactionManager()

	tx1 := mgr.Begin()
	tx2 := mgr.Begin()

	if tx1.ID() == tx2.ID() {
		t.Error("expected different transaction IDs")
	}

	if tx2.StartTS() < tx1.StartTS() {
		t.Error("expected tx2 startTS >= tx1 startTS")
	}
}

func TestTransactionManagerCommit(t *testing.T) {
	mgr := NewTransactionManager()

	tx := mgr.Begin()
	startTS := tx.StartTS()

	err := mgr.Commit(tx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tx.State() != TxStateCommitted {
		t.Errorf("expected Committed state, got %v", tx.State())
	}

	if tx.CommitTS() <= startTS {
		t.Error("expected commitTS > startTS")
	}
}

func TestTransactionManagerRollback(t *testing.T) {
	mgr := NewTransactionManager()

	tx := mgr.Begin()

	err := mgr.Rollback(tx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tx.State() != TxStateAborted {
		t.Errorf("expected Aborted state, got %v", tx.State())
	}
}

func TestTransactionManagerGetTransaction(t *testing.T) {
	mgr := NewTransactionManager()

	tx := mgr.Begin()
	txID := tx.ID()

	found := mgr.GetTransaction(txID)
	if found == nil {
		t.Fatal("expected to find transaction")
	}

	if found.ID() != txID {
		t.Errorf("expected ID %d, got %d", txID, found.ID())
	}
}

func TestTransactionManagerGetTransactionNotFound(t *testing.T) {
	mgr := NewTransactionManager()

	found := mgr.GetTransaction(99999)
	if found != nil {
		t.Error("expected nil for non-existent transaction")
	}
}

func TestTransactionManagerActiveTransactions(t *testing.T) {
	mgr := NewTransactionManager()

	tx1 := mgr.Begin()
	tx2 := mgr.Begin()
	tx3 := mgr.Begin()

	active := mgr.ActiveTransactions()
	if len(active) != 3 {
		t.Errorf("expected 3 active transactions, got %d", len(active))
	}

	mgr.Commit(tx1)

	active = mgr.ActiveTransactions()
	if len(active) != 2 {
		t.Errorf("expected 2 active transactions, got %d", len(active))
	}

	mgr.Rollback(tx2)

	active = mgr.ActiveTransactions()
	if len(active) != 1 {
		t.Errorf("expected 1 active transaction, got %d", len(active))
	}

	// Check it's tx3
	if active[0].ID() != tx3.ID() {
		t.Errorf("expected tx3, got tx with ID %d", active[0].ID())
	}
}

func TestTransactionManagerConcurrentBegin(t *testing.T) {
	mgr := NewTransactionManager()

	var wg sync.WaitGroup
	txIDs := make(chan uint64, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := mgr.Begin()
			txIDs <- tx.ID()
		}()
	}

	wg.Wait()
	close(txIDs)

	// Check all IDs are unique
	seen := make(map[uint64]bool)
	for id := range txIDs {
		if seen[id] {
			t.Errorf("duplicate transaction ID: %d", id)
		}
		seen[id] = true
	}

	if len(seen) != 100 {
		t.Errorf("expected 100 unique IDs, got %d", len(seen))
	}
}

func TestTransactionManagerCurrentTimestamp(t *testing.T) {
	mgr := NewTransactionManager()

	ts1 := mgr.CurrentTimestamp()
	ts2 := mgr.CurrentTimestamp()

	if ts2 < ts1 {
		t.Error("timestamp should not decrease")
	}
}

func TestTransactionManagerMinActiveTimestamp(t *testing.T) {
	mgr := NewTransactionManager()

	// No active transactions - should return max uint64
	minTS := mgr.MinActiveTimestamp()
	if minTS != ^uint64(0) {
		t.Errorf("expected max uint64 with no active transactions, got %d", minTS)
	}

	tx1 := mgr.Begin()
	tx1StartTS := tx1.StartTS()

	// Start another transaction with higher timestamp
	tx2 := mgr.Begin()

	minTS = mgr.MinActiveTimestamp()
	if minTS != tx1StartTS {
		t.Errorf("expected min timestamp %d, got %d", tx1StartTS, minTS)
	}

	// Commit tx1, min should now be tx2's start
	mgr.Commit(tx1)
	minTS = mgr.MinActiveTimestamp()
	if minTS != tx2.StartTS() {
		t.Errorf("expected min timestamp %d, got %d", tx2.StartTS(), minTS)
	}
}

func TestTransactionManagerCommitInactiveTransaction(t *testing.T) {
	mgr := NewTransactionManager()

	tx := mgr.Begin()
	mgr.Commit(tx)

	// Try to commit again
	err := mgr.Commit(tx)
	if err != ErrTxNotActive {
		t.Errorf("expected ErrTxNotActive, got %v", err)
	}
}

func TestTransactionManagerRollbackInactiveTransaction(t *testing.T) {
	mgr := NewTransactionManager()

	tx := mgr.Begin()
	mgr.Commit(tx)

	// Try to rollback committed transaction
	err := mgr.Rollback(tx)
	if err != ErrTxNotActive {
		t.Errorf("expected ErrTxNotActive, got %v", err)
	}
}
