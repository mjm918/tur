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
