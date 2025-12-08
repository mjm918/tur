// pkg/mvcc/conflict_test.go
package mvcc

import (
	"testing"
)

func TestWriteSetCreate(t *testing.T) {
	ws := NewWriteSet()

	if ws == nil {
		t.Fatal("expected non-nil WriteSet")
	}

	if len(ws.Keys()) != 0 {
		t.Errorf("expected empty write set, got %d keys", len(ws.Keys()))
	}
}

func TestWriteSetAddKey(t *testing.T) {
	ws := NewWriteSet()

	ws.Add([]byte("key1"))
	ws.Add([]byte("key2"))

	keys := ws.Keys()
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}

	if !ws.Contains([]byte("key1")) {
		t.Error("expected key1 to be in write set")
	}
	if !ws.Contains([]byte("key2")) {
		t.Error("expected key2 to be in write set")
	}
	if ws.Contains([]byte("key3")) {
		t.Error("expected key3 to NOT be in write set")
	}
}

func TestWriteSetDuplicateKey(t *testing.T) {
	ws := NewWriteSet()

	ws.Add([]byte("key1"))
	ws.Add([]byte("key1"))

	if len(ws.Keys()) != 1 {
		t.Errorf("expected 1 key after duplicate, got %d", len(ws.Keys()))
	}
}

func TestConflictDetectorNoConflict(t *testing.T) {
	mgr := NewTransactionManager()
	detector := NewConflictDetector()

	tx1 := mgr.Begin()
	tx2 := mgr.Begin()

	ws1 := NewWriteSet()
	ws1.Add([]byte("key1"))
	ws1.Add([]byte("key2"))

	ws2 := NewWriteSet()
	ws2.Add([]byte("key3"))
	ws2.Add([]byte("key4"))

	// Register tx1's writes
	detector.RegisterWrites(tx1, ws1)

	// tx2 writes to different keys - no conflict
	err := detector.CheckConflict(tx2, ws2)
	if err != nil {
		t.Errorf("expected no conflict, got %v", err)
	}
}

func TestConflictDetectorWriteWriteConflict(t *testing.T) {
	mgr := NewTransactionManager()
	detector := NewConflictDetector()

	tx1 := mgr.Begin()
	tx2 := mgr.Begin()

	ws1 := NewWriteSet()
	ws1.Add([]byte("key1"))
	ws1.Add([]byte("key2"))

	ws2 := NewWriteSet()
	ws2.Add([]byte("key2")) // Same key as tx1
	ws2.Add([]byte("key3"))

	// Register tx1's writes
	detector.RegisterWrites(tx1, ws1)

	// tx2 tries to write to key2 - conflict!
	err := detector.CheckConflict(tx2, ws2)
	if err != ErrWriteConflict {
		t.Errorf("expected ErrWriteConflict, got %v", err)
	}
}

func TestConflictDetectorNoConflictAfterCommit(t *testing.T) {
	mgr := NewTransactionManager()
	detector := NewConflictDetector()

	tx1 := mgr.Begin()
	ws1 := NewWriteSet()
	ws1.Add([]byte("key1"))
	detector.RegisterWrites(tx1, ws1)

	// Commit tx1
	mgr.Commit(tx1)
	detector.OnCommit(tx1)

	// tx2 starts after commit - should NOT conflict
	tx2 := mgr.Begin()
	ws2 := NewWriteSet()
	ws2.Add([]byte("key1"))

	err := detector.CheckConflict(tx2, ws2)
	if err != nil {
		t.Errorf("expected no conflict after commit, got %v", err)
	}
}

func TestConflictDetectorNoConflictAfterAbort(t *testing.T) {
	mgr := NewTransactionManager()
	detector := NewConflictDetector()

	tx1 := mgr.Begin()
	ws1 := NewWriteSet()
	ws1.Add([]byte("key1"))
	detector.RegisterWrites(tx1, ws1)

	// Abort tx1
	mgr.Rollback(tx1)
	detector.OnAbort(tx1)

	// tx2 should NOT conflict (tx1 was aborted)
	tx2 := mgr.Begin()
	ws2 := NewWriteSet()
	ws2.Add([]byte("key1"))

	err := detector.CheckConflict(tx2, ws2)
	if err != nil {
		t.Errorf("expected no conflict after abort, got %v", err)
	}
}

func TestConflictDetectorOwnTransactionNoConflict(t *testing.T) {
	mgr := NewTransactionManager()
	detector := NewConflictDetector()

	tx1 := mgr.Begin()
	ws1 := NewWriteSet()
	ws1.Add([]byte("key1"))
	detector.RegisterWrites(tx1, ws1)

	// Same transaction checking its own keys - should NOT conflict
	err := detector.CheckConflict(tx1, ws1)
	if err != nil {
		t.Errorf("expected no conflict with own transaction, got %v", err)
	}
}

func TestConflictDetectorMultipleTransactions(t *testing.T) {
	mgr := NewTransactionManager()
	detector := NewConflictDetector()

	tx1 := mgr.Begin()
	tx2 := mgr.Begin()
	tx3 := mgr.Begin()

	ws1 := NewWriteSet()
	ws1.Add([]byte("key1"))
	detector.RegisterWrites(tx1, ws1)

	ws2 := NewWriteSet()
	ws2.Add([]byte("key2"))
	detector.RegisterWrites(tx2, ws2)

	// tx3 conflicts with tx1
	ws3a := NewWriteSet()
	ws3a.Add([]byte("key1"))
	err := detector.CheckConflict(tx3, ws3a)
	if err != ErrWriteConflict {
		t.Errorf("expected conflict with tx1, got %v", err)
	}

	// tx3 conflicts with tx2
	ws3b := NewWriteSet()
	ws3b.Add([]byte("key2"))
	err = detector.CheckConflict(tx3, ws3b)
	if err != ErrWriteConflict {
		t.Errorf("expected conflict with tx2, got %v", err)
	}

	// tx3 doesn't conflict with key3
	ws3c := NewWriteSet()
	ws3c.Add([]byte("key3"))
	err = detector.CheckConflict(tx3, ws3c)
	if err != nil {
		t.Errorf("expected no conflict with key3, got %v", err)
	}
}

func TestConflictingTransaction(t *testing.T) {
	mgr := NewTransactionManager()
	detector := NewConflictDetector()

	tx1 := mgr.Begin()
	ws1 := NewWriteSet()
	ws1.Add([]byte("key1"))
	detector.RegisterWrites(tx1, ws1)

	tx2 := mgr.Begin()
	ws2 := NewWriteSet()
	ws2.Add([]byte("key1"))

	// Check for conflict and get conflicting transaction
	conflictingTx := detector.FindConflictingTransaction(tx2, ws2)
	if conflictingTx == nil {
		t.Fatal("expected to find conflicting transaction")
	}
	if conflictingTx.ID() != tx1.ID() {
		t.Errorf("expected conflicting transaction to be tx1, got tx%d", conflictingTx.ID())
	}
}

func TestReadSetTracking(t *testing.T) {
	rs := NewReadSet()

	rs.Add([]byte("key1"))
	rs.Add([]byte("key2"))

	if !rs.Contains([]byte("key1")) {
		t.Error("expected key1 in read set")
	}
	if !rs.Contains([]byte("key2")) {
		t.Error("expected key2 in read set")
	}
	if rs.Contains([]byte("key3")) {
		t.Error("expected key3 NOT in read set")
	}
}
