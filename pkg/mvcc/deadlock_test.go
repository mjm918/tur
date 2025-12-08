// pkg/mvcc/deadlock_test.go
package mvcc

import (
	"testing"
	"time"
)

// ============ Wait-For Graph Tests ============

// Test 1: Empty wait-for graph has no cycles
func TestWaitForGraph_EmptyNoCycle(t *testing.T) {
	wfg := NewWaitForGraph()

	cycle := wfg.DetectCycle()
	if cycle != nil {
		t.Error("expected no cycle in empty graph")
	}
}

// Test 2: Add wait edge between transactions
func TestWaitForGraph_AddWaitEdge(t *testing.T) {
	wfg := NewWaitForGraph()

	tx1 := NewTransaction(1, 100)
	tx2 := NewTransaction(2, 200)

	wfg.AddWait(tx1, tx2) // tx1 waits for tx2

	if !wfg.IsWaiting(tx1.ID()) {
		t.Error("expected tx1 to be waiting")
	}
}

// Test 3: Remove wait edge
func TestWaitForGraph_RemoveWait(t *testing.T) {
	wfg := NewWaitForGraph()

	tx1 := NewTransaction(1, 100)
	tx2 := NewTransaction(2, 200)

	wfg.AddWait(tx1, tx2)
	wfg.RemoveWait(tx1.ID())

	if wfg.IsWaiting(tx1.ID()) {
		t.Error("expected tx1 to not be waiting after removal")
	}
}

// Test 4: Simple cycle detection (A -> B -> A)
func TestWaitForGraph_SimpleCycle(t *testing.T) {
	wfg := NewWaitForGraph()

	tx1 := NewTransaction(1, 100)
	tx2 := NewTransaction(2, 200)

	wfg.AddWait(tx1, tx2) // tx1 waits for tx2
	wfg.AddWait(tx2, tx1) // tx2 waits for tx1 (cycle!)

	cycle := wfg.DetectCycle()
	if cycle == nil {
		t.Fatal("expected to detect cycle")
	}
	if len(cycle) < 2 {
		t.Errorf("expected cycle with at least 2 transactions, got %d", len(cycle))
	}
}

// Test 5: Longer cycle detection (A -> B -> C -> A)
func TestWaitForGraph_LongerCycle(t *testing.T) {
	wfg := NewWaitForGraph()

	tx1 := NewTransaction(1, 100)
	tx2 := NewTransaction(2, 200)
	tx3 := NewTransaction(3, 300)

	wfg.AddWait(tx1, tx2) // tx1 waits for tx2
	wfg.AddWait(tx2, tx3) // tx2 waits for tx3
	wfg.AddWait(tx3, tx1) // tx3 waits for tx1 (cycle!)

	cycle := wfg.DetectCycle()
	if cycle == nil {
		t.Fatal("expected to detect cycle")
	}
	if len(cycle) < 3 {
		t.Errorf("expected cycle with at least 3 transactions, got %d", len(cycle))
	}
}

// Test 6: No cycle in chain (A -> B -> C)
func TestWaitForGraph_NoCycleInChain(t *testing.T) {
	wfg := NewWaitForGraph()

	tx1 := NewTransaction(1, 100)
	tx2 := NewTransaction(2, 200)
	tx3 := NewTransaction(3, 300)

	wfg.AddWait(tx1, tx2) // tx1 waits for tx2
	wfg.AddWait(tx2, tx3) // tx2 waits for tx3

	cycle := wfg.DetectCycle()
	if cycle != nil {
		t.Error("expected no cycle in chain without back edge")
	}
}

// Test 7: Remove transaction clears all edges
func TestWaitForGraph_RemoveTransaction(t *testing.T) {
	wfg := NewWaitForGraph()

	tx1 := NewTransaction(1, 100)
	tx2 := NewTransaction(2, 200)
	tx3 := NewTransaction(3, 300)

	wfg.AddWait(tx1, tx2)
	wfg.AddWait(tx3, tx2)

	wfg.RemoveTransaction(tx2.ID())

	// tx1 and tx3 should no longer be waiting (their target is gone)
	if wfg.IsWaiting(tx1.ID()) {
		t.Error("expected tx1 to not be waiting after tx2 removed")
	}
}

// ============ Deadlock Detector Tests ============

// Test 8: Deadlock detector selects youngest transaction as victim
func TestDeadlockDetector_SelectYoungestVictim(t *testing.T) {
	dd := NewDeadlockDetector()

	tx1 := NewTransaction(1, 100) // oldest
	tx2 := NewTransaction(2, 200)
	tx3 := NewTransaction(3, 300) // youngest

	dd.AddWait(tx1, tx2)
	dd.AddWait(tx2, tx3)
	dd.AddWait(tx3, tx1) // cycle!

	victim := dd.DetectAndSelectVictim()
	if victim == nil {
		t.Fatal("expected to detect deadlock and select victim")
	}
	// Youngest transaction (highest ID or startTS) should be victim
	if victim.ID() != 3 {
		t.Errorf("expected youngest tx (ID=3) as victim, got ID=%d", victim.ID())
	}
}

// Test 9: No deadlock returns nil victim
func TestDeadlockDetector_NoDeadlock(t *testing.T) {
	dd := NewDeadlockDetector()

	tx1 := NewTransaction(1, 100)
	tx2 := NewTransaction(2, 200)

	dd.AddWait(tx1, tx2)

	victim := dd.DetectAndSelectVictim()
	if victim != nil {
		t.Error("expected no victim when there's no deadlock")
	}
}

// Test 10: Deadlock error type
func TestErrDeadlock(t *testing.T) {
	if ErrDeadlock == nil {
		t.Error("expected ErrDeadlock to be defined")
	}
	if ErrDeadlock.Error() != "deadlock detected" {
		t.Errorf("unexpected error message: %s", ErrDeadlock.Error())
	}
}

// Test 11: Deadlock detector with timeout configuration
func TestDeadlockDetector_TimeoutConfig(t *testing.T) {
	dd := NewDeadlockDetector()

	// Default timeout should be set
	if dd.GetTimeout() == 0 {
		t.Error("expected non-zero default timeout")
	}

	// Set custom timeout
	dd.SetTimeout(5 * time.Second)
	if dd.GetTimeout() != 5*time.Second {
		t.Errorf("expected 5s timeout, got %v", dd.GetTimeout())
	}
}

// Test 12: WaitWithTimeout returns deadlock error on cycle
func TestDeadlockDetector_WaitWithTimeout(t *testing.T) {
	dd := NewDeadlockDetector()
	dd.SetTimeout(100 * time.Millisecond)

	tx1 := NewTransaction(1, 100)
	tx2 := NewTransaction(2, 200)

	// Create immediate deadlock
	dd.AddWait(tx2, tx1)

	// tx1 trying to wait for tx2 should detect deadlock immediately
	err := dd.WaitFor(tx1, tx2)
	if err != ErrDeadlock {
		t.Errorf("expected ErrDeadlock, got %v", err)
	}
}

// Test 13: Concurrent access to deadlock detector
func TestDeadlockDetector_ConcurrentAccess(t *testing.T) {
	dd := NewDeadlockDetector()

	done := make(chan bool)

	// Multiple goroutines adding waits
	for i := 0; i < 10; i++ {
		go func(id uint64) {
			tx1 := NewTransaction(id, id*100)
			tx2 := NewTransaction(id+100, (id+100)*100)
			dd.AddWait(tx1, tx2)
			dd.RemoveWait(tx1.ID())
			done <- true
		}(uint64(i))
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should not panic or deadlock
}

// Test 14: Clear all waits when transaction commits/aborts
func TestDeadlockDetector_OnTransactionEnd(t *testing.T) {
	dd := NewDeadlockDetector()

	tx1 := NewTransaction(1, 100)
	tx2 := NewTransaction(2, 200)
	tx3 := NewTransaction(3, 300)

	dd.AddWait(tx1, tx2)
	dd.AddWait(tx3, tx2)

	// When tx2 commits, all transactions waiting for it should be notified
	dd.OnTransactionEnd(tx2)

	if dd.IsWaiting(tx1.ID()) {
		t.Error("expected tx1 to not be waiting after tx2 ended")
	}
	if dd.IsWaiting(tx3.ID()) {
		t.Error("expected tx3 to not be waiting after tx2 ended")
	}
}
