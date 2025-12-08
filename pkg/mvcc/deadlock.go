// pkg/mvcc/deadlock.go
// Deadlock detection using wait-for graph analysis.
package mvcc

import (
	"errors"
	"sync"
	"time"
)

// ErrDeadlock is returned when a deadlock is detected
var ErrDeadlock = errors.New("deadlock detected")

// DefaultDeadlockTimeout is the default timeout for deadlock detection
const DefaultDeadlockTimeout = 10 * time.Second

// WaitForGraph represents a directed graph of transaction wait relationships.
// An edge from tx1 to tx2 means tx1 is waiting for tx2 to release a lock.
type WaitForGraph struct {
	mu sync.RWMutex
	// edges maps waiter transaction ID to the transaction it's waiting for
	edges map[uint64]*Transaction
	// transactions maps ID to transaction for cycle reporting
	transactions map[uint64]*Transaction
}

// NewWaitForGraph creates a new empty wait-for graph
func NewWaitForGraph() *WaitForGraph {
	return &WaitForGraph{
		edges:        make(map[uint64]*Transaction),
		transactions: make(map[uint64]*Transaction),
	}
}

// AddWait adds a wait edge: waiter is waiting for holder
func (wfg *WaitForGraph) AddWait(waiter, holder *Transaction) {
	wfg.mu.Lock()
	defer wfg.mu.Unlock()

	wfg.edges[waiter.ID()] = holder
	wfg.transactions[waiter.ID()] = waiter
	wfg.transactions[holder.ID()] = holder
}

// RemoveWait removes a wait edge for the given transaction
func (wfg *WaitForGraph) RemoveWait(waiterID uint64) {
	wfg.mu.Lock()
	defer wfg.mu.Unlock()

	delete(wfg.edges, waiterID)
}

// IsWaiting returns true if the transaction is waiting for another
func (wfg *WaitForGraph) IsWaiting(txID uint64) bool {
	wfg.mu.RLock()
	defer wfg.mu.RUnlock()

	_, ok := wfg.edges[txID]
	return ok
}

// RemoveTransaction removes all edges involving the given transaction
func (wfg *WaitForGraph) RemoveTransaction(txID uint64) {
	wfg.mu.Lock()
	defer wfg.mu.Unlock()

	// Remove edge where this tx is waiting
	delete(wfg.edges, txID)

	// Remove edges where other transactions are waiting for this one
	for waiterID, holder := range wfg.edges {
		if holder.ID() == txID {
			delete(wfg.edges, waiterID)
		}
	}

	delete(wfg.transactions, txID)
}

// DetectCycle detects if there is a cycle in the wait-for graph.
// Returns the transactions involved in the cycle, or nil if no cycle exists.
func (wfg *WaitForGraph) DetectCycle() []*Transaction {
	wfg.mu.RLock()
	defer wfg.mu.RUnlock()

	// Use DFS with coloring to detect cycles
	// white (0) = unvisited, gray (1) = in current path, black (2) = finished
	color := make(map[uint64]int)

	var cyclePath []*Transaction

	var dfs func(txID uint64, path []*Transaction) bool
	dfs = func(txID uint64, path []*Transaction) bool {
		if color[txID] == 1 {
			// Found cycle - extract the cycle from the path
			cycleStart := -1
			for i, tx := range path {
				if tx.ID() == txID {
					cycleStart = i
					break
				}
			}
			if cycleStart >= 0 {
				cyclePath = append([]*Transaction{}, path[cycleStart:]...)
			}
			return true
		}

		if color[txID] == 2 {
			return false
		}

		color[txID] = 1 // gray - in current path

		tx := wfg.transactions[txID]
		if tx != nil {
			path = append(path, tx)
		}

		if holder, ok := wfg.edges[txID]; ok {
			if dfs(holder.ID(), path) {
				return true
			}
		}

		color[txID] = 2 // black - finished
		return false
	}

	// Start DFS from each unvisited node
	for txID := range wfg.edges {
		if color[txID] == 0 {
			if dfs(txID, nil) {
				return cyclePath
			}
		}
	}

	return nil
}

// DeadlockDetector detects and resolves deadlocks between transactions
type DeadlockDetector struct {
	mu      sync.RWMutex
	graph   *WaitForGraph
	timeout time.Duration
}

// NewDeadlockDetector creates a new deadlock detector
func NewDeadlockDetector() *DeadlockDetector {
	return &DeadlockDetector{
		graph:   NewWaitForGraph(),
		timeout: DefaultDeadlockTimeout,
	}
}

// SetTimeout sets the deadlock detection timeout
func (dd *DeadlockDetector) SetTimeout(timeout time.Duration) {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	dd.timeout = timeout
}

// GetTimeout returns the current deadlock detection timeout
func (dd *DeadlockDetector) GetTimeout() time.Duration {
	dd.mu.RLock()
	defer dd.mu.RUnlock()
	return dd.timeout
}

// AddWait records that waiter is waiting for holder
func (dd *DeadlockDetector) AddWait(waiter, holder *Transaction) {
	dd.graph.AddWait(waiter, holder)
}

// RemoveWait removes the wait entry for a transaction
func (dd *DeadlockDetector) RemoveWait(waiterID uint64) {
	dd.graph.RemoveWait(waiterID)
}

// IsWaiting returns true if the transaction is waiting
func (dd *DeadlockDetector) IsWaiting(txID uint64) bool {
	return dd.graph.IsWaiting(txID)
}

// OnTransactionEnd cleans up when a transaction commits or aborts
func (dd *DeadlockDetector) OnTransactionEnd(tx *Transaction) {
	dd.graph.RemoveTransaction(tx.ID())
}

// DetectAndSelectVictim detects a deadlock and selects the youngest transaction
// as the victim to abort. Returns nil if no deadlock exists.
func (dd *DeadlockDetector) DetectAndSelectVictim() *Transaction {
	cycle := dd.graph.DetectCycle()
	if cycle == nil || len(cycle) == 0 {
		return nil
	}

	// Select youngest transaction (highest start timestamp) as victim
	var victim *Transaction
	for _, tx := range cycle {
		if victim == nil || tx.StartTS() > victim.StartTS() {
			victim = tx
		}
	}

	return victim
}

// WaitFor attempts to wait for the holder transaction.
// Returns ErrDeadlock if adding this wait would create a deadlock.
func (dd *DeadlockDetector) WaitFor(waiter, holder *Transaction) error {
	// Add the wait edge
	dd.AddWait(waiter, holder)

	// Check for deadlock
	victim := dd.DetectAndSelectVictim()
	if victim != nil {
		// Remove the wait edge since we're not actually waiting
		dd.RemoveWait(waiter.ID())

		// If the victim is the waiter, return deadlock error
		if victim.ID() == waiter.ID() {
			return ErrDeadlock
		}

		// Otherwise, still return deadlock - caller should handle
		return ErrDeadlock
	}

	return nil
}
