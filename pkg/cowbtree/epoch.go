// pkg/cowbtree/epoch.go
package cowbtree

import (
	"sync"
	"sync/atomic"
)

// EpochManager provides epoch-based memory reclamation for lock-free data structures.
// It tracks reader epochs to safely determine when old tree versions can be freed.
//
// The algorithm works as follows:
// 1. The global epoch is a monotonically increasing counter
// 2. Readers "enter" an epoch before accessing the tree and "leave" when done
// 3. Writers advance the epoch after making changes
// 4. Old nodes can only be freed when no reader is in an epoch where they were visible
type EpochManager struct {
	// globalEpoch is the current epoch, atomically incremented by writers
	globalEpoch uint64

	// readers tracks active readers and their entry epochs
	readers sync.Map // readerID -> *readerState

	// retired holds nodes retired at each epoch, waiting to be freed
	retiredMu sync.Mutex
	retired   map[uint64][]retiredNode

	// nextReaderID is used to assign unique IDs to readers
	nextReaderID uint64

	// minSafeEpoch caches the minimum safe epoch for reclamation
	minSafeEpoch uint64
}

// readerState tracks a single reader's epoch state
type readerState struct {
	epoch  uint64 // epoch when reader entered (0 = not active)
	active int32  // atomic flag: 1 = active, 0 = inactive
}

// retiredNode represents a node waiting to be reclaimed
type retiredNode struct {
	node     *CowNode
	retireAt uint64
}

// NewEpochManager creates a new epoch manager
func NewEpochManager() *EpochManager {
	return &EpochManager{
		globalEpoch: 1, // Start at 1 so epoch 0 means "not set"
		retired:     make(map[uint64][]retiredNode),
	}
}

// ReaderGuard represents an active reader session
type ReaderGuard struct {
	mgr      *EpochManager
	state    *readerState
	readerID uint64
}

// Enter begins a read operation, recording the current epoch.
// Returns a ReaderGuard that must be released with Leave().
// While the guard is held, the reader is guaranteed to see a consistent
// snapshot of the tree.
func (e *EpochManager) Enter() *ReaderGuard {
	// Get or create reader state
	readerID := atomic.AddUint64(&e.nextReaderID, 1)
	state := &readerState{}

	// Record current epoch before becoming active
	currentEpoch := atomic.LoadUint64(&e.globalEpoch)
	state.epoch = currentEpoch
	atomic.StoreInt32(&state.active, 1)

	e.readers.Store(readerID, state)

	return &ReaderGuard{
		mgr:      e,
		state:    state,
		readerID: readerID,
	}
}

// Leave ends a read operation, allowing epoch advancement
func (g *ReaderGuard) Leave() {
	if g == nil || g.state == nil {
		return
	}

	// Mark as inactive
	atomic.StoreInt32(&g.state.active, 0)
	g.mgr.readers.Delete(g.readerID)
}

// Epoch returns the epoch this reader entered at
func (g *ReaderGuard) Epoch() uint64 {
	if g == nil || g.state == nil {
		return 0
	}
	return g.state.epoch
}

// Advance increments the global epoch and returns the new epoch.
// Called by writers after making changes visible.
func (e *EpochManager) Advance() uint64 {
	return atomic.AddUint64(&e.globalEpoch, 1)
}

// CurrentEpoch returns the current global epoch
func (e *EpochManager) CurrentEpoch() uint64 {
	return atomic.LoadUint64(&e.globalEpoch)
}

// Retire marks a node for later reclamation.
// The node will be freed once all readers that might see it have left.
func (e *EpochManager) Retire(node *CowNode) {
	if node == nil {
		return
	}

	currentEpoch := atomic.LoadUint64(&e.globalEpoch)

	e.retiredMu.Lock()
	e.retired[currentEpoch] = append(e.retired[currentEpoch], retiredNode{
		node:     node,
		retireAt: currentEpoch,
	})
	e.retiredMu.Unlock()
}

// RetireNodes retires multiple nodes at once
func (e *EpochManager) RetireNodes(nodes []*CowNode) {
	if len(nodes) == 0 {
		return
	}

	currentEpoch := atomic.LoadUint64(&e.globalEpoch)

	e.retiredMu.Lock()
	for _, node := range nodes {
		if node != nil {
			e.retired[currentEpoch] = append(e.retired[currentEpoch], retiredNode{
				node:     node,
				retireAt: currentEpoch,
			})
		}
	}
	e.retiredMu.Unlock()
}

// TryReclaim attempts to reclaim nodes that are safe to free.
// Returns the number of nodes reclaimed.
func (e *EpochManager) TryReclaim() int {
	// Find the minimum epoch among all active readers
	minEpoch := e.findMinActiveEpoch()

	// Update cached minimum
	atomic.StoreUint64(&e.minSafeEpoch, minEpoch)

	e.retiredMu.Lock()
	defer e.retiredMu.Unlock()

	reclaimed := 0

	// Free all nodes retired before minEpoch
	for epoch, nodes := range e.retired {
		if epoch < minEpoch {
			reclaimed += len(nodes)
			// Nodes are garbage collected by Go runtime
			// In a lower-level language, we'd call free() here
			delete(e.retired, epoch)
		}
	}

	return reclaimed
}

// findMinActiveEpoch finds the minimum epoch among active readers.
// If no readers are active, returns the current epoch.
func (e *EpochManager) findMinActiveEpoch() uint64 {
	minEpoch := atomic.LoadUint64(&e.globalEpoch)

	e.readers.Range(func(_, value interface{}) bool {
		state := value.(*readerState)
		if atomic.LoadInt32(&state.active) == 1 {
			epoch := state.epoch
			if epoch < minEpoch {
				minEpoch = epoch
			}
		}
		return true
	})

	return minEpoch
}

// PendingCount returns the number of nodes waiting to be reclaimed
func (e *EpochManager) PendingCount() int {
	e.retiredMu.Lock()
	defer e.retiredMu.Unlock()

	count := 0
	for _, nodes := range e.retired {
		count += len(nodes)
	}
	return count
}

// ActiveReaderCount returns the number of active readers
func (e *EpochManager) ActiveReaderCount() int {
	count := 0
	e.readers.Range(func(_, value interface{}) bool {
		state := value.(*readerState)
		if atomic.LoadInt32(&state.active) == 1 {
			count++
		}
		return true
	})
	return count
}
