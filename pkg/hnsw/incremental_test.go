// pkg/hnsw/incremental_test.go
package hnsw

import (
	"testing"

	"tur/pkg/types"
)

func TestChangeLog_RecordsInsertOperation(t *testing.T) {
	// Create a change log
	log := NewChangeLog()

	// Record an insert operation
	vec := types.NewVector([]float32{1.0, 2.0, 3.0})
	log.RecordInsert(1, 100, vec)

	// Verify the operation is recorded
	ops := log.Operations()
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(ops))
	}

	op := ops[0]
	if op.Type != OpInsert {
		t.Errorf("expected OpInsert, got %v", op.Type)
	}
	if op.NodeID != 1 {
		t.Errorf("expected nodeID 1, got %d", op.NodeID)
	}
	if op.RowID != 100 {
		t.Errorf("expected rowID 100, got %d", op.RowID)
	}
	if op.Vector == nil {
		t.Error("expected vector to be set")
	}
}

func TestChangeLog_RecordsDeleteOperation(t *testing.T) {
	log := NewChangeLog()

	// Record a delete operation
	log.RecordDelete(1, 100)

	ops := log.Operations()
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(ops))
	}

	op := ops[0]
	if op.Type != OpDelete {
		t.Errorf("expected OpDelete, got %v", op.Type)
	}
	if op.NodeID != 1 {
		t.Errorf("expected nodeID 1, got %d", op.NodeID)
	}
	if op.RowID != 100 {
		t.Errorf("expected rowID 100, got %d", op.RowID)
	}
}

func TestChangeLog_RecordsUpdateOperation(t *testing.T) {
	log := NewChangeLog()

	// Record an update operation
	oldVec := types.NewVector([]float32{1.0, 2.0, 3.0})
	newVec := types.NewVector([]float32{4.0, 5.0, 6.0})
	log.RecordUpdate(1, 100, oldVec, newVec)

	ops := log.Operations()
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(ops))
	}

	op := ops[0]
	if op.Type != OpUpdate {
		t.Errorf("expected OpUpdate, got %v", op.Type)
	}
	if op.NodeID != 1 {
		t.Errorf("expected nodeID 1, got %d", op.NodeID)
	}
	if op.OldVector == nil || op.Vector == nil {
		t.Error("expected both old and new vectors to be set")
	}
}

func TestChangeLog_TracksSequenceNumbers(t *testing.T) {
	log := NewChangeLog()

	vec := types.NewVector([]float32{1.0, 2.0, 3.0})
	log.RecordInsert(1, 100, vec)
	log.RecordDelete(2, 200)
	log.RecordInsert(3, 300, vec)

	ops := log.Operations()
	if len(ops) != 3 {
		t.Fatalf("expected 3 operations, got %d", len(ops))
	}

	// Sequence numbers should be monotonically increasing
	for i := 0; i < len(ops)-1; i++ {
		if ops[i].Seq >= ops[i+1].Seq {
			t.Errorf("sequence numbers not increasing: %d >= %d", ops[i].Seq, ops[i+1].Seq)
		}
	}
}

func TestChangeLog_Size(t *testing.T) {
	log := NewChangeLog()

	if log.Size() != 0 {
		t.Errorf("expected size 0, got %d", log.Size())
	}

	vec := types.NewVector([]float32{1.0, 2.0, 3.0})
	log.RecordInsert(1, 100, vec)
	log.RecordDelete(2, 200)

	if log.Size() != 2 {
		t.Errorf("expected size 2, got %d", log.Size())
	}
}

func TestChangeLog_Clear(t *testing.T) {
	log := NewChangeLog()

	vec := types.NewVector([]float32{1.0, 2.0, 3.0})
	log.RecordInsert(1, 100, vec)
	log.RecordDelete(2, 200)

	log.Clear()

	if log.Size() != 0 {
		t.Errorf("expected size 0 after clear, got %d", log.Size())
	}
}

func TestChangeLog_LastSeq(t *testing.T) {
	log := NewChangeLog()

	// Initially zero
	if log.LastSeq() != 0 {
		t.Errorf("expected lastSeq 0, got %d", log.LastSeq())
	}

	vec := types.NewVector([]float32{1.0, 2.0, 3.0})
	log.RecordInsert(1, 100, vec)
	log.RecordDelete(2, 200)

	lastSeq := log.LastSeq()
	if lastSeq == 0 {
		t.Error("expected lastSeq > 0")
	}

	ops := log.Operations()
	if ops[len(ops)-1].Seq != lastSeq {
		t.Errorf("lastSeq should match last operation's seq")
	}
}

func TestChangeLog_OperationsSince(t *testing.T) {
	log := NewChangeLog()

	vec := types.NewVector([]float32{1.0, 2.0, 3.0})
	log.RecordInsert(1, 100, vec)
	seq1 := log.LastSeq()
	log.RecordDelete(2, 200)
	log.RecordInsert(3, 300, vec)

	// Get operations since seq1
	ops := log.OperationsSince(seq1)
	if len(ops) != 2 {
		t.Fatalf("expected 2 operations since seq1, got %d", len(ops))
	}

	// First operation should be the delete (seq > seq1)
	if ops[0].Type != OpDelete {
		t.Errorf("expected first op to be delete, got %v", ops[0].Type)
	}
}

// Tests for IncrementalIndex - an index that tracks changes for efficient merge

func TestIncrementalIndex_InsertAndSearch(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	// Insert some vectors
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})
	v3 := types.NewVector([]float32{0.9, 0.1, 0.0})

	if err := idx.Insert(1, v1); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if err := idx.Insert(2, v2); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if err := idx.Insert(3, v3); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Search for similar to v1
	results, err := idx.SearchKNN(v1, 2)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// v1 and v3 should be most similar
	if results[0].RowID != 1 {
		t.Errorf("expected rowID 1 as closest, got %d", results[0].RowID)
	}
}

func TestIncrementalIndex_TracksChanges(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	// Insert should record in change log
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	idx.Insert(1, v1)

	if idx.PendingChanges() != 1 {
		t.Errorf("expected 1 pending change, got %d", idx.PendingChanges())
	}

	// Delete should record in change log
	idx.Delete(1)

	if idx.PendingChanges() != 2 {
		t.Errorf("expected 2 pending changes, got %d", idx.PendingChanges())
	}
}

func TestIncrementalIndex_GetDelta(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	// Record initial state
	initialSeq := idx.LastSeq()

	// Make some changes
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})
	idx.Insert(1, v1)
	idx.Insert(2, v2)
	idx.Delete(1)

	// Get delta since initial
	ops := idx.OperationsSince(initialSeq)
	if len(ops) != 3 {
		t.Fatalf("expected 3 operations in delta, got %d", len(ops))
	}

	// Verify operation types
	if ops[0].Type != OpInsert {
		t.Errorf("expected first op to be insert, got %v", ops[0].Type)
	}
	if ops[1].Type != OpInsert {
		t.Errorf("expected second op to be insert, got %v", ops[1].Type)
	}
	if ops[2].Type != OpDelete {
		t.Errorf("expected third op to be delete, got %v", ops[2].Type)
	}
}

func TestIncrementalIndex_Snapshot(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	// Insert some vectors
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})
	idx.Insert(1, v1)
	idx.Insert(2, v2)

	// Create a snapshot
	snapshot := idx.Snapshot()

	if snapshot.Version == 0 {
		t.Error("expected snapshot version > 0")
	}

	if snapshot.NodeCount != 2 {
		t.Errorf("expected 2 nodes in snapshot, got %d", snapshot.NodeCount)
	}
}

func TestIncrementalIndex_ApplyDelta(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	// Create base index
	baseIdx := NewIncrementalIndex(config)
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	baseIdx.Insert(1, v1)

	// Create delta operations
	delta := []Operation{
		{Type: OpInsert, NodeID: 2, RowID: 2, Vector: types.NewVector([]float32{0.0, 1.0, 0.0})},
		{Type: OpInsert, NodeID: 3, RowID: 3, Vector: types.NewVector([]float32{0.0, 0.0, 1.0})},
	}

	// Apply delta
	err := baseIdx.ApplyDelta(delta)
	if err != nil {
		t.Fatalf("apply delta failed: %v", err)
	}

	// Verify nodes were added
	if baseIdx.Len() != 3 {
		t.Errorf("expected 3 nodes after delta, got %d", baseIdx.Len())
	}

	// Verify search still works
	results, _ := baseIdx.SearchKNN(v1, 3)
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}

func TestIncrementalIndex_ClearPendingChanges(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	idx.Insert(1, v1)
	idx.Insert(2, v1)

	if idx.PendingChanges() != 2 {
		t.Errorf("expected 2 pending changes, got %d", idx.PendingChanges())
	}

	// Clear pending changes (after merge)
	idx.ClearPendingChanges()

	if idx.PendingChanges() != 0 {
		t.Errorf("expected 0 pending changes after clear, got %d", idx.PendingChanges())
	}
}

// Tests for Version Tracking

func TestIncrementalIndex_VersionIncrementsOnChange(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	// Initial version should be 0
	if idx.Version() != 0 {
		t.Errorf("expected initial version 0, got %d", idx.Version())
	}

	// Insert should increment version
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	idx.Insert(1, v1)

	if idx.Version() != 1 {
		t.Errorf("expected version 1 after insert, got %d", idx.Version())
	}

	// Delete should increment version
	idx.Delete(1)

	if idx.Version() != 2 {
		t.Errorf("expected version 2 after delete, got %d", idx.Version())
	}
}

func TestIncrementalIndex_SnapshotContainsVersion(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	idx.Insert(1, v1)
	idx.Insert(2, v1)

	snapshot := idx.Snapshot()

	// Snapshot version should match last sequence
	if snapshot.Version != idx.LastSeq() {
		t.Errorf("snapshot version %d doesn't match lastSeq %d", snapshot.Version, idx.LastSeq())
	}
}

func TestIncrementalIndex_CreateCheckpoint(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})
	idx.Insert(1, v1)
	idx.Insert(2, v2)

	// Create a checkpoint
	checkpoint := idx.CreateCheckpoint()

	if checkpoint.Version == 0 {
		t.Error("expected checkpoint version > 0")
	}

	if checkpoint.Timestamp == 0 {
		t.Error("expected checkpoint timestamp > 0")
	}

	if checkpoint.NodeCount != 2 {
		t.Errorf("expected 2 nodes in checkpoint, got %d", checkpoint.NodeCount)
	}

	// Make more changes
	idx.Insert(3, v1)

	// Version should have advanced
	if idx.Version() <= checkpoint.Version {
		t.Error("version should have advanced after checkpoint")
	}
}

func TestIncrementalIndex_RestoreToCheckpoint(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})
	idx.Insert(1, v1)
	idx.Insert(2, v2)

	// Create checkpoint
	checkpoint := idx.CreateCheckpoint()

	// Make more changes
	v3 := types.NewVector([]float32{0.0, 0.0, 1.0})
	idx.Insert(3, v3)
	idx.Delete(1)

	// Verify current state
	if idx.Len() != 2 {
		t.Errorf("expected 2 nodes before restore, got %d", idx.Len())
	}

	// Get operations since checkpoint
	ops := idx.OperationsSince(checkpoint.Version)
	if len(ops) != 2 {
		t.Errorf("expected 2 operations since checkpoint, got %d", len(ops))
	}
}

func TestIncrementalIndex_CheckpointHistory(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})

	// Create multiple checkpoints
	idx.Insert(1, v1)
	cp1 := idx.CreateCheckpoint()

	idx.Insert(2, v1)
	cp2 := idx.CreateCheckpoint()

	idx.Insert(3, v1)
	cp3 := idx.CreateCheckpoint()

	// Get checkpoint history
	history := idx.CheckpointHistory()

	if len(history) != 3 {
		t.Fatalf("expected 3 checkpoints, got %d", len(history))
	}

	// Checkpoints should be in order
	if history[0].Version != cp1.Version {
		t.Error("checkpoint history not in order")
	}
	if history[1].Version != cp2.Version {
		t.Error("checkpoint history not in order")
	}
	if history[2].Version != cp3.Version {
		t.Error("checkpoint history not in order")
	}
}

func TestIncrementalIndex_GetOperationsBetweenCheckpoints(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})

	idx.Insert(1, v1)
	cp1 := idx.CreateCheckpoint()

	idx.Insert(2, v1)
	idx.Insert(3, v1)
	idx.Delete(2)
	cp2 := idx.CreateCheckpoint()

	// Get operations between checkpoints
	ops := idx.OperationsBetween(cp1.Version, cp2.Version)

	if len(ops) != 3 {
		t.Fatalf("expected 3 operations between checkpoints, got %d", len(ops))
	}
}

// Tests for Memory Optimization

func TestChangeLog_CompactDuplicates(t *testing.T) {
	log := NewChangeLog()

	// Insert then delete same row - should compact to nothing
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	log.RecordInsert(1, 100, v1)
	log.RecordDelete(1, 100)

	// Compact the log
	log.Compact()

	// After compaction, insert-delete pair should be removed
	ops := log.Operations()
	if len(ops) != 0 {
		t.Errorf("expected 0 operations after compact, got %d", len(ops))
	}
}

func TestChangeLog_CompactPreservesNetChanges(t *testing.T) {
	log := NewChangeLog()

	// Insert, update, then final state should just be final insert
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})
	v3 := types.NewVector([]float32{0.0, 0.0, 1.0})

	log.RecordInsert(1, 100, v1)
	log.RecordUpdate(1, 100, v1, v2)
	log.RecordUpdate(1, 100, v2, v3)

	// Before compaction
	if log.Size() != 3 {
		t.Fatalf("expected 3 operations before compact, got %d", log.Size())
	}

	// Compact the log
	log.Compact()

	// After compaction, should have single insert with final vector
	ops := log.Operations()
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation after compact, got %d", len(ops))
	}

	if ops[0].Type != OpInsert {
		t.Errorf("expected OpInsert, got %v", ops[0].Type)
	}

	// Should have the final vector
	if ops[0].Vector == nil {
		t.Error("expected vector to be set")
	}
}

func TestChangeLog_CompactMultipleRows(t *testing.T) {
	log := NewChangeLog()

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})

	// Row 100: insert, update, delete = net nothing
	log.RecordInsert(1, 100, v1)
	log.RecordUpdate(1, 100, v1, v2)
	log.RecordDelete(1, 100)

	// Row 200: insert only = net insert
	log.RecordInsert(2, 200, v1)

	// Row 300: insert, update = net insert with final vector
	log.RecordInsert(3, 300, v1)
	log.RecordUpdate(3, 300, v1, v2)

	log.Compact()

	ops := log.Operations()
	if len(ops) != 2 {
		t.Fatalf("expected 2 operations after compact, got %d", len(ops))
	}
}

func TestChangeLog_EstimateMemoryUsage(t *testing.T) {
	log := NewChangeLog()

	// Should have some base memory
	baseUsage := log.EstimateMemoryUsage()
	if baseUsage == 0 {
		t.Error("expected non-zero base memory usage")
	}

	// Add operations - memory should grow
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	for i := 0; i < 100; i++ {
		log.RecordInsert(uint64(i), int64(i), v1)
	}

	usageAfter := log.EstimateMemoryUsage()
	if usageAfter <= baseUsage {
		t.Errorf("expected memory to grow after inserts, base=%d after=%d", baseUsage, usageAfter)
	}
}

func TestIncrementalIndex_CompactChangeLog(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	idx := NewIncrementalIndex(config)

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})

	// Make changes that cancel out
	idx.Insert(1, v1)
	idx.Delete(1)
	idx.Insert(2, v2)

	if idx.PendingChanges() != 3 {
		t.Errorf("expected 3 pending changes before compact, got %d", idx.PendingChanges())
	}

	// Compact the change log
	idx.CompactChangeLog()

	// Should only have 1 net change (insert 2)
	if idx.PendingChanges() != 1 {
		t.Errorf("expected 1 pending change after compact, got %d", idx.PendingChanges())
	}
}

func TestChangeLog_TruncateOlderThan(t *testing.T) {
	log := NewChangeLog()

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})

	// Add some operations
	log.RecordInsert(1, 100, v1)
	log.RecordInsert(2, 200, v1)
	seq := log.LastSeq()
	log.RecordInsert(3, 300, v1)
	log.RecordInsert(4, 400, v1)

	if log.Size() != 4 {
		t.Fatalf("expected 4 operations, got %d", log.Size())
	}

	// Truncate operations older than seq
	log.TruncateOlderThan(seq)

	// Should only have operations after seq
	ops := log.Operations()
	if len(ops) != 2 {
		t.Fatalf("expected 2 operations after truncate, got %d", len(ops))
	}

	// All remaining operations should have seq > the truncate point
	for _, op := range ops {
		if op.Seq <= seq {
			t.Errorf("found operation with seq %d <= truncate seq %d", op.Seq, seq)
		}
	}
}

// Tests for Incremental Update Correctness vs Full Rebuild

func TestIncrementalIndex_CorrectnessVsFullRebuild_Simple(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	// Create vectors
	vectors := make([]*types.Vector, 10)
	for i := 0; i < 10; i++ {
		vectors[i] = types.NewVector([]float32{
			float32(i) * 0.1,
			float32(i) * 0.2,
			float32(i) * 0.3,
		})
	}

	// Method 1: Build index incrementally using IncrementalIndex
	incIdx := NewIncrementalIndex(config)
	for i := 0; i < 10; i++ {
		incIdx.Insert(int64(i), vectors[i])
	}

	// Method 2: Build index from scratch using regular Index
	fullIdx := NewIndex(config)
	for i := 0; i < 10; i++ {
		fullIdx.Insert(int64(i), vectors[i])
	}

	// Both should have same number of nodes
	if incIdx.Len() != fullIdx.Len() {
		t.Errorf("length mismatch: incremental=%d, full=%d", incIdx.Len(), fullIdx.Len())
	}

	// Search should return same results (same vectors)
	query := vectors[5]

	incResults, _ := incIdx.SearchKNN(query, 3)
	fullResults, _ := fullIdx.SearchKNN(query, 3)

	if len(incResults) != len(fullResults) {
		t.Fatalf("result count mismatch: incremental=%d, full=%d", len(incResults), len(fullResults))
	}

	// The closest result should be the same for both
	if incResults[0].RowID != fullResults[0].RowID {
		t.Errorf("closest result mismatch: incremental rowID=%d, full rowID=%d",
			incResults[0].RowID, fullResults[0].RowID)
	}
}

func TestIncrementalIndex_CorrectnessAfterDeltaMerge(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	// Create base index with initial data
	baseIdx := NewIncrementalIndex(config)
	for i := 0; i < 5; i++ {
		vec := types.NewVector([]float32{float32(i), 0, 0})
		baseIdx.Insert(int64(i), vec)
	}

	// Get checkpoint after initial data
	checkpoint := baseIdx.CreateCheckpoint()

	// Add more data
	for i := 5; i < 10; i++ {
		vec := types.NewVector([]float32{float32(i), 0, 0})
		baseIdx.Insert(int64(i), vec)
	}

	// Get delta since checkpoint
	delta := baseIdx.OperationsSince(checkpoint.Version)

	// Create new index and apply delta
	newIdx := NewIncrementalIndex(config)
	// First add base data
	for i := 0; i < 5; i++ {
		vec := types.NewVector([]float32{float32(i), 0, 0})
		newIdx.Insert(int64(i), vec)
	}
	// Then apply delta
	newIdx.ApplyDelta(delta)

	// Both indexes should have same node count
	if baseIdx.Len() != newIdx.Len() {
		t.Errorf("length mismatch after delta: base=%d, new=%d", baseIdx.Len(), newIdx.Len())
	}

	// Search should return same results
	query := types.NewVector([]float32{7.5, 0, 0})

	baseResults, _ := baseIdx.SearchKNN(query, 3)
	newResults, _ := newIdx.SearchKNN(query, 3)

	if len(baseResults) != len(newResults) {
		t.Fatalf("result count mismatch: base=%d, new=%d", len(baseResults), len(newResults))
	}

	// Results should be similar (exact ordering may differ due to HNSW randomness)
	baseRowIDs := make(map[int64]bool)
	for _, r := range baseResults {
		baseRowIDs[r.RowID] = true
	}

	for _, r := range newResults {
		if !baseRowIDs[r.RowID] {
			t.Errorf("result rowID %d in new but not in base results", r.RowID)
		}
	}
}

func TestIncrementalIndex_CorrectnessWithDeletesAndUpdates(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      3,
		ML:             0.25,
	}

	// Create reference index built from final state
	refIdx := NewIndex(config)

	// Create incremental index with changes
	incIdx := NewIncrementalIndex(config)

	// Add some initial vectors
	for i := 0; i < 10; i++ {
		vec := types.NewVector([]float32{float32(i), float32(i), 0})
		incIdx.Insert(int64(i), vec)
	}

	// Delete some vectors
	incIdx.Delete(3)
	incIdx.Delete(7)

	// Update some vectors
	newVec := types.NewVector([]float32{100, 100, 0})
	incIdx.Update(5, newVec)

	// Build reference with final state
	for i := 0; i < 10; i++ {
		if i == 3 || i == 7 {
			continue // deleted
		}
		var vec *types.Vector
		if i == 5 {
			vec = types.NewVector([]float32{100, 100, 0}) // updated
		} else {
			vec = types.NewVector([]float32{float32(i), float32(i), 0})
		}
		refIdx.Insert(int64(i), vec)
	}

	// Both should have same count
	if incIdx.Len() != refIdx.Len() {
		t.Errorf("length mismatch: incremental=%d, reference=%d", incIdx.Len(), refIdx.Len())
	}

	// Verify all expected rows are present
	for i := 0; i < 10; i++ {
		if i == 3 || i == 7 {
			if incIdx.Contains(int64(i)) {
				t.Errorf("deleted row %d should not be in incremental index", i)
			}
		} else {
			if !incIdx.Contains(int64(i)) {
				t.Errorf("row %d should be in incremental index", i)
			}
		}
	}

	// Search for updated vector should find it
	query := types.NewVector([]float32{100, 100, 0})
	incResults, _ := incIdx.SearchKNN(query, 1)

	if len(incResults) != 1 {
		t.Fatalf("expected 1 result, got %d", len(incResults))
	}

	if incResults[0].RowID != 5 {
		t.Errorf("expected rowID 5 for updated vector, got %d", incResults[0].RowID)
	}
}

func TestIncrementalIndex_RecallAfterCompaction(t *testing.T) {
	config := Config{
		M:              16,
		MMax0:          32,
		EfConstruction: 100,
		EfSearch:       50,
		Dimension:      8,
		ML:             0.25,
	}

	// Create index with redundant operations
	idx := NewIncrementalIndex(config)

	// Insert 50 vectors
	vectors := make([]*types.Vector, 50)
	for i := 0; i < 50; i++ {
		vec := make([]float32, 8)
		for j := 0; j < 8; j++ {
			vec[j] = float32(i*10+j) / 100.0
		}
		vectors[i] = types.NewVector(vec)
		idx.Insert(int64(i), vectors[i])
	}

	// Delete half, then reinsert them (creates redundant operations)
	for i := 0; i < 25; i++ {
		idx.Delete(int64(i))
	}
	for i := 0; i < 25; i++ {
		idx.Insert(int64(i), vectors[i])
	}

	// Search before compaction
	query := vectors[10]
	beforeResults, _ := idx.SearchKNN(query, 5)

	// Compact the change log
	idx.CompactChangeLog()

	// Search after compaction (index state should be unchanged)
	afterResults, _ := idx.SearchKNN(query, 5)

	// Results should be identical (compaction doesn't change index, only log)
	if len(beforeResults) != len(afterResults) {
		t.Fatalf("result count changed after compaction: before=%d, after=%d",
			len(beforeResults), len(afterResults))
	}

	for i := range beforeResults {
		if beforeResults[i].RowID != afterResults[i].RowID {
			t.Errorf("result %d changed: before=%d, after=%d",
				i, beforeResults[i].RowID, afterResults[i].RowID)
		}
	}

	// Index should still have all 50 vectors
	if idx.Len() != 50 {
		t.Errorf("expected 50 nodes, got %d", idx.Len())
	}
}
