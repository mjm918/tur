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
