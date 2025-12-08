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
