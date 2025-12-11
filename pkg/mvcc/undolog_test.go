package mvcc

import (
	"sync"
	"testing"
)

func TestUndoLog_Add(t *testing.T) {
	log := NewUndoLog()

	op := UndoOperation{
		Type:      UndoInsert,
		TableName: "users",
		Key:       []byte{0, 0, 0, 0, 0, 0, 0, 1},
		OldData:   nil,
	}
	log.Add(op)

	ops := log.GetAllOperations()
	if len(ops) != 1 {
		t.Fatalf("expected 1 operation, got %d", len(ops))
	}
	if ops[0].Type != UndoInsert {
		t.Errorf("expected UndoInsert, got %v", ops[0].Type)
	}
	if ops[0].TableName != "users" {
		t.Errorf("expected table 'users', got %s", ops[0].TableName)
	}
}

func TestUndoLog_MultipleOperations(t *testing.T) {
	log := NewUndoLog()

	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{1}})
	log.Add(UndoOperation{Type: UndoUpdate, TableName: "t1", Key: []byte{2}, OldData: []byte("old")})
	log.Add(UndoOperation{Type: UndoDelete, TableName: "t1", Key: []byte{3}, OldData: []byte("deleted")})

	ops := log.GetAllOperations()
	if len(ops) != 3 {
		t.Fatalf("expected 3 operations, got %d", len(ops))
	}

	// Verify order is preserved
	if ops[0].Type != UndoInsert {
		t.Error("first op should be UndoInsert")
	}
	if ops[1].Type != UndoUpdate {
		t.Error("second op should be UndoUpdate")
	}
	if ops[2].Type != UndoDelete {
		t.Error("third op should be UndoDelete")
	}
}

func TestUndoLog_Clear(t *testing.T) {
	log := NewUndoLog()

	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{1}})
	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{2}})

	if len(log.GetAllOperations()) != 2 {
		t.Fatal("expected 2 operations before clear")
	}

	log.Clear()

	if len(log.GetAllOperations()) != 0 {
		t.Error("expected 0 operations after clear")
	}
}

func TestUndoLog_CreateSavepoint(t *testing.T) {
	log := NewUndoLog()

	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{1}})
	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{2}})

	err := log.CreateSavepoint("sp1")
	if err != nil {
		t.Fatalf("CreateSavepoint failed: %v", err)
	}

	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{3}})

	// Should have 3 total operations
	if len(log.GetAllOperations()) != 3 {
		t.Fatalf("expected 3 operations, got %d", len(log.GetAllOperations()))
	}
}

func TestUndoLog_RollbackToSavepoint(t *testing.T) {
	log := NewUndoLog()

	// Add ops before savepoint
	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{1}})
	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{2}})

	log.CreateSavepoint("sp1")

	// Add ops after savepoint
	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{3}})
	log.Add(UndoOperation{Type: UndoUpdate, TableName: "t1", Key: []byte{4}, OldData: []byte("old")})

	// Rollback to savepoint - should return ops to undo
	opsToUndo, err := log.RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	// Should return 2 ops (the ones after savepoint)
	if len(opsToUndo) != 2 {
		t.Fatalf("expected 2 ops to undo, got %d", len(opsToUndo))
	}

	// Ops should be in reverse order for LIFO undo
	if opsToUndo[0].Type != UndoUpdate {
		t.Error("first undo op should be UndoUpdate (last added)")
	}
	if opsToUndo[1].Type != UndoInsert {
		t.Error("second undo op should be UndoInsert")
	}

	// Log should now only have ops before savepoint
	remaining := log.GetAllOperations()
	if len(remaining) != 2 {
		t.Fatalf("expected 2 remaining ops, got %d", len(remaining))
	}
}

func TestUndoLog_RollbackToSavepoint_NonExistent(t *testing.T) {
	log := NewUndoLog()
	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{1}})

	_, err := log.RollbackToSavepoint("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent savepoint")
	}
}

func TestUndoLog_NestedSavepoints(t *testing.T) {
	log := NewUndoLog()

	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{1}})
	log.CreateSavepoint("sp1")

	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{2}})
	log.CreateSavepoint("sp2")

	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{3}})

	// Rollback to sp2 - should only undo 1 op
	opsToUndo, err := log.RollbackToSavepoint("sp2")
	if err != nil {
		t.Fatalf("RollbackToSavepoint sp2 failed: %v", err)
	}
	if len(opsToUndo) != 1 {
		t.Fatalf("expected 1 op to undo, got %d", len(opsToUndo))
	}

	// Now rollback to sp1 - should undo 1 more op
	opsToUndo, err = log.RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("RollbackToSavepoint sp1 failed: %v", err)
	}
	if len(opsToUndo) != 1 {
		t.Fatalf("expected 1 op to undo, got %d", len(opsToUndo))
	}

	// Should have 1 op remaining (before sp1)
	if len(log.GetAllOperations()) != 1 {
		t.Errorf("expected 1 remaining op, got %d", len(log.GetAllOperations()))
	}
}

func TestUndoLog_ReleaseSavepoint(t *testing.T) {
	log := NewUndoLog()

	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{1}})
	log.CreateSavepoint("sp1")
	log.Add(UndoOperation{Type: UndoInsert, TableName: "t1", Key: []byte{2}})

	// Release savepoint - should remove it but keep operations
	err := log.ReleaseSavepoint("sp1")
	if err != nil {
		t.Fatalf("ReleaseSavepoint failed: %v", err)
	}

	// Operations should still be there
	if len(log.GetAllOperations()) != 2 {
		t.Errorf("expected 2 ops after release, got %d", len(log.GetAllOperations()))
	}

	// But savepoint should be gone
	_, err = log.RollbackToSavepoint("sp1")
	if err == nil {
		t.Error("expected error - savepoint should be released")
	}
}

func TestUndoLog_ThreadSafety(t *testing.T) {
	log := NewUndoLog()
	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			log.Add(UndoOperation{
				Type:      UndoInsert,
				TableName: "t1",
				Key:       []byte{byte(id)},
			})
		}(i)
	}

	wg.Wait()

	ops := log.GetAllOperations()
	if len(ops) != 100 {
		t.Errorf("expected 100 operations, got %d", len(ops))
	}
}

func TestUndoLog_IndexOperations(t *testing.T) {
	log := NewUndoLog()

	// Add index insert operation
	log.Add(UndoOperation{
		Type:      UndoIndexInsert,
		TableName: "users",
		IndexName: "idx_email",
		IndexKey:  []byte("alice@example.com"),
		IndexVal:  []byte{0, 0, 0, 0, 0, 0, 0, 1}, // rowid
	})

	// Add index delete operation
	log.Add(UndoOperation{
		Type:      UndoIndexDelete,
		TableName: "users",
		IndexName: "idx_email",
		IndexKey:  []byte("bob@example.com"),
		IndexVal:  []byte{0, 0, 0, 0, 0, 0, 0, 2},
	})

	ops := log.GetAllOperations()
	if len(ops) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(ops))
	}

	if ops[0].Type != UndoIndexInsert {
		t.Error("first op should be UndoIndexInsert")
	}
	if ops[0].IndexName != "idx_email" {
		t.Errorf("expected index name 'idx_email', got %s", ops[0].IndexName)
	}
}
