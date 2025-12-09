package executor

import (
	"testing"
)

func TestExecutor_Begin(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	result, err := exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Execute BEGIN: %v", err)
	}

	// BEGIN should return success with 0 rows affected
	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}

	// After BEGIN, executor should have an active transaction
	if !exec.HasActiveTransaction() {
		t.Error("Expected active transaction after BEGIN")
	}
}

func TestExecutor_BeginTransaction(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	result, err := exec.Execute("BEGIN TRANSACTION")
	if err != nil {
		t.Fatalf("Execute BEGIN TRANSACTION: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}

	if !exec.HasActiveTransaction() {
		t.Error("Expected active transaction after BEGIN TRANSACTION")
	}
}

func TestExecutor_Commit(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Start a transaction first
	_, err := exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Execute BEGIN: %v", err)
	}

	// Now commit
	result, err := exec.Execute("COMMIT")
	if err != nil {
		t.Fatalf("Execute COMMIT: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}

	// After COMMIT, no active transaction
	if exec.HasActiveTransaction() {
		t.Error("Expected no active transaction after COMMIT")
	}
}

func TestExecutor_Rollback(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Start a transaction first
	_, err := exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Execute BEGIN: %v", err)
	}

	// Now rollback
	result, err := exec.Execute("ROLLBACK")
	if err != nil {
		t.Fatalf("Execute ROLLBACK: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}

	// After ROLLBACK, no active transaction
	if exec.HasActiveTransaction() {
		t.Error("Expected no active transaction after ROLLBACK")
	}
}

func TestExecutor_CommitWithoutTransaction(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// COMMIT without BEGIN should error
	_, err := exec.Execute("COMMIT")
	if err == nil {
		t.Fatal("Expected error when COMMIT without active transaction")
	}
}

func TestExecutor_RollbackWithoutTransaction(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// ROLLBACK without BEGIN should error
	_, err := exec.Execute("ROLLBACK")
	if err == nil {
		t.Fatal("Expected error when ROLLBACK without active transaction")
	}
}

func TestExecutor_NestedBeginError(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// First BEGIN
	_, err := exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Execute BEGIN: %v", err)
	}

	// Second BEGIN should error (nested transactions not supported)
	_, err = exec.Execute("BEGIN")
	if err == nil {
		t.Fatal("Expected error for nested BEGIN (transaction already active)")
	}
}
