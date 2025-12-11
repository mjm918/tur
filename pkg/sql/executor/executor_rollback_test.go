package executor

import (
	"os"
	"path/filepath"
	"testing"

	"tur/pkg/pager"
)

// setupRollbackTestExecutor creates an executor for rollback testing
func setupRollbackTestExecutor(t *testing.T) (*Executor, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "rollback_test")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}

	dbPath := filepath.Join(dir, "test.db")
	p, err := pager.Open(dbPath, pager.Options{})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("pager.Open: %v", err)
	}

	exec := New(p)
	cleanup := func() {
		exec.Close()
		os.RemoveAll(dir)
	}

	return exec, cleanup
}

// Test: ROLLBACK undoes INSERT
func TestExecutor_Rollback_UndoesInsert(t *testing.T) {
	exec, cleanup := setupRollbackTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Begin transaction
	_, err = exec.Execute("BEGIN")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Insert row
	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify row exists before rollback
	result, err := exec.Execute("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if result.Rows[0][0].Int() != 1 {
		t.Fatalf("expected 1 row before rollback, got %v", result.Rows[0][0])
	}

	// Rollback
	_, err = exec.Execute("ROLLBACK")
	if err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	// Verify row is gone
	result, err = exec.Execute("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("SELECT after ROLLBACK failed: %v", err)
	}
	count := result.Rows[0][0].Int()
	if count != 0 {
		t.Errorf("expected 0 rows after ROLLBACK, got %d", count)
	}
}

// Test: ROLLBACK undoes UPDATE
func TestExecutor_Rollback_UndoesUpdate(t *testing.T) {
	exec, cleanup := setupRollbackTestExecutor(t)
	defer cleanup()

	// Setup: Create table and insert data outside transaction
	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")

	// Verify initial state
	result, _ := exec.Execute("SELECT name FROM users WHERE id = 1")
	if result.Rows[0][0].Text() != "Alice" {
		t.Fatalf("expected name 'Alice', got %v", result.Rows[0][0])
	}

	// Begin transaction
	exec.Execute("BEGIN")

	// Update row
	_, err := exec.Execute("UPDATE users SET name = 'Bob' WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Verify update happened
	result, _ = exec.Execute("SELECT name FROM users WHERE id = 1")
	if result.Rows[0][0].Text() != "Bob" {
		t.Fatalf("expected name 'Bob' after update, got %v", result.Rows[0][0])
	}

	// Rollback
	exec.Execute("ROLLBACK")

	// Verify update was undone
	result, _ = exec.Execute("SELECT name FROM users WHERE id = 1")
	name := result.Rows[0][0].Text()
	if name != "Alice" {
		t.Errorf("expected name 'Alice' after ROLLBACK, got '%s'", name)
	}
}

// Test: ROLLBACK undoes DELETE
func TestExecutor_Rollback_UndoesDelete(t *testing.T) {
	exec, cleanup := setupRollbackTestExecutor(t)
	defer cleanup()

	// Setup: Create table and insert data outside transaction
	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")

	// Verify initial state
	result, _ := exec.Execute("SELECT COUNT(*) FROM users")
	if result.Rows[0][0].Int() != 2 {
		t.Fatalf("expected 2 rows initially, got %v", result.Rows[0][0])
	}

	// Begin transaction
	exec.Execute("BEGIN")

	// Delete a row
	_, err := exec.Execute("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Verify delete happened
	result, _ = exec.Execute("SELECT COUNT(*) FROM users")
	if result.Rows[0][0].Int() != 1 {
		t.Fatalf("expected 1 row after delete, got %v", result.Rows[0][0])
	}

	// Rollback
	exec.Execute("ROLLBACK")

	// Verify delete was undone
	result, _ = exec.Execute("SELECT COUNT(*) FROM users")
	count := result.Rows[0][0].Int()
	if count != 2 {
		t.Errorf("expected 2 rows after ROLLBACK, got %d", count)
	}

	// Verify Alice is back
	result, _ = exec.Execute("SELECT name FROM users WHERE id = 1")
	if len(result.Rows) == 0 {
		t.Error("expected Alice to be restored after ROLLBACK")
	} else if result.Rows[0][0].Text() != "Alice" {
		t.Errorf("expected name 'Alice', got %v", result.Rows[0][0])
	}
}

// Test: COMMIT makes changes permanent (no rollback after commit)
func TestExecutor_Commit_MakesPermanent(t *testing.T) {
	exec, cleanup := setupRollbackTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")

	// Begin, insert, commit
	exec.Execute("BEGIN")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("COMMIT")

	// Verify row exists after commit
	result, _ := exec.Execute("SELECT COUNT(*) FROM users")
	count := result.Rows[0][0].Int()
	if count != 1 {
		t.Errorf("expected 1 row after COMMIT, got %d", count)
	}
}

// Test: Multiple operations in single transaction
func TestExecutor_Rollback_MultipleOperations(t *testing.T) {
	exec, cleanup := setupRollbackTestExecutor(t)
	defer cleanup()

	// Setup
	exec.Execute("CREATE TABLE users (id INT, name TEXT, age INT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice', 30)")

	// Begin transaction
	exec.Execute("BEGIN")

	// Multiple operations
	exec.Execute("INSERT INTO users VALUES (2, 'Bob', 25)")
	exec.Execute("UPDATE users SET age = 31 WHERE id = 1")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
	exec.Execute("DELETE FROM users WHERE id = 1")

	// Rollback all
	exec.Execute("ROLLBACK")

	// Verify: should be back to original state (only Alice, age 30)
	result, _ := exec.Execute("SELECT COUNT(*) FROM users")
	if result.Rows[0][0].Int() != 1 {
		t.Errorf("expected 1 row after ROLLBACK, got %v", result.Rows[0][0])
	}

	result, _ = exec.Execute("SELECT name, age FROM users WHERE id = 1")
	if len(result.Rows) != 1 {
		t.Fatal("expected Alice to exist after ROLLBACK")
	}
	if result.Rows[0][0].Text() != "Alice" {
		t.Errorf("expected name 'Alice', got %v", result.Rows[0][0])
	}
	if result.Rows[0][1].Int() != 30 {
		t.Errorf("expected age 30, got %v", result.Rows[0][1])
	}
}

// Test: ROLLBACK TO SAVEPOINT
func TestExecutor_RollbackToSavepoint(t *testing.T) {
	exec, cleanup := setupRollbackTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")

	exec.Execute("BEGIN")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")

	// Create savepoint
	exec.Execute("SAVEPOINT sp1")

	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")

	// Verify 3 rows
	result, _ := exec.Execute("SELECT COUNT(*) FROM users")
	if result.Rows[0][0].Int() != 3 {
		t.Fatalf("expected 3 rows before rollback to savepoint, got %v", result.Rows[0][0])
	}

	// Rollback to savepoint
	exec.Execute("ROLLBACK TO sp1")

	// Should have only Alice
	result, _ = exec.Execute("SELECT COUNT(*) FROM users")
	count := result.Rows[0][0].Int()
	if count != 1 {
		t.Errorf("expected 1 row after ROLLBACK TO savepoint, got %d", count)
	}

	// Can still commit
	exec.Execute("COMMIT")

	// Alice should persist
	result, _ = exec.Execute("SELECT name FROM users")
	if len(result.Rows) != 1 || result.Rows[0][0].Text() != "Alice" {
		t.Error("expected only Alice after commit")
	}
}

// Test: Auto-commit mode (no explicit BEGIN) should not use undo logging
func TestExecutor_AutoCommit_NoUndoLogging(t *testing.T) {
	exec, cleanup := setupRollbackTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")

	// Insert without BEGIN - auto-commit
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")

	// Verify row exists
	result, _ := exec.Execute("SELECT COUNT(*) FROM users")
	if result.Rows[0][0].Int() != 1 {
		t.Error("expected 1 row in auto-commit mode")
	}

	// ROLLBACK without BEGIN should fail
	_, err := exec.Execute("ROLLBACK")
	if err == nil {
		t.Error("expected error for ROLLBACK without active transaction")
	}
}
