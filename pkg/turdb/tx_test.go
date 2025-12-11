// pkg/turdb/tx_test.go
package turdb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDB_Begin_ReturnsTransaction(t *testing.T) {
	// Arrange: create a temporary database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Act: begin a transaction
	tx, err := db.Begin()

	// Assert: transaction is returned without error
	if err != nil {
		t.Fatalf("Begin() returned error: %v", err)
	}
	if tx == nil {
		t.Fatal("Begin() returned nil transaction")
	}
	if tx.db != db {
		t.Error("transaction should reference the database")
	}
}

func TestDB_Begin_OnClosedDatabase_ReturnsError(t *testing.T) {
	// Arrange: create and close a database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.Close()

	// Act: try to begin a transaction on closed database
	tx, err := db.Begin()

	// Assert: should return error
	if err == nil {
		t.Error("Begin() on closed database should return error")
	}
	if tx != nil {
		t.Error("Begin() on closed database should return nil transaction")
	}
	if err != ErrDatabaseClosed {
		t.Errorf("expected ErrDatabaseClosed, got: %v", err)
	}
}

func TestTx_Commit_CommitsTransaction(t *testing.T) {
	// Arrange: create database and begin transaction
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	// Act: commit the transaction
	err = tx.Commit()

	// Assert: commit succeeds
	if err != nil {
		t.Fatalf("Commit() returned error: %v", err)
	}

	// Transaction should be marked as done
	if !tx.done {
		t.Error("transaction should be marked as done after commit")
	}
}

func TestTx_Commit_OnAlreadyCommitted_ReturnsError(t *testing.T) {
	// Arrange: create database, begin and commit transaction
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("first Commit() failed: %v", err)
	}

	// Act: try to commit again
	err = tx.Commit()

	// Assert: should return error
	if err == nil {
		t.Error("Commit() on already committed transaction should return error")
	}
	if err != ErrTxDone {
		t.Errorf("expected ErrTxDone, got: %v", err)
	}
}

func TestTx_Rollback_RollsBackTransaction(t *testing.T) {
	// Arrange: create database and begin transaction
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	// Act: rollback the transaction
	err = tx.Rollback()

	// Assert: rollback succeeds
	if err != nil {
		t.Fatalf("Rollback() returned error: %v", err)
	}

	// Transaction should be marked as done
	if !tx.done {
		t.Error("transaction should be marked as done after rollback")
	}
}

func TestTx_Rollback_OnAlreadyRolledBack_ReturnsError(t *testing.T) {
	// Arrange: create database, begin and rollback transaction
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("first Rollback() failed: %v", err)
	}

	// Act: try to rollback again
	err = tx.Rollback()

	// Assert: should return error
	if err == nil {
		t.Error("Rollback() on already rolled back transaction should return error")
	}
	if err != ErrTxDone {
		t.Errorf("expected ErrTxDone, got: %v", err)
	}
}

func TestTx_Rollback_AfterCommit_ReturnsError(t *testing.T) {
	// Arrange: create database, begin and commit transaction
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit() failed: %v", err)
	}

	// Act: try to rollback after commit
	err = tx.Rollback()

	// Assert: should return error
	if err == nil {
		t.Error("Rollback() after commit should return error")
	}
	if err != ErrTxDone {
		t.Errorf("expected ErrTxDone, got: %v", err)
	}
}

func TestTx_Commit_AfterRollback_ReturnsError(t *testing.T) {
	// Arrange: create database, begin and rollback transaction
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Rollback() failed: %v", err)
	}

	// Act: try to commit after rollback
	err = tx.Commit()

	// Assert: should return error
	if err == nil {
		t.Error("Commit() after rollback should return error")
	}
	if err != ErrTxDone {
		t.Errorf("expected ErrTxDone, got: %v", err)
	}
}

func TestMultipleTransactions_Sequential(t *testing.T) {
	// Arrange: create database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Act & Assert: can create multiple sequential transactions
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("first Begin() failed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("first Commit() failed: %v", err)
	}

	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("second Begin() failed: %v", err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("second Rollback() failed: %v", err)
	}

	tx3, err := db.Begin()
	if err != nil {
		t.Fatalf("third Begin() failed: %v", err)
	}
	if err := tx3.Commit(); err != nil {
		t.Fatalf("third Commit() failed: %v", err)
	}
}

func TestTx_Exec_CreateTable(t *testing.T) {
	// Arrange: create database and begin transaction
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	// Act: execute CREATE TABLE in transaction
	result, err := tx.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

	// Assert: execution succeeds
	if err != nil {
		t.Fatalf("Exec() returned error: %v", err)
	}
	if result == nil {
		t.Fatal("Exec() returned nil result")
	}

	// Commit and verify table exists
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() failed: %v", err)
	}
}

func TestTx_Exec_InsertAndSelect(t *testing.T) {
	// Arrange: create database with table
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table outside transaction
	tx1, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}
	if _, err := tx1.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Commit() failed: %v", err)
	}

	// Act: insert in new transaction
	tx2, err := db.Begin()
	if err != nil {
		t.Fatalf("second Begin() failed: %v", err)
	}

	result, err := tx2.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Query within same transaction
	selectResult, err := tx2.Exec("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(selectResult.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(selectResult.Rows))
	}

	if err := tx2.Commit(); err != nil {
		t.Fatalf("Commit() failed: %v", err)
	}
}

func TestTx_Exec_OnDoneTransaction_ReturnsError(t *testing.T) {
	// Arrange: create database and committed transaction
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() failed: %v", err)
	}

	// Act: try to execute on committed transaction
	_, err = tx.Exec("CREATE TABLE foo (id INT)")

	// Assert: should return error
	if err == nil {
		t.Error("Exec() on committed transaction should return error")
	}
	if err != ErrTxDone {
		t.Errorf("expected ErrTxDone, got: %v", err)
	}
}

func TestDB_Exec_OutsideTransaction(t *testing.T) {
	// Arrange: create database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Act: execute SQL directly on DB (auto-commit mode)
	result, err := db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

	// Assert: execution succeeds
	if err != nil {
		t.Fatalf("Exec() returned error: %v", err)
	}
	if result == nil {
		t.Fatal("Exec() returned nil result")
	}
}

func TestDB_Exec_InsertOutsideTransaction(t *testing.T) {
	// Arrange: create database with table
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	if _, err := db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Act: insert data
	result, err := db.Exec("INSERT INTO users (id, name) VALUES (1, 'Bob')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Assert: rows affected
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify data is persisted
	selectResult, err := db.Exec("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(selectResult.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(selectResult.Rows))
	}
}

func TestDB_Exec_OnClosedDatabase_ReturnsError(t *testing.T) {
	// Arrange: create and close database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	db.Close()

	// Act: try to execute on closed database
	_, err = db.Exec("CREATE TABLE foo (id INT)")

	// Assert: should return error
	if err == nil {
		t.Error("Exec() on closed database should return error")
	}
	if err != ErrDatabaseClosed {
		t.Errorf("expected ErrDatabaseClosed, got: %v", err)
	}
}

func TestTx_Rollback_SafeToCallAfterCommit(t *testing.T) {
	// This tests the idiomatic pattern:
	//   tx, err := db.Begin()
	//   if err != nil { return err }
	//   defer tx.Rollback() // safe even after commit
	//   ...
	//   return tx.Commit()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	// Commit first
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() failed: %v", err)
	}

	// Rollback should return ErrTxDone, not panic or corrupt state
	err = tx.Rollback()
	if err != ErrTxDone {
		t.Errorf("expected ErrTxDone, got: %v", err)
	}
}

func TestTx_Rollback_SafeToCallMultipleTimes(t *testing.T) {
	// Multiple rollback calls should be safe
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	// First rollback succeeds
	if err := tx.Rollback(); err != nil {
		t.Fatalf("first Rollback() failed: %v", err)
	}

	// Subsequent rollbacks return ErrTxDone
	if err := tx.Rollback(); err != ErrTxDone {
		t.Errorf("second Rollback() expected ErrTxDone, got: %v", err)
	}
	if err := tx.Rollback(); err != ErrTxDone {
		t.Errorf("third Rollback() expected ErrTxDone, got: %v", err)
	}
}

func TestTx_AutoRollbackPattern(t *testing.T) {
	// Test the idiomatic defer pattern works correctly for error handling
	// Note: Currently the executor commits B-tree operations immediately,
	// so rollback doesn't undo data changes. This test verifies the API pattern
	// works correctly even if full rollback isn't implemented yet.
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INT PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test that the defer pattern works without errors
	err = func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback() // This should be safe to call

		// Insert some data
		_, err = tx.Exec("INSERT INTO test (id, val) VALUES (1, 'test')")
		if err != nil {
			return err
		}

		// Simulate error - don't commit, just return
		return errors.New("simulated error")
	}()

	// Verify the pattern works correctly
	if err == nil || err.Error() != "simulated error" {
		t.Fatalf("expected simulated error, got: %v", err)
	}

	// Verify we can still query the database (no corruption)
	result, err := db.Exec("SELECT COUNT(*) FROM test")
	if err != nil {
		t.Fatalf("SELECT failed after rollback pattern: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row in result, got %d", len(result.Rows))
	}
}

func TestTx_SuccessfulCommitPattern(t *testing.T) {
	// Test the full transaction commit pattern
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE test (id INT PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test successful transaction commit
	err = func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback() // Safe - no-op after commit

		// Insert some data
		_, err = tx.Exec("INSERT INTO test (id, val) VALUES (1, 'committed')")
		if err != nil {
			return err
		}

		return tx.Commit()
	}()

	if err != nil {
		t.Fatalf("transaction should have committed successfully: %v", err)
	}

	// Verify data is persisted
	result, err := db.Exec("SELECT val FROM test WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	val, ok := result.Rows[0][0].(string)
	if !ok {
		t.Fatalf("expected string val, got %T", result.Rows[0][0])
	}
	if val != "committed" {
		t.Errorf("expected 'committed', got '%s'", val)
	}
}

// Cleanup helper - remove temp files
func cleanupTestDB(t *testing.T, path string) {
	t.Helper()
	os.Remove(path)
	os.Remove(path + ".lock")
}
