// pkg/turdb/context_test.go
package turdb

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestDB_ExecContext_BasicExecution(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "context.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Execute with a background context (should succeed)
	ctx := context.Background()
	result, err := db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestDB_ExecContext_CanceledContext(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "context_cancel.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create an already-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Execute with canceled context should fail
	_, err = db.ExecContext(ctx, "CREATE TABLE test (id INTEGER)")
	if err == nil {
		t.Error("expected error with canceled context, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

func TestDB_ExecContext_TimeoutContext(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "context_timeout.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a context that times out immediately
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait for timeout to occur
	time.Sleep(1 * time.Millisecond)

	// Execute with timed-out context should fail
	_, err = db.ExecContext(ctx, "CREATE TABLE test (id INTEGER)")
	if err == nil {
		t.Error("expected error with timed-out context, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded error, got: %v", err)
	}
}

func TestStmt_ExecContext_BasicExecution(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "stmt_context.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table first
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Prepare a statement
	stmt, err := db.Prepare("INSERT INTO test (id, name) VALUES (?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Bind parameters and execute with context
	stmt.BindInt(1, 1)
	stmt.BindText(2, "Alice")

	ctx := context.Background()
	result, err := stmt.ExecContext(ctx)
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected())
	}
}

func TestStmt_ExecContext_CanceledContext(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "stmt_context_cancel.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table first
	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Prepare a statement
	stmt, err := db.Prepare("INSERT INTO test (id) VALUES (?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	stmt.BindInt(1, 1)

	// Create an already-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Execute with canceled context should fail
	_, err = stmt.ExecContext(ctx)
	if err == nil {
		t.Error("expected error with canceled context, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

func TestStmt_QueryContext_BasicExecution(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "query_context.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Prepare a SELECT statement
	stmt, err := db.Prepare("SELECT id, name FROM test WHERE id = ?")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	stmt.BindInt(1, 1)

	ctx := context.Background()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("QueryContext failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Error("expected at least one row")
	}
}

func TestStmt_QueryContext_CanceledContext(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "query_context_cancel.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table first
	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Prepare a statement
	stmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	stmt.BindInt(1, 1)

	// Create an already-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Query with canceled context should fail
	_, err = stmt.QueryContext(ctx)
	if err == nil {
		t.Error("expected error with canceled context, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

func TestTx_ExecContext_BasicExecution(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tx_context.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table first
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Execute with context within transaction
	ctx := context.Background()
	result, err := tx.ExecContext(ctx, "INSERT INTO test (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	if result == nil {
		t.Error("expected non-nil result")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}

func TestTx_ExecContext_CanceledContext(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tx_context_cancel.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table first
	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Create an already-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Execute with canceled context should fail
	_, err = tx.ExecContext(ctx, "INSERT INTO test (id) VALUES (1)")
	if err == nil {
		t.Error("expected error with canceled context, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

func TestDB_BeginContext_BasicExecution(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "begin_context.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginContext(ctx)
	if err != nil {
		t.Fatalf("BeginContext failed: %v", err)
	}
	defer tx.Rollback()

	// Transaction should be usable
	if tx.done {
		t.Error("transaction should not be done")
	}
}

func TestDB_BeginContext_CanceledContext(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "begin_context_cancel.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create an already-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Begin with canceled context should fail
	_, err = db.BeginContext(ctx)
	if err == nil {
		t.Error("expected error with canceled context, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}
