// pkg/turdb/stmt_test.go
package turdb

import (
	"path/filepath"
	"testing"

	"tur/pkg/types"
)

// TestStmt_HasRequiredFields verifies the Stmt struct has the required design fields
func TestStmt_HasRequiredFields(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a simple table for testing
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Prepare a statement - this tests the Stmt struct exists and can be created
	stmt, err := db.Prepare("SELECT id, name FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Verify the Stmt has SQL text accessible
	if stmt.SQL() != "SELECT id, name FROM users WHERE id = ?" {
		t.Errorf("expected SQL text to be preserved, got %q", stmt.SQL())
	}

	// Verify parameter count
	if stmt.NumParams() != 1 {
		t.Errorf("expected 1 parameter, got %d", stmt.NumParams())
	}
}

// TestStmt_Close verifies closing a prepared statement
func TestStmt_Close(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("SELECT * FROM test")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	// First close should succeed
	if err := stmt.Close(); err != nil {
		t.Errorf("first Close failed: %v", err)
	}

	// Second close should return error
	if err := stmt.Close(); err == nil {
		t.Error("expected error on second Close, got nil")
	}
}

// TestPrepare_InvalidSQL verifies that invalid SQL returns an error
func TestPrepare_InvalidSQL(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Invalid SQL should return an error
	_, err = db.Prepare("THIS IS NOT VALID SQL")
	if err == nil {
		t.Error("expected error for invalid SQL, got nil")
	}
}

// TestPrepare_MultipleParameters verifies parameter counting
func TestPrepare_MultipleParameters(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Test with multiple parameters
	stmt, err := db.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	if stmt.NumParams() != 3 {
		t.Errorf("expected 3 parameters, got %d", stmt.NumParams())
	}
}

// TestStmt_BindInt verifies binding integer parameters
func TestStmt_BindInt(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (id) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Bind an integer parameter (1-indexed like SQLite)
	if err := stmt.BindInt(1, 42); err != nil {
		t.Fatalf("BindInt failed: %v", err)
	}
}

// TestStmt_BindText verifies binding text parameters
func TestStmt_BindText(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (name) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Bind a text parameter
	if err := stmt.BindText(1, "hello"); err != nil {
		t.Fatalf("BindText failed: %v", err)
	}
}

// TestStmt_BindNull verifies binding NULL parameters
func TestStmt_BindNull(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (value) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Bind NULL
	if err := stmt.BindNull(1); err != nil {
		t.Fatalf("BindNull failed: %v", err)
	}
}

// TestStmt_BindFloat verifies binding float parameters
func TestStmt_BindFloat(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (value REAL)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (value) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Bind a float parameter
	if err := stmt.BindFloat(1, 3.14); err != nil {
		t.Fatalf("BindFloat failed: %v", err)
	}
}

// TestStmt_BindValue verifies binding generic Value parameters
func TestStmt_BindValue(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (value) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Bind a Value directly
	val := types.NewText("test value")
	if err := stmt.BindValue(1, val); err != nil {
		t.Fatalf("BindValue failed: %v", err)
	}
}

// TestStmt_Bind_OutOfRange verifies binding out of range index returns error
func TestStmt_Bind_OutOfRange(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (id) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Index 0 is out of range (1-indexed)
	if err := stmt.BindInt(0, 42); err == nil {
		t.Error("expected error for index 0, got nil")
	}

	// Index 2 is out of range (only 1 parameter)
	if err := stmt.BindInt(2, 42); err == nil {
		t.Error("expected error for index 2, got nil")
	}
}

// TestStmt_ClearBindings verifies clearing bound parameters
func TestStmt_ClearBindings(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (id) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Bind and then clear
	stmt.BindInt(1, 42)
	stmt.ClearBindings()

	// After clearing, all bindings should be NULL/unset
	// This is verified by the ability to rebind
	if err := stmt.BindInt(1, 100); err != nil {
		t.Fatalf("BindInt after ClearBindings failed: %v", err)
	}
}

// TestStmt_Reset verifies resetting a statement for reuse
func TestStmt_Reset(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (id) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Reset should not return an error
	if err := stmt.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}
}

// TestStmt_Exec verifies executing an INSERT statement
func TestStmt_Exec(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Prepare an INSERT statement
	stmt, err := db.Prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Bind parameters and execute
	stmt.BindInt(1, 1)
	stmt.BindText(2, "Alice")
	stmt.BindInt(3, 30)

	result, err := stmt.Exec()
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	// Should affect 1 row
	if result.RowsAffected() != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected())
	}
}

// TestStmt_Exec_MultipleExecutions verifies executing the same statement multiple times
func TestStmt_Exec_MultipleExecutions(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Prepare once
	stmt, err := db.Prepare("INSERT INTO items (id, value) VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Execute multiple times with different parameters
	for i := 1; i <= 5; i++ {
		stmt.ClearBindings()
		stmt.BindInt(1, int64(i))
		stmt.BindText(2, "value"+string(rune('0'+i)))

		result, err := stmt.Exec()
		if err != nil {
			t.Fatalf("Exec %d failed: %v", i, err)
		}

		if result.RowsAffected() != 1 {
			t.Errorf("execution %d: expected 1 row affected, got %d", i, result.RowsAffected())
		}

		stmt.Reset()
	}
}

// TestStmt_Exec_Update verifies executing an UPDATE statement
func TestStmt_Exec_Update(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Setup
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Prepare UPDATE statement
	stmt, err := db.Prepare("UPDATE users SET name = ? WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	stmt.BindText(1, "Bob")
	stmt.BindInt(2, 1)

	result, err := stmt.Exec()
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected())
	}
}

// TestStmt_Exec_Delete verifies executing a DELETE statement
func TestStmt_Exec_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Setup
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Prepare DELETE statement
	stmt, err := db.Prepare("DELETE FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	stmt.BindInt(1, 1)

	result, err := stmt.Exec()
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	if result.RowsAffected() != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected())
	}
}

// TestStmt_Exec_ClosedStmt verifies error when executing a closed statement
func TestStmt_Exec_ClosedStmt(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test (id) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	stmt.Close()

	// Exec on closed statement should fail
	_, err = stmt.Exec()
	if err == nil {
		t.Error("expected error when executing closed statement, got nil")
	}
}

// TestStmt_Query verifies executing a SELECT statement with parameters
func TestStmt_Query(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Setup
	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Prepare a SELECT statement
	stmt, err := db.Prepare("SELECT id, name, age FROM users WHERE age > ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Query with age > 20
	stmt.BindInt(1, 20)

	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Should return 2 rows
	count := 0
	for rows.Next() {
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// TestStmt_Query_NoResults verifies Query with no matching rows
func TestStmt_Query_NoResults(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	stmt, err := db.Prepare("SELECT * FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	// Query with non-existent ID
	stmt.BindInt(1, 999)

	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Should return 0 rows
	if rows.Next() {
		t.Error("expected no rows, but got at least one")
	}
}

// TestStmt_Query_Scan verifies scanning values from query results
func TestStmt_Query_Scan(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	stmt, err := db.Prepare("SELECT id, name, age FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	stmt.BindInt(1, 1)

	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected one row")
	}

	var id int64
	var name string
	var age int64

	err = rows.Scan(&id, &name, &age)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if id != 1 {
		t.Errorf("expected id 1, got %d", id)
	}
	if name != "Alice" {
		t.Errorf("expected name 'Alice', got '%s'", name)
	}
	if age != 30 {
		t.Errorf("expected age 30, got %d", age)
	}
}

// TestStmt_Query_Columns verifies getting column names from query results
func TestStmt_Query_Columns(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("SELECT id, name, email FROM users WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	stmt.BindInt(1, 1)

	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	columns := rows.Columns()
	if len(columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(columns))
	}

	expectedCols := []string{"id", "name", "email"}
	for i, col := range columns {
		if col != expectedCols[i] {
			t.Errorf("column %d: expected %q, got %q", i, expectedCols[i], col)
		}
	}
}

// TestStmt_Query_ClosedStmt verifies error when querying a closed statement
func TestStmt_Query_ClosedStmt(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	stmt.Close()

	// Query on closed statement should fail
	_, err = stmt.Query()
	if err == nil {
		t.Error("expected error when querying closed statement, got nil")
	}
}

// TestDB_PrepareCache verifies that statements are cached
func TestDB_PrepareCache(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	sql := "SELECT * FROM test WHERE id = ?"

	// First prepare - should create new statement
	stmt1, err := db.PrepareWithCache(sql)
	if err != nil {
		t.Fatalf("first Prepare failed: %v", err)
	}

	// Second prepare with same SQL - should return cached statement
	stmt2, err := db.PrepareWithCache(sql)
	if err != nil {
		t.Fatalf("second Prepare failed: %v", err)
	}

	// Both statements should be the same instance
	if stmt1 != stmt2 {
		t.Error("expected cached statement to be returned, got different instance")
	}

	// Clean up
	stmt1.Close()
}

// TestDB_PrepareCache_DifferentSQL verifies different SQL gets different statements
func TestDB_PrepareCache_DifferentSQL(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	stmt1, err := db.PrepareWithCache("SELECT id FROM test")
	if err != nil {
		t.Fatalf("Prepare 1 failed: %v", err)
	}
	defer stmt1.Close()

	stmt2, err := db.PrepareWithCache("SELECT name FROM test")
	if err != nil {
		t.Fatalf("Prepare 2 failed: %v", err)
	}
	defer stmt2.Close()

	// Different SQL should get different statements
	if stmt1 == stmt2 {
		t.Error("expected different statements for different SQL")
	}
}

// TestDB_ClearStmtCache verifies clearing the statement cache
func TestDB_ClearStmtCache(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	sql := "SELECT * FROM test"

	// Prepare a statement
	stmt1, err := db.PrepareWithCache(sql)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	// Clear the cache
	db.ClearStmtCache()

	// Prepare again - should get a new statement
	stmt2, err := db.PrepareWithCache(sql)
	if err != nil {
		t.Fatalf("Prepare after clear failed: %v", err)
	}
	defer stmt2.Close()

	// Should be different instances after cache clear
	if stmt1 == stmt2 {
		t.Error("expected new statement after cache clear")
	}
}
