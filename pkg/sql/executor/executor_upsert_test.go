package executor

import (
	"os"
	"testing"

	"tur/pkg/pager"
	"tur/pkg/sql/lexer"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

func TestFindConflictingRowByPrimaryKey(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_conflict_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	p, err := pager.Open(tmpFile.Name(), pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	_, err = exec.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	table := exec.catalog.GetTable("users")
	if table == nil {
		t.Fatal("table not found")
	}

	// Test: Find conflict with existing key
	values := []types.Value{types.NewInt(1), types.NewText("Bob")}
	rowID, err := exec.findConflictingRow(table, values)
	if err != nil {
		t.Fatalf("findConflictingRow error: %v", err)
	}
	if rowID == -1 {
		t.Error("expected to find conflicting row, got -1")
	}

	// Test: No conflict with new key
	values = []types.Value{types.NewInt(2), types.NewText("Charlie")}
	rowID, err = exec.findConflictingRow(table, values)
	if err != nil {
		t.Fatalf("findConflictingRow error: %v", err)
	}
	if rowID != -1 {
		t.Errorf("expected no conflict (rowID=-1), got %d", rowID)
	}
}

func TestFindConflictingRowByUniqueIndex(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_conflict_unique_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	p, err := pager.Open(tmpFile.Name(), pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	_, err = exec.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users (id, email, name) VALUES (1, 'alice@example.com', 'Alice')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	table := exec.catalog.GetTable("users")

	// Test: Find conflict by unique email (different id)
	values := []types.Value{types.NewInt(2), types.NewText("alice@example.com"), types.NewText("Alice2")}
	rowID, err := exec.findConflictingRow(table, values)
	if err != nil {
		t.Fatalf("findConflictingRow error: %v", err)
	}
	if rowID == -1 {
		t.Error("expected to find conflicting row by unique index, got -1")
	}

	// Test: No conflict with new email
	values = []types.Value{types.NewInt(2), types.NewText("bob@example.com"), types.NewText("Bob")}
	rowID, err = exec.findConflictingRow(table, values)
	if err != nil {
		t.Fatalf("findConflictingRow error: %v", err)
	}
	if rowID != -1 {
		t.Errorf("expected no conflict, got rowID=%d", rowID)
	}
}

func TestFindConflictingRowNullAllowed(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_conflict_null_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	p, err := pager.Open(tmpFile.Name(), pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	_, err = exec.Execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items (id, code) VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	table := exec.catalog.GetTable("items")

	// Test: NULL doesn't conflict with NULL (SQL standard)
	values := []types.Value{types.NewInt(2), types.NewNull()}
	rowID, err := exec.findConflictingRow(table, values)
	if err != nil {
		t.Fatalf("findConflictingRow error: %v", err)
	}
	if rowID != -1 {
		t.Errorf("expected no conflict for NULL values, got rowID=%d", rowID)
	}
}

func TestGetRowByID(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_getrow_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	p, err := pager.Open(tmpFile.Name(), pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	_, err = exec.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	table := exec.catalog.GetTable("users")

	// The first inserted row should have rowID 1 (CREATE TABLE initializes rowid to 1)
	values, err := exec.getRowByID(table, 1)
	if err != nil {
		t.Fatalf("getRowByID error: %v", err)
	}

	if len(values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(values))
	}

	if values[0].Int() != 1 {
		t.Errorf("expected id=1, got %v", values[0])
	}
	if values[1].Text() != "Alice" {
		t.Errorf("expected name='Alice', got %v", values[1])
	}
	if values[2].Int() != 30 {
		t.Errorf("expected age=30, got %v", values[2])
	}
}

func TestEvaluateValuesFunc(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_valuesfunc_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	p, err := pager.Open(tmpFile.Name(), pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create a ValuesFunc AST node
	valuesFunc := &parser.ValuesFunc{ColumnName: "name"}

	// Set up the values context with would-be-inserted values
	exec.valuesContext = map[string]types.Value{
		"id":   types.NewInt(1),
		"name": types.NewText("Alice"),
	}

	// Evaluate - pass nil for row and colMap since we're using valuesContext
	result, err := exec.evaluateExpr(valuesFunc, nil, nil)
	if err != nil {
		t.Fatalf("evaluateExpr error: %v", err)
	}

	if result.Type() != types.TypeText {
		t.Errorf("expected TypeText, got %v", result.Type())
	}
	if result.Text() != "Alice" {
		t.Errorf("expected 'Alice', got %q", result.Text())
	}

	// Clean up
	exec.valuesContext = nil
}

func TestExecuteOnDuplicateUpdate(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_ondupupdate_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	p, err := pager.Open(tmpFile.Name(), pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	_, err = exec.Execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, count INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert initial row
	_, err = exec.Execute("INSERT INTO counter (id, count, name) VALUES (1, 10, 'test')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	table := exec.catalog.GetTable("counter")

	// Prepare update assignments: count = count + VALUES(count), name = VALUES(name)
	assignments := []parser.Assignment{
		{Column: "count", Value: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "count"},
			Op:    lexer.PLUS,
			Right: &parser.ValuesFunc{ColumnName: "count"},
		}},
		{Column: "name", Value: &parser.ValuesFunc{ColumnName: "name"}},
	}

	// New values that would be inserted
	newValues := []types.Value{types.NewInt(1), types.NewInt(5), types.NewText("updated")}

	// Execute update on existing row (rowID 1 - first insert gets rowID 1)
	changed, err := exec.executeOnDuplicateUpdate(table, 1, newValues, assignments)
	if err != nil {
		t.Fatalf("executeOnDuplicateUpdate error: %v", err)
	}

	if !changed {
		t.Error("expected changed=true")
	}

	// Verify the update
	result, err := exec.Execute("SELECT id, count, name FROM counter WHERE id = 1")
	if err != nil {
		t.Fatalf("select error: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if row[0].Int() != 1 {
		t.Errorf("expected id=1, got %v", row[0])
	}
	if row[1].Int() != 15 { // 10 + 5
		t.Errorf("expected count=15, got %v", row[1])
	}
	if row[2].Text() != "updated" {
		t.Errorf("expected name='updated', got %v", row[2])
	}
}

func TestInsertOnDuplicateKeyUpdateBasic(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_insert_ondup_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	p, err := pager.Open(tmpFile.Name(), pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create table
	_, err = exec.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, visits INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Initial insert
	result, err := exec.Execute("INSERT INTO users (id, name, visits) VALUES (1, 'Alice', 1)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Insert with ON DUPLICATE KEY UPDATE - should update
	result, err = exec.Execute("INSERT INTO users (id, name, visits) VALUES (1, 'Alice Updated', 1) ON DUPLICATE KEY UPDATE name = VALUES(name), visits = visits + VALUES(visits)")
	if err != nil {
		t.Fatalf("failed to execute ON DUPLICATE KEY UPDATE: %v", err)
	}
	// MySQL returns 2 for update with changes
	if result.RowsAffected != 2 {
		t.Errorf("expected 2 rows affected (update), got %d", result.RowsAffected)
	}

	// Verify
	selectResult, err := exec.Execute("SELECT id, name, visits FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("select error: %v", err)
	}
	if len(selectResult.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(selectResult.Rows))
	}
	row := selectResult.Rows[0]
	if row[1].Text() != "Alice Updated" {
		t.Errorf("expected name='Alice Updated', got %q", row[1].Text())
	}
	if row[2].Int() != 2 {
		t.Errorf("expected visits=2, got %d", row[2].Int())
	}

	// Insert new row - should insert
	result, err = exec.Execute("INSERT INTO users (id, name, visits) VALUES (2, 'Bob', 1) ON DUPLICATE KEY UPDATE visits = visits + 1")
	if err != nil {
		t.Fatalf("failed to insert new row: %v", err)
	}
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected (insert), got %d", result.RowsAffected)
	}

	// Verify total rows
	selectAll, err := exec.Execute("SELECT id, name, visits FROM users")
	if err != nil {
		t.Fatalf("select all error: %v", err)
	}
	if len(selectAll.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(selectAll.Rows))
	}
}

func TestInsertOnDuplicateKeyUpdateNoChange(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_insert_nochange_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	p, err := pager.Open(tmpFile.Name(), pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	_, err = exec.Execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items (id, value) VALUES (1, 100)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Update with same value - no actual change
	result, err := exec.Execute("INSERT INTO items (id, value) VALUES (1, 100) ON DUPLICATE KEY UPDATE value = VALUES(value)")
	if err != nil {
		t.Fatalf("failed to execute: %v", err)
	}
	// MySQL returns 0 for update with no changes
	if result.RowsAffected != 0 {
		t.Errorf("expected 0 rows affected (no change), got %d", result.RowsAffected)
	}
}
