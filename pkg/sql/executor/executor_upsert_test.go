package executor

import (
	"os"
	"testing"

	"tur/pkg/pager"
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
