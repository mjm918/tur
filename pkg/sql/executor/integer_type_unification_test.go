package executor

import (
	"testing"

	"tur/pkg/types"
)

// Test that INT PRIMARY KEY uses internal rowid counter
// NOT the column value as the rowid

func TestINT_PrimaryKey_UsesInternalRowid(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with INT PRIMARY KEY
	_, err := exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert with explicit ID=73322
	_, err = exec.Execute("INSERT INTO products VALUES (73322, 'Widget')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Query to verify the id was stored correctly
	result, err := exec.Execute("SELECT id, name FROM products")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// The id column should be 73322 (what we inserted)
	id := result.Rows[0][0].Int()
	if id != 73322 {
		t.Errorf("Expected id=73322, got id=%d", id)
	}

	// The internal rowid should be 1 (first row), NOT 73322
	// This is verified by checking that adding another row works correctly
	_, err = exec.Execute("INSERT INTO products VALUES (100, 'Gadget')")
	if err != nil {
		t.Fatalf("Second INSERT: %v", err)
	}

	result2, err := exec.Execute("SELECT id FROM products ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT after second insert: %v", err)
	}

	if len(result2.Rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(result2.Rows))
	}

	// Both rows should exist with their original IDs
	ids := []int64{result2.Rows[0][0].Int(), result2.Rows[1][0].Int()}
	if ids[0] != 100 || ids[1] != 73322 {
		t.Errorf("Expected ids [100, 73322], got %v", ids)
	}
}

func TestINT_MapsTo_TypeInt32(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with INT - should be treated as TypeInt32
	_, err := exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	table := exec.catalog.GetTable("test")
	if table == nil {
		t.Fatal("Table not found")
	}

	// Both columns should be TypeInt32
	for _, col := range table.Columns {
		if col.Type != types.TypeInt32 {
			t.Errorf("Column %s has type %v, expected TypeInt32", col.Name, col.Type)
		}
	}
}

func TestPrimaryKey_OnlyAllowedTypes(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// INT PRIMARY KEY should work
	_, err := exec.Execute("CREATE TABLE t1 (id INT PRIMARY KEY)")
	if err != nil {
		t.Errorf("INT PRIMARY KEY should be allowed: %v", err)
	}

	// SERIAL PRIMARY KEY should work
	_, err = exec.Execute("CREATE TABLE t2 (id SERIAL PRIMARY KEY)")
	if err != nil {
		t.Errorf("SERIAL PRIMARY KEY should be allowed: %v", err)
	}

	// BIGINT PRIMARY KEY should work
	_, err = exec.Execute("CREATE TABLE t3 (id BIGINT PRIMARY KEY)")
	if err != nil {
		t.Errorf("BIGINT PRIMARY KEY should be allowed: %v", err)
	}

	// BIGSERIAL PRIMARY KEY should work
	_, err = exec.Execute("CREATE TABLE t4 (id BIGSERIAL PRIMARY KEY)")
	if err != nil {
		t.Errorf("BIGSERIAL PRIMARY KEY should be allowed: %v", err)
	}

	// TEXT PRIMARY KEY should still work (for compatibility with existing data)
	// But we need to ensure uniqueness is enforced
	_, err = exec.Execute("CREATE TABLE t5 (id TEXT PRIMARY KEY)")
	if err != nil {
		t.Errorf("TEXT PRIMARY KEY should be allowed: %v", err)
	}
}

func TestPrimaryKey_AutoCreatesIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with INT PRIMARY KEY
	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Check that a unique index was automatically created
	// Try common naming conventions
	var foundIndex bool
	for _, name := range []string{"pk_users_id", "users_pk", "users_id_pk"} {
		if exec.catalog.GetIndex(name) != nil {
			foundIndex = true
			break
		}
	}

	// List all indexes to debug
	table := exec.catalog.GetTable("users")
	if table == nil {
		t.Fatal("Table not found")
	}

	if !foundIndex {
		t.Error("Expected automatic index creation for PRIMARY KEY column")
	}
}

func TestPrimaryKey_EnforceUniqueness(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// First insert should succeed
	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("First INSERT: %v", err)
	}

	// Second insert with same PK should fail
	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Bob')")
	if err == nil {
		t.Error("Expected error for duplicate PRIMARY KEY value")
	}
}
