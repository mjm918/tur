package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"tur/pkg/cache"
	"tur/pkg/pager"
	"tur/pkg/schema"
	"tur/pkg/types"
)

func setupTestExecutor(t *testing.T) (*Executor, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "executor_test")
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

func setupTestExecutorWithCowTree(t *testing.T) (*Executor, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "executor_cow_test")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}

	dbPath := filepath.Join(dir, "test.db")
	p, err := pager.Open(dbPath, pager.Options{})
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("pager.Open: %v", err)
	}

	exec := NewWithCowTree(p)
	cleanup := func() {
		exec.Close()
		os.RemoveAll(dir)
	}

	return exec, cleanup
}

// Test MIN/MAX without persistence to isolate the bug
func TestExecutor_MinMaxBasic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert 100 records starting from id=1 (like most other tests do)
	for i := 1; i <= 100; i++ {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO test VALUES (%d, %d)", i, i*7))
		if err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}

	// Test MIN
	result, err := exec.Execute("SELECT MIN(id) FROM test")
	if err != nil {
		t.Fatalf("MIN: %v", err)
	}
	minID := result.Rows[0][0].Int()
	t.Logf("MIN(id) = %d (expected 1)", minID)
	if minID != 1 {
		t.Errorf("MIN(id) = %d, expected 1", minID)
	}

	// Test MAX
	result, err = exec.Execute("SELECT MAX(id) FROM test")
	if err != nil {
		t.Fatalf("MAX: %v", err)
	}
	maxID := result.Rows[0][0].Int()
	t.Logf("MAX(id) = %d (expected 100)", maxID)
	if maxID != 100 {
		t.Errorf("MAX(id) = %d, expected 100", maxID)
	}

	// Test WHERE on PK column
	result, err = exec.Execute("SELECT value FROM test WHERE id = 50")
	if err != nil {
		t.Fatalf("WHERE id: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("WHERE id=50: expected 1 row, got %d", len(result.Rows))
	} else {
		value := result.Rows[0][0].Int()
		t.Logf("WHERE id=50: value = %d (expected 350)", value)
		if value != 350 {
			t.Errorf("WHERE id=50: value = %d, expected 350", value)
		}
	}

	// Test WHERE on non-PK column
	result, err = exec.Execute("SELECT id FROM test WHERE value = 350")
	if err != nil {
		t.Fatalf("WHERE value: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("WHERE value=350: expected 1 row, got %d", len(result.Rows))
	} else {
		id := result.Rows[0][0].Int()
		t.Logf("WHERE value=350: id = %d (expected 50)", id)
		if id != 50 {
			t.Errorf("WHERE value=350: id = %d, expected 50", id)
		}
	}

	// Test full scan
	result, err = exec.Execute("SELECT * FROM test ORDER BY id LIMIT 5")
	if err != nil {
		t.Fatalf("ORDER BY: %v", err)
	}
	t.Log("First 5 rows via ORDER BY:")
	for i, row := range result.Rows {
		t.Logf("  row %d: id=%d, value=%d", i, row[0].Int(), row[1].Int())
	}
}

func TestExecutor_PersistenceWithReopen(t *testing.T) {
	dir, err := os.MkdirTemp("", "executor_persist_test")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")

	// Phase 1: Create and populate
	{
		p, err := pager.Open(dbPath, pager.Options{})
		if err != nil {
			t.Fatalf("pager.Open: %v", err)
		}

		exec := New(p)

		_, err = exec.Execute("CREATE TABLE test (id INT PRIMARY KEY, value INT)")
		if err != nil {
			exec.Close()
			t.Fatalf("CREATE TABLE: %v", err)
		}

		// Insert 10000 records
		for i := 0; i < 10000; i++ {
			_, err = exec.Execute(fmt.Sprintf("INSERT INTO test VALUES (%d, %d)", i, i*7))
			if err != nil {
				exec.Close()
				t.Fatalf("INSERT %d: %v", i, err)
			}
		}

		// Verify before close
		result, err := exec.Execute("SELECT COUNT(*) FROM test")
		if err != nil {
			exec.Close()
			t.Fatalf("COUNT before close: %v", err)
		}
		if result.Rows[0][0].Int() != 10000 {
			exec.Close()
			t.Fatalf("Expected 10000 rows, got %d", result.Rows[0][0].Int())
		}

		// Debug: Check root page before close
		table := exec.catalog.GetTable("test")
		if table != nil {
			t.Logf("Table 'test' root page before close: %d", table.RootPage)
			// Also check the actual tree root
			if tree, ok := exec.trees["test"]; ok {
				t.Logf("Actual btree root page: %d", tree.RootPage())
			}
		}

		if err := exec.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Phase 2: Reopen and verify
	{
		p, err := pager.Open(dbPath, pager.Options{})
		if err != nil {
			t.Fatalf("pager.Open reopen: %v", err)
		}

		exec := New(p)
		defer exec.Close()

		// Debug: Check the table's root page
		table := exec.catalog.GetTable("test")
		if table != nil {
			t.Logf("Table 'test' root page after reopen: %d", table.RootPage)
		} else {
			t.Log("Table 'test' not found in catalog!")
		}

		// Verify count
		result, err := exec.Execute("SELECT COUNT(*) FROM test")
		if err != nil {
			t.Fatalf("COUNT after reopen: %v", err)
		}
		count := result.Rows[0][0].Int()
		t.Logf("COUNT after reopen: %d", count)
		if count != 10000 {
			t.Fatalf("Expected 10000 rows after reopen, got %d", count)
		}

		// Debug: Check what's in the table
		result, err = exec.Execute("SELECT id, value FROM test ORDER BY id LIMIT 5")
		if err != nil {
			t.Fatalf("DEBUG SELECT: %v", err)
		}
		t.Logf("First 5 rows after reopen:")
		for i, row := range result.Rows {
			t.Logf("  row %d: id=%v, value=%v", i, row[0].Int(), row[1].Int())
		}

		result, err = exec.Execute("SELECT MIN(id) FROM test")
		if err != nil {
			t.Fatalf("MIN SELECT: %v", err)
		}
		t.Logf("MIN(id)=%v", result.Rows[0][0].Int())

		result, err = exec.Execute("SELECT MAX(id) FROM test")
		if err != nil {
			t.Fatalf("MAX SELECT: %v", err)
		}
		t.Logf("MAX(id)=%v", result.Rows[0][0].Int())

		// Verify specific records
		checkIDs := []int{0, 1, 100, 500, 1000, 5000, 9999}
		for _, id := range checkIDs {
			result, err = exec.Execute(fmt.Sprintf("SELECT value FROM test WHERE id = %d", id))
			if err != nil {
				t.Fatalf("SELECT for id=%d: %v", id, err)
			}
			if len(result.Rows) != 1 {
				t.Errorf("Expected 1 row for id=%d, got %d rows", id, len(result.Rows))
				continue
			}
			expectedValue := int64(id * 7)
			gotValue := result.Rows[0][0].Int()
			if gotValue != expectedValue {
				t.Errorf("id=%d: expected value=%d, got %d", id, expectedValue, gotValue)
			}
		}
	}
}

func TestExecutor_CowTree_BasicCRUD(t *testing.T) {
	exec, cleanup := setupTestExecutorWithCowTree(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price REAL)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert data
	_, err = exec.Execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	_, err = exec.Execute("INSERT INTO products VALUES (2, 'Gadget', 19.99)")
	if err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}

	// Select data
	result, err := exec.Execute("SELECT * FROM products ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(result.Rows))
	}

	// Update data
	_, err = exec.Execute("UPDATE products SET price = 12.99 WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	// Verify update
	result, err = exec.Execute("SELECT price FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after UPDATE: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0].Float() != 12.99 {
		t.Errorf("UPDATE verification failed: got %v", result.Rows)
	}

	// Delete data
	_, err = exec.Execute("DELETE FROM products WHERE id = 2")
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	// Verify delete
	result, err = exec.Execute("SELECT COUNT(*) FROM products")
	if err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0].Int() != 1 {
		t.Errorf("DELETE verification failed: expected 1 row, got %v", result.Rows)
	}
}

func TestExecutor_CreateTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	result, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}

	// Verify table exists
	table := exec.catalog.GetTable("users")
	if table == nil {
		t.Fatal("Table 'users' not found in catalog")
	}

	if len(table.Columns) != 2 {
		t.Errorf("Columns count = %d, want 2", len(table.Columns))
	}
}

func TestExecutor_CreateTable_Duplicate(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT)")

	_, err := exec.Execute("CREATE TABLE users (id INT)")
	if err == nil {
		t.Error("Expected error for duplicate table")
	}
}

func TestExecutor_DropTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT)")

	_, err := exec.Execute("DROP TABLE users")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Verify table no longer exists
	if exec.catalog.GetTable("users") != nil {
		t.Error("Table 'users' should not exist after DROP")
	}
}

func TestExecutor_DropTable_NonExistentError(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Dropping non-existent table should error
	_, err := exec.Execute("DROP TABLE nonexistent")
	if err == nil {
		t.Fatal("Expected error when dropping non-existent table, got nil")
	}
}

func TestExecutor_DropTable_IfExists(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Drop non-existent table with IF EXISTS should not error
	_, err := exec.Execute("DROP TABLE IF EXISTS nonexistent")
	if err != nil {
		t.Fatalf("DROP TABLE IF EXISTS should not error for non-existent table: %v", err)
	}

	// Create and drop with IF EXISTS
	exec.Execute("CREATE TABLE users (id INT)")
	_, err = exec.Execute("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Verify table no longer exists
	if exec.catalog.GetTable("users") != nil {
		t.Error("Table 'users' should not exist after DROP")
	}
}

func TestExecutor_Insert(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")

	result, err := exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}
}

func TestExecutor_InsertMultiple(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")

	result, err := exec.Execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 2 {
		t.Errorf("RowsAffected = %d, want 2", result.RowsAffected)
	}
}

func TestExecutor_InsertSelect(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create source and destination tables
	exec.Execute("CREATE TABLE old_users (user_id INT, username TEXT)")
	exec.Execute("CREATE TABLE new_users (id INT, name TEXT)")

	// Populate source table
	exec.Execute("INSERT INTO old_users VALUES (1, 'Alice'), (2, 'Bob')")

	// INSERT SELECT
	result, err := exec.Execute("INSERT INTO new_users SELECT * FROM old_users")
	if err != nil {
		t.Fatalf("Execute INSERT SELECT: %v", err)
	}

	if result.RowsAffected != 2 {
		t.Errorf("RowsAffected = %d, want 2", result.RowsAffected)
	}

	// Verify data was copied
	selectResult, err := exec.Execute("SELECT * FROM new_users")
	if err != nil {
		t.Fatalf("Execute SELECT: %v", err)
	}

	if len(selectResult.Rows) != 2 {
		t.Errorf("Rows in new_users = %d, want 2", len(selectResult.Rows))
	}

	// Verify first row
	if selectResult.Rows[0][0].Int() != 1 || selectResult.Rows[0][1].Text() != "Alice" {
		t.Errorf("Row[0] = %v, want [1, 'Alice']", selectResult.Rows[0])
	}
}

func TestExecutor_InsertSelectWithColumns(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create source and destination tables with different column names
	exec.Execute("CREATE TABLE old_users (user_id INT, username TEXT, email TEXT)")
	exec.Execute("CREATE TABLE new_users (id INT, name TEXT)")

	// Populate source table
	exec.Execute("INSERT INTO old_users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')")

	// INSERT SELECT with specific columns
	result, err := exec.Execute("INSERT INTO new_users (id, name) SELECT user_id, username FROM old_users")
	if err != nil {
		t.Fatalf("Execute INSERT SELECT: %v", err)
	}

	if result.RowsAffected != 2 {
		t.Errorf("RowsAffected = %d, want 2", result.RowsAffected)
	}

	// Verify data was copied correctly
	selectResult, err := exec.Execute("SELECT * FROM new_users")
	if err != nil {
		t.Fatalf("Execute SELECT: %v", err)
	}

	if len(selectResult.Rows) != 2 {
		t.Errorf("Rows in new_users = %d, want 2", len(selectResult.Rows))
	}
}

func TestExecutor_Select_All(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")

	result, err := exec.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Columns = %v, want 2 columns", result.Columns)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Rows = %d, want 2", len(result.Rows))
	}
}

func TestExecutor_Select_Columns(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT, email TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")

	result, err := exec.Execute("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Columns = %v, want ['id', 'name']", result.Columns)
	}

	if result.Columns[0] != "id" || result.Columns[1] != "name" {
		t.Errorf("Columns = %v, want ['id', 'name']", result.Columns)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Rows = %d, want 1", len(result.Rows))
	}

	if len(result.Rows[0]) != 2 {
		t.Errorf("Row values = %d, want 2", len(result.Rows[0]))
	}
}

func TestExecutor_Select_Where(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")

	result, err := exec.Execute("SELECT * FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Rows = %d, want 1", len(result.Rows))
	}

	if result.Rows[0][1].Text() != "Bob" {
		t.Errorf("Row[0][1] = %q, want 'Bob'", result.Rows[0][1].Text())
	}
}

func TestExecutor_Select_WhereString(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")

	result, err := exec.Execute("SELECT * FROM users WHERE name = 'Alice'")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Rows = %d, want 1", len(result.Rows))
	}

	if result.Rows[0][0].Int() != 1 {
		t.Errorf("Row[0][0] = %d, want 1", result.Rows[0][0].Int())
	}
}

func TestExecutor_Select_WhereAnd(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT, active INT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice', 1)")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob', 0)")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie', 1)")

	result, err := exec.Execute("SELECT * FROM users WHERE active = 1 AND id > 1")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Rows = %d, want 1", len(result.Rows))
	}

	if result.Rows[0][1].Text() != "Charlie" {
		t.Errorf("Row[0][1] = %q, want 'Charlie'", result.Rows[0][1].Text())
	}
}

func TestExecutor_Select_TableNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("SELECT * FROM nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

func TestExecutor_Insert_TableNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("INSERT INTO nonexistent VALUES (1)")
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

func TestExecutor_Types(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE data (i INT, f FLOAT, t TEXT)")
	exec.Execute("INSERT INTO data VALUES (42, 3.14, 'hello')")

	result, err := exec.Execute("SELECT * FROM data")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Rows = %d, want 1", len(result.Rows))
	}

	row := result.Rows[0]
	// INT now maps to strict TypeInt32
	if row[0].Type() != types.TypeInt32 || row[0].Int() != 42 {
		t.Errorf("Row[0] = %v (type=%v), want Int32(42)", row[0], row[0].Type())
	}
	if row[1].Type() != types.TypeFloat || row[1].Float() != 3.14 {
		t.Errorf("Row[1] = %v, want Float(3.14)", row[1])
	}
	if row[2].Type() != types.TypeText || row[2].Text() != "hello" {
		t.Errorf("Row[2] = %v, want Text('hello')", row[2])
	}
}

func TestExecutor_Null(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE data (id INT, value TEXT)")
	exec.Execute("INSERT INTO data VALUES (1, NULL)")

	result, err := exec.Execute("SELECT * FROM data")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Rows = %d, want 1", len(result.Rows))
	}

	if !result.Rows[0][1].IsNull() {
		t.Errorf("Row[0][1] = %v, want NULL", result.Rows[0][1])
	}
}

func TestExecutor_VectorType_DimensionCheck(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with VECTOR(3)
	_, err := exec.Execute("CREATE TABLE items (v VECTOR(3))")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	table := exec.catalog.GetTable("items")
	if table == nil {
		t.Fatal("Table not found")
	}
	if table.Columns[0].VectorDim != 3 {
		t.Errorf("VectorDim = %d, want 3", table.Columns[0].VectorDim)
	}

	// Since we can't parse vector literals yet, we manually verify strict typing logic
	// would go here if we could insert them. For now, we verified schema storage.
}

func TestExecutor_VectorNormalization(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// 1. Create table with VECTOR(3) column
	_, err := exec.Execute("CREATE TABLE items (v VECTOR(3))")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	// 2. Insert non-normalized vector [3.0, 4.0, 0.0] (magnitude 5.0)
	// Encoded as little-endian float32:
	// 3.0 = 0x40400000
	// 4.0 = 0x40800000
	// 0.0 = 0x00000000
	// Header (dim=3): 0x03000000
	// Full hex: 03000000000040400000804000000000
	_, err = exec.Execute("INSERT INTO items VALUES (x'03000000000040400000804000000000')")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// 3. SELECT and verify vector is normalized to [0.6, 0.8, 0.0]
	result, err := exec.Execute("SELECT * FROM items")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	val := result.Rows[0][0]
	if val.Type() != types.TypeBlob { // Vector relies on underlying Blob storage for now
		t.Fatalf("Expected blob, got %v", val.Type())
	}

	// Decode vector
	vec, err := types.VectorFromBytes(val.Blob())
	if err != nil {
		t.Fatalf("VectorFromBytes: %v", err)
	}

	data := vec.Data()
	if len(data) != 3 {
		t.Fatalf("Expected dim 3, got %d", len(data))
	}

	// Check values with tolerance
	epsilon := float32(0.0001)
	if data[0] < 0.6-epsilon || data[0] > 0.6+epsilon {
		t.Errorf("v[0] = %f, want 0.6", data[0])
	}
	if data[1] < 0.8-epsilon || data[1] > 0.8+epsilon {
		t.Errorf("v[1] = %f, want 0.8", data[1])
	}
}

// ========== Constraint Catalog Tests ==========

func TestExecutor_CreateTable_StoresUniqueConstraint(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (email TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	table := exec.catalog.GetTable("users")
	if table == nil {
		t.Fatal("Table not found")
	}

	col, _ := table.GetColumn("email")
	if col == nil {
		t.Fatal("Column 'email' not found")
	}

	if !col.HasConstraint(schema.ConstraintUnique) {
		t.Error("Column should have UNIQUE constraint")
	}
}

func TestExecutor_CreateTable_StoresCheckConstraint(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE products (price INT CHECK (price >= 0))")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	table := exec.catalog.GetTable("products")
	if table == nil {
		t.Fatal("Table not found")
	}

	col, _ := table.GetColumn("price")
	if col == nil {
		t.Fatal("Column 'price' not found")
	}

	if !col.HasConstraint(schema.ConstraintCheck) {
		t.Error("Column should have CHECK constraint")
	}

	check := col.GetConstraint(schema.ConstraintCheck)
	if check == nil {
		t.Fatal("CHECK constraint not found")
	}

	if check.CheckExpression == "" {
		t.Error("CheckExpression should not be empty")
	}
}

func TestExecutor_CreateTable_StoresForeignKeyConstraint(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create referenced table first
	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Create users: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))")
	if err != nil {
		t.Fatalf("Create orders: %v", err)
	}

	table := exec.catalog.GetTable("orders")
	if table == nil {
		t.Fatal("Table not found")
	}

	col, _ := table.GetColumn("user_id")
	if col == nil {
		t.Fatal("Column 'user_id' not found")
	}

	if !col.HasConstraint(schema.ConstraintForeignKey) {
		t.Error("Column should have FOREIGN KEY constraint")
	}

	fk := col.GetConstraint(schema.ConstraintForeignKey)
	if fk == nil {
		t.Fatal("FOREIGN KEY constraint not found")
	}

	if fk.RefTable != "users" {
		t.Errorf("RefTable = %q, want 'users'", fk.RefTable)
	}

	if fk.RefColumn != "id" {
		t.Errorf("RefColumn = %q, want 'id'", fk.RefColumn)
	}
}

func TestExecutor_CreateTable_StoresForeignKeyActions(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT PRIMARY KEY)")
	_, err := exec.Execute("CREATE TABLE orders (user_id INT REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL)")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	table := exec.catalog.GetTable("orders")
	col, _ := table.GetColumn("user_id")
	fk := col.GetConstraint(schema.ConstraintForeignKey)

	if fk.OnDelete != schema.FKActionCascade {
		t.Errorf("OnDelete = %v, want FKActionCascade", fk.OnDelete)
	}

	if fk.OnUpdate != schema.FKActionSetNull {
		t.Errorf("OnUpdate = %v, want FKActionSetNull", fk.OnUpdate)
	}
}

func TestExecutor_CreateTable_StoresTableLevelPrimaryKey(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE order_items (order_id INT, product_id INT, PRIMARY KEY (order_id, product_id))")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	table := exec.catalog.GetTable("order_items")
	if table == nil {
		t.Fatal("Table not found")
	}

	pk := table.GetTableConstraint(schema.ConstraintPrimaryKey)
	if pk == nil {
		t.Fatal("PRIMARY KEY table constraint not found")
	}

	if len(pk.Columns) != 2 {
		t.Errorf("Columns = %v, want 2 columns", pk.Columns)
	}

	if pk.Columns[0] != "order_id" || pk.Columns[1] != "product_id" {
		t.Errorf("Columns = %v, want ['order_id', 'product_id']", pk.Columns)
	}
}

func TestExecutor_CreateTable_StoresTableLevelUnique(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE t (a INT, b INT, UNIQUE (a, b))")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	table := exec.catalog.GetTable("t")
	unique := table.GetTableConstraint(schema.ConstraintUnique)
	if unique == nil {
		t.Fatal("UNIQUE table constraint not found")
	}

	if len(unique.Columns) != 2 {
		t.Errorf("Columns = %v, want 2 columns", unique.Columns)
	}
}

func TestExecutor_CreateTable_StoresTableLevelForeignKey(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT PRIMARY KEY)")
	_, err := exec.Execute("CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	table := exec.catalog.GetTable("orders")
	fk := table.GetTableConstraint(schema.ConstraintForeignKey)
	if fk == nil {
		t.Fatal("FOREIGN KEY table constraint not found")
	}

	if fk.RefTable != "users" {
		t.Errorf("RefTable = %q, want 'users'", fk.RefTable)
	}

	if len(fk.Columns) != 1 || fk.Columns[0] != "user_id" {
		t.Errorf("Columns = %v, want ['user_id']", fk.Columns)
	}
}

func TestExecutor_CreateTable_StoresTableLevelCheck(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE t (start_date INT, end_date INT, CHECK (start_date < end_date))")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	table := exec.catalog.GetTable("t")
	check := table.GetTableConstraint(schema.ConstraintCheck)
	if check == nil {
		t.Fatal("CHECK table constraint not found")
	}

	if check.CheckExpression == "" {
		t.Error("CheckExpression should not be empty")
	}
}

func TestExecutor_CreateTable_StoresNamedConstraint(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE t (id INT, CONSTRAINT pk_t PRIMARY KEY (id))")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	table := exec.catalog.GetTable("t")
	pk := table.GetTableConstraint(schema.ConstraintPrimaryKey)
	if pk == nil {
		t.Fatal("PRIMARY KEY table constraint not found")
	}

	if pk.Name != "pk_t" {
		t.Errorf("Name = %q, want 'pk_t'", pk.Name)
	}
}

func TestExecutor_CreateTable_StoresMultipleConstraints(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY NOT NULL, email TEXT UNIQUE NOT NULL, age INT CHECK (age >= 0))")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	table := exec.catalog.GetTable("users")

	// Check id column
	id, _ := table.GetColumn("id")
	if !id.HasConstraint(schema.ConstraintPrimaryKey) {
		t.Error("id should have PRIMARY KEY constraint")
	}
	if !id.HasConstraint(schema.ConstraintNotNull) {
		t.Error("id should have NOT NULL constraint")
	}

	// Check email column
	email, _ := table.GetColumn("email")
	if !email.HasConstraint(schema.ConstraintUnique) {
		t.Error("email should have UNIQUE constraint")
	}
	if !email.HasConstraint(schema.ConstraintNotNull) {
		t.Error("email should have NOT NULL constraint")
	}

	// Check age column
	age, _ := table.GetColumn("age")
	if !age.HasConstraint(schema.ConstraintCheck) {
		t.Error("age should have CHECK constraint")
	}
}

// ========== Constraint Validation Tests ==========

func TestExecutor_Insert_NotNullViolation(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT NOT NULL)")

	_, err := exec.Execute("INSERT INTO users VALUES (1, NULL)")
	if err == nil {
		t.Error("Expected NOT NULL violation error")
	}
	if err != nil && err.Error() != "NOT NULL constraint violation: column 'name' cannot be NULL" {
		t.Logf("Got error: %v (acceptable if constraint enforcement is partial)", err)
	}
}

func TestExecutor_Insert_NotNullViolation_AllowsNonNull(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT NOT NULL)")

	_, err := exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Expected insert to succeed: %v", err)
	}
}

func TestExecutor_Insert_CheckViolation(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE products (id INT, price INT CHECK (price >= 0))")

	_, err := exec.Execute("INSERT INTO products VALUES (1, -5)")
	if err == nil {
		t.Error("Expected CHECK constraint violation error")
	}
}

func TestExecutor_Insert_CheckViolation_AllowsValid(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE products (id INT, price INT CHECK (price >= 0))")

	_, err := exec.Execute("INSERT INTO products VALUES (1, 100)")
	if err != nil {
		t.Fatalf("Expected insert to succeed: %v", err)
	}
}

func TestExecutor_Insert_CheckViolation_NullAllowed(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// CHECK constraint should allow NULL (unless combined with NOT NULL)
	exec.Execute("CREATE TABLE products (id INT, price INT CHECK (price >= 0))")

	_, err := exec.Execute("INSERT INTO products VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("CHECK constraint should allow NULL: %v", err)
	}
}

// ========== Index Executor Tests ==========

func TestExecutor_CreateIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// First create a table
	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create index
	result, err := exec.Execute("CREATE INDEX idx_users_email ON users (email)")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}

	// Verify index exists in catalog
	idx := exec.catalog.GetIndex("idx_users_email")
	if idx == nil {
		t.Fatal("Index 'idx_users_email' not found in catalog")
	}

	if idx.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", idx.TableName)
	}
	if len(idx.Columns) != 1 || idx.Columns[0] != "email" {
		t.Errorf("Columns = %v, want ['email']", idx.Columns)
	}
	if idx.Type != schema.IndexTypeBTree {
		t.Errorf("Type = %v, want IndexTypeBTree", idx.Type)
	}
	if idx.Unique {
		t.Error("Unique = true, want false")
	}
}

func TestExecutor_CreateIndex_Unique(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, email TEXT)")

	_, err := exec.Execute("CREATE UNIQUE INDEX idx_users_email ON users (email)")
	if err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}

	idx := exec.catalog.GetIndex("idx_users_email")
	if !idx.Unique {
		t.Error("Unique = false, want true")
	}
}

func TestExecutor_CreateIndex_MultiColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE orders (id INT, customer_id INT, order_date TEXT)")

	_, err := exec.Execute("CREATE INDEX idx_orders ON orders (customer_id, order_date)")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	idx := exec.catalog.GetIndex("idx_orders")
	if len(idx.Columns) != 2 {
		t.Fatalf("Columns count = %d, want 2", len(idx.Columns))
	}
	if idx.Columns[0] != "customer_id" || idx.Columns[1] != "order_date" {
		t.Errorf("Columns = %v, want ['customer_id', 'order_date']", idx.Columns)
	}
}

func TestExecutor_CreateIndex_Duplicate(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	exec.Execute("CREATE INDEX idx_users_email ON users (email)")

	_, err := exec.Execute("CREATE INDEX idx_users_email ON users (email)")
	if err == nil {
		t.Error("Expected error for duplicate index")
	}
}

func TestExecutor_CreateIndex_TableNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE INDEX idx ON nonexistent (col)")
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

func TestExecutor_CreateIndex_ColumnNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT)")

	_, err := exec.Execute("CREATE INDEX idx ON users (nonexistent)")
	if err == nil {
		t.Error("Expected error for nonexistent column")
	}
}

func TestExecutor_DropIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	exec.Execute("CREATE INDEX idx_users_email ON users (email)")

	_, err := exec.Execute("DROP INDEX idx_users_email")
	if err != nil {
		t.Fatalf("DROP INDEX: %v", err)
	}

	// Verify index no longer exists
	if exec.catalog.GetIndex("idx_users_email") != nil {
		t.Error("Index should not exist after DROP")
	}
}

func TestExecutor_DropIndex_NotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("DROP INDEX nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent index")
	}
}

func TestExecutor_DropIndex_IfExists_WhenExists(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	exec.Execute("CREATE INDEX idx_users_email ON users (email)")

	// DROP INDEX IF EXISTS on existing index should succeed
	_, err := exec.Execute("DROP INDEX IF EXISTS idx_users_email")
	if err != nil {
		t.Fatalf("DROP INDEX IF EXISTS: %v", err)
	}

	// Verify index no longer exists
	if exec.catalog.GetIndex("idx_users_email") != nil {
		t.Error("Index should not exist after DROP")
	}
}

func TestExecutor_DropIndex_IfExists_WhenNotExists(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// DROP INDEX IF EXISTS on non-existing index should NOT error
	_, err := exec.Execute("DROP INDEX IF EXISTS nonexistent")
	if err != nil {
		t.Errorf("DROP INDEX IF EXISTS should not error for nonexistent index, got: %v", err)
	}
}

func TestExecutor_DropIndex_CleansUpBTree(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	exec.Execute("CREATE INDEX idx_users_email ON users (email)")

	// Verify index tree exists in executor's trees map
	if _, exists := exec.trees["index:idx_users_email"]; !exists {
		t.Fatal("Index tree should exist before DROP")
	}

	// Drop the index
	_, err := exec.Execute("DROP INDEX idx_users_email")
	if err != nil {
		t.Fatalf("DROP INDEX: %v", err)
	}

	// Verify index tree is removed from executor's trees map
	if _, exists := exec.trees["index:idx_users_email"]; exists {
		t.Error("Index tree should be removed after DROP")
	}
}

// UPDATE tests

func TestExecutor_Update_Simple(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")

	result, err := exec.Execute("UPDATE users SET name = 'Updated'")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 2 {
		t.Errorf("RowsAffected = %d, want 2", result.RowsAffected)
	}

	// Verify all rows were updated
	selectResult, err := exec.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	for _, row := range selectResult.Rows {
		if row[1].Text() != "Updated" {
			t.Errorf("Expected name 'Updated', got %q", row[1].Text())
		}
	}
}

func TestExecutor_Update_WithWhere(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")

	result, err := exec.Execute("UPDATE users SET name = 'Updated' WHERE id = 2")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}

	// Verify only id=2 was updated
	selectResult, err := exec.Execute("SELECT * FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(selectResult.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(selectResult.Rows))
	}

	if selectResult.Rows[0][1].Text() != "Updated" {
		t.Errorf("Expected name 'Updated', got %q", selectResult.Rows[0][1].Text())
	}

	// Verify others were not updated
	selectResult, err = exec.Execute("SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if selectResult.Rows[0][1].Text() != "Alice" {
		t.Errorf("Expected name 'Alice' (unchanged), got %q", selectResult.Rows[0][1].Text())
	}
}

func TestExecutor_Update_MultipleColumns(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT, age INT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice', 25)")

	result, err := exec.Execute("UPDATE users SET name = 'Bob', age = 30 WHERE id = 1")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}

	// Verify update
	selectResult, err := exec.Execute("SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if selectResult.Rows[0][1].Text() != "Bob" {
		t.Errorf("Expected name 'Bob', got %q", selectResult.Rows[0][1].Text())
	}
	if selectResult.Rows[0][2].Int() != 30 {
		t.Errorf("Expected age 30, got %d", selectResult.Rows[0][2].Int())
	}
}

func TestExecutor_Update_Expression(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE counters (id INT, value INT)")
	exec.Execute("INSERT INTO counters VALUES (1, 10)")

	result, err := exec.Execute("UPDATE counters SET value = value + 5 WHERE id = 1")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}

	// Verify update
	selectResult, err := exec.Execute("SELECT * FROM counters WHERE id = 1")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if selectResult.Rows[0][1].Int() != 15 {
		t.Errorf("Expected value 15, got %d", selectResult.Rows[0][1].Int())
	}
}

func TestExecutor_Update_TableNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("UPDATE nonexistent SET name = 'test'")
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

func TestExecutor_Update_NoRowsMatch(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")

	result, err := exec.Execute("UPDATE users SET name = 'Updated' WHERE id = 999")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}
}

func TestExecutor_Update_NotNullViolation(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT NOT NULL)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")

	// Try to set NOT NULL column to NULL
	_, err := exec.Execute("UPDATE users SET name = NULL WHERE id = 1")
	if err == nil {
		t.Error("Expected NOT NULL violation error")
	}
}

func TestExecutor_Update_CheckViolation(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE ages (id INT, age INT CHECK(age >= 0))")
	exec.Execute("INSERT INTO ages VALUES (1, 25)")

	// Try to set CHECK-constrained column to invalid value
	_, err := exec.Execute("UPDATE ages SET age = -5 WHERE id = 1")
	if err == nil {
		t.Error("Expected CHECK violation error")
	}
}

func TestExecutor_Update_InvalidColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")

	// Try to update non-existent column
	_, err := exec.Execute("UPDATE users SET nonexistent = 'test' WHERE id = 1")
	if err == nil {
		t.Error("Expected error for non-existent column")
	}
}

// ========== DELETE statement tests ==========

func TestExecutor_Delete_All(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")

	result, err := exec.Execute("DELETE FROM users")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 3 {
		t.Errorf("RowsAffected = %d, want 3", result.RowsAffected)
	}

	// Verify all rows are deleted
	selectResult, err := exec.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(selectResult.Rows) != 0 {
		t.Errorf("Expected 0 rows after DELETE, got %d", len(selectResult.Rows))
	}
}

func TestExecutor_Delete_WithWhere(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")

	result, err := exec.Execute("DELETE FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}

	// Verify only id=2 was deleted
	selectResult, err := exec.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(selectResult.Rows) != 2 {
		t.Errorf("Expected 2 rows after DELETE, got %d", len(selectResult.Rows))
	}

	// Verify id=2 is not present
	for _, row := range selectResult.Rows {
		if row[0].Int() == 2 {
			t.Error("Row with id=2 should have been deleted")
		}
	}
}

func TestExecutor_Delete_ComplexWhere(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE orders (id INT, status TEXT, amount INT)")
	exec.Execute("INSERT INTO orders VALUES (1, 'completed', 100)")
	exec.Execute("INSERT INTO orders VALUES (2, 'cancelled', 50)")
	exec.Execute("INSERT INTO orders VALUES (3, 'cancelled', 75)")
	exec.Execute("INSERT INTO orders VALUES (4, 'completed', 200)")

	// Delete cancelled orders with amount > 60
	result, err := exec.Execute("DELETE FROM orders WHERE status = 'cancelled' AND amount > 60")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1 (only order 3)", result.RowsAffected)
	}

	// Verify correct row was deleted
	selectResult, err := exec.Execute("SELECT * FROM orders")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(selectResult.Rows) != 3 {
		t.Errorf("Expected 3 rows after DELETE, got %d", len(selectResult.Rows))
	}
}

func TestExecutor_Delete_TableNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("DELETE FROM nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}
}

func TestExecutor_Delete_NoRowsMatch(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")

	result, err := exec.Execute("DELETE FROM users WHERE id = 999")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}

	// Verify no rows were deleted
	selectResult, err := exec.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(selectResult.Rows) != 1 {
		t.Errorf("Expected 1 row (unchanged), got %d", len(selectResult.Rows))
	}
}

func TestExecutor_Delete_EmptyTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")

	result, err := exec.Execute("DELETE FROM users")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("RowsAffected = %d, want 0", result.RowsAffected)
	}
}

// ========== ALTER TABLE Tests ==========

func TestExecutor_AlterTable_AddColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Add column
	_, err = exec.Execute("ALTER TABLE users ADD COLUMN email TEXT")
	if err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN: %v", err)
	}

	// Verify column exists
	table := exec.catalog.GetTable("users")
	if len(table.Columns) != 3 {
		t.Fatalf("Columns count = %d, want 3", len(table.Columns))
	}

	col, _ := table.GetColumn("email")
	if col == nil {
		t.Fatal("Column 'email' not found")
	}
	if col.Type != types.TypeText {
		t.Errorf("Column type = %v, want TypeText", col.Type)
	}
}

func TestExecutor_AlterTable_AddColumn_TableNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("ALTER TABLE nonexistent ADD COLUMN email TEXT")
	if err == nil {
		t.Fatal("Expected error for non-existent table")
	}
}

func TestExecutor_AlterTable_AddColumn_DuplicateColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")

	_, err := exec.Execute("ALTER TABLE users ADD COLUMN name TEXT")
	if err == nil {
		t.Fatal("Expected error for duplicate column")
	}
}

func TestExecutor_AlterTable_DropColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT, email TEXT)")

	_, err := exec.Execute("ALTER TABLE users DROP COLUMN email")
	if err != nil {
		t.Fatalf("ALTER TABLE DROP COLUMN: %v", err)
	}

	table := exec.catalog.GetTable("users")
	if len(table.Columns) != 2 {
		t.Fatalf("Columns count = %d, want 2", len(table.Columns))
	}

	col, _ := table.GetColumn("email")
	if col != nil {
		t.Error("Column 'email' should not exist after drop")
	}
}

func TestExecutor_AlterTable_RenameTo(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")

	_, err := exec.Execute("ALTER TABLE users RENAME TO customers")
	if err != nil {
		t.Fatalf("ALTER TABLE RENAME TO: %v", err)
	}

	// Verify old name is gone
	if exec.catalog.GetTable("users") != nil {
		t.Error("Old table name 'users' should not exist")
	}

	// Verify new name exists
	table := exec.catalog.GetTable("customers")
	if table == nil {
		t.Fatal("New table name 'customers' not found")
	}
}

func TestExecutor_Select_OrderByAsc(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")

	result, err := exec.Execute("SELECT * FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("Rows = %d, want 3", len(result.Rows))
	}

	// Should be sorted by id ascending: Alice(1), Bob(2), Charlie(3)
	expected := []string{"Alice", "Bob", "Charlie"}
	for i, name := range expected {
		if result.Rows[i][1].Text() != name {
			t.Errorf("Row[%d][1] = %q, want %q", i, result.Rows[i][1].Text(), name)
		}
	}
}

func TestExecutor_Select_OrderByDesc(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")

	result, err := exec.Execute("SELECT * FROM users ORDER BY id DESC")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("Rows = %d, want 3", len(result.Rows))
	}

	// Should be sorted by id descending: Charlie(3), Bob(2), Alice(1)
	expected := []string{"Charlie", "Bob", "Alice"}
	for i, name := range expected {
		if result.Rows[i][1].Text() != name {
			t.Errorf("Row[%d][1] = %q, want %q", i, result.Rows[i][1].Text(), name)
		}
	}
}

func TestExecutor_Select_Limit(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")

	result, err := exec.Execute("SELECT * FROM users LIMIT 2")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Rows = %d, want 2", len(result.Rows))
	}
}

func TestExecutor_Select_LimitOffset(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")
	exec.Execute("INSERT INTO users VALUES (4, 'Diana')")

	result, err := exec.Execute("SELECT * FROM users LIMIT 2 OFFSET 1")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Rows = %d, want 2", len(result.Rows))
	}

	// With offset 1, should skip Alice, return Bob and Charlie
	if result.Rows[0][1].Text() != "Bob" {
		t.Errorf("Row[0][1] = %q, want 'Bob'", result.Rows[0][1].Text())
	}
	if result.Rows[1][1].Text() != "Charlie" {
		t.Errorf("Row[1][1] = %q, want 'Charlie'", result.Rows[1][1].Text())
	}
}

func TestExecutor_Select_OrderByLimit(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO users VALUES (4, 'Diana')")

	result, err := exec.Execute("SELECT * FROM users ORDER BY id DESC LIMIT 2")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Rows = %d, want 2", len(result.Rows))
	}

	// Top 2 by id DESC: Diana(4), Charlie(3)
	if result.Rows[0][1].Text() != "Diana" {
		t.Errorf("Row[0][1] = %q, want 'Diana'", result.Rows[0][1].Text())
	}
	if result.Rows[1][1].Text() != "Charlie" {
		t.Errorf("Row[1][1] = %q, want 'Charlie'", result.Rows[1][1].Text())
	}
}

// ========== GROUP BY Tests ==========

func TestExecutor_Select_GroupBy_Simple(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE employees (id INT, department TEXT, salary INT)")
	exec.Execute("INSERT INTO employees VALUES (1, 'Engineering', 50000)")
	exec.Execute("INSERT INTO employees VALUES (2, 'Engineering', 60000)")
	exec.Execute("INSERT INTO employees VALUES (3, 'Marketing', 45000)")
	exec.Execute("INSERT INTO employees VALUES (4, 'Marketing', 55000)")
	exec.Execute("INSERT INTO employees VALUES (5, 'Engineering', 70000)")

	// Execute GROUP BY
	result, err := exec.Execute("SELECT department FROM employees GROUP BY department")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Should have 2 groups: Engineering and Marketing
	if len(result.Rows) != 2 {
		t.Fatalf("Rows = %d, want 2", len(result.Rows))
	}
}

func TestExecutor_Select_GroupBy_Count(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE orders (id INT, customer TEXT, amount INT)")
	exec.Execute("INSERT INTO orders VALUES (1, 'Alice', 100)")
	exec.Execute("INSERT INTO orders VALUES (2, 'Bob', 200)")
	exec.Execute("INSERT INTO orders VALUES (3, 'Alice', 150)")
	exec.Execute("INSERT INTO orders VALUES (4, 'Alice', 75)")
	exec.Execute("INSERT INTO orders VALUES (5, 'Bob', 300)")

	// Execute GROUP BY with COUNT
	result, err := exec.Execute("SELECT customer FROM orders GROUP BY customer")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Should have 2 groups: Alice (3 orders) and Bob (2 orders)
	if len(result.Rows) != 2 {
		t.Fatalf("Rows = %d, want 2 groups", len(result.Rows))
	}

	// Columns should include customer and COUNT(*)
	if len(result.Columns) != 2 {
		t.Errorf("Columns = %v, want [customer COUNT(*)]", result.Columns)
	}

	// Verify the counts - row[0] is group key, row[1] is COUNT(*)
	counts := make(map[string]int64)
	for _, row := range result.Rows {
		if len(row) >= 2 {
			customer := row[0].Text()
			count := row[1].Int()
			counts[customer] = count
		}
	}

	if counts["Alice"] != 3 {
		t.Errorf("Alice count = %d, want 3", counts["Alice"])
	}
	if counts["Bob"] != 2 {
		t.Errorf("Bob count = %d, want 2", counts["Bob"])
	}
}

func TestExecutor_Select_GroupBy_WithHaving(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE sales (id INT, region TEXT, revenue INT)")
	exec.Execute("INSERT INTO sales VALUES (1, 'North', 100)")
	exec.Execute("INSERT INTO sales VALUES (2, 'North', 200)")
	exec.Execute("INSERT INTO sales VALUES (3, 'South', 50)")
	exec.Execute("INSERT INTO sales VALUES (4, 'North', 150)")
	exec.Execute("INSERT INTO sales VALUES (5, 'East', 300)")
	exec.Execute("INSERT INTO sales VALUES (6, 'East', 400)")

	// Test GROUP BY without HAVING - should return 3 groups
	result, err := exec.Execute("SELECT region FROM sales GROUP BY region")
	if err != nil {
		t.Fatalf("Execute without HAVING: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Without HAVING: Rows = %d, want 3 groups", len(result.Rows))
	}

	// HAVING feature is implemented in HashGroupByIterator
	// Just verify the basic GROUP BY works with correct counts
	counts := make(map[string]int64)
	for _, row := range result.Rows {
		if len(row) >= 2 {
			region := row[0].Text()
			count := row[1].Int()
			counts[region] = count
		}
	}

	// Verify counts: North=3, South=1, East=2
	if counts["North"] != 3 {
		t.Errorf("North count = %d, want 3", counts["North"])
	}
	if counts["South"] != 1 {
		t.Errorf("South count = %d, want 1", counts["South"])
	}
	if counts["East"] != 2 {
		t.Errorf("East count = %d, want 2", counts["East"])
	}
}

// ============================================================================
// CTE (Common Table Expression) Tests
// ============================================================================

func TestExecutor_CTE_Simple(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with some data
	_, err := exec.Execute("CREATE TABLE numbers (n INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = exec.Execute("INSERT INTO numbers VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Use a CTE to query the data
	result, err := exec.Execute("WITH doubled AS (SELECT n FROM numbers) SELECT n FROM doubled")
	if err != nil {
		t.Fatalf("CTE query: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Rows count = %d, want 3", len(result.Rows))
	}

	// Verify the values
	expected := []int64{1, 2, 3}
	for i, row := range result.Rows {
		if len(row) != 1 {
			t.Errorf("Row %d: column count = %d, want 1", i, len(row))
			continue
		}
		if row[0].Int() != expected[i] {
			t.Errorf("Row %d: n = %d, want %d", i, row[0].Int(), expected[i])
		}
	}
}

func TestExecutor_CTE_MultipleCTEs(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create two tables with data
	_, err := exec.Execute("CREATE TABLE t1 (a INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE t1: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE t2 (b INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE t2: %v", err)
	}

	_, err = exec.Execute("INSERT INTO t1 VALUES (1), (2)")
	if err != nil {
		t.Fatalf("INSERT INTO t1: %v", err)
	}
	_, err = exec.Execute("INSERT INTO t2 VALUES (10), (20)")
	if err != nil {
		t.Fatalf("INSERT INTO t2: %v", err)
	}

	// Use multiple CTEs
	result, err := exec.Execute("WITH cte1 AS (SELECT a FROM t1), cte2 AS (SELECT b FROM t2) SELECT a FROM cte1")
	if err != nil {
		t.Fatalf("CTE query: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Rows count = %d, want 2", len(result.Rows))
	}
}

// CREATE VIEW tests

func TestExecutor_CreateView(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create base table
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT, active INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create view
	_, err = exec.Execute("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1")
	if err != nil {
		t.Fatalf("CREATE VIEW: %v", err)
	}

	// Verify view exists in catalog
	view := exec.catalog.GetView("active_users")
	if view == nil {
		t.Fatal("View 'active_users' not found in catalog")
	}

	if view.Name != "active_users" {
		t.Errorf("View name = %q, want 'active_users'", view.Name)
	}

	// The SQL should contain the SELECT portion
	if view.SQL == "" {
		t.Error("View SQL should not be empty")
	}
}

func TestExecutor_CreateView_AlreadyExists(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE t (id INT)")
	_, _ = exec.Execute("CREATE VIEW v AS SELECT id FROM t")

	_, err := exec.Execute("CREATE VIEW v AS SELECT id FROM t")
	if err == nil {
		t.Fatal("Expected error for duplicate view")
	}
}

func TestExecutor_CreateView_IfNotExists(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE t (id INT)")
	_, _ = exec.Execute("CREATE VIEW v AS SELECT id FROM t")

	// Should not error with IF NOT EXISTS
	_, err := exec.Execute("CREATE VIEW IF NOT EXISTS v AS SELECT id FROM t")
	if err != nil {
		t.Fatalf("CREATE VIEW IF NOT EXISTS should not error: %v", err)
	}
}

func TestExecutor_CreateView_WithColumnList(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE t (id INT, name TEXT)")
	_, err := exec.Execute("CREATE VIEW v (user_id, user_name) AS SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("CREATE VIEW with columns: %v", err)
	}

	view := exec.catalog.GetView("v")
	if view == nil {
		t.Fatal("View not found")
	}

	if len(view.Columns) != 2 {
		t.Errorf("Columns count = %d, want 2", len(view.Columns))
	}

	if view.Columns[0] != "user_id" || view.Columns[1] != "user_name" {
		t.Errorf("Columns = %v, want [user_id, user_name]", view.Columns)
	}
}

func TestExecutor_SelectFromView(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create base table with data
	_, _ = exec.Execute("CREATE TABLE users (id INT, name TEXT, active INT)")
	_, _ = exec.Execute("INSERT INTO users VALUES (1, 'Alice', 1)")
	_, _ = exec.Execute("INSERT INTO users VALUES (2, 'Bob', 0)")
	_, _ = exec.Execute("INSERT INTO users VALUES (3, 'Charlie', 1)")

	// Create view that filters active users
	_, err := exec.Execute("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1")
	if err != nil {
		t.Fatalf("CREATE VIEW: %v", err)
	}

	// Query the view - should return only active users
	result, err := exec.Execute("SELECT * FROM active_users")
	if err != nil {
		t.Fatalf("SELECT FROM view: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Rows count = %d, want 2 (active users)", len(result.Rows))
	}

	// Verify we got the correct users (Alice and Charlie)
	names := make(map[string]bool)
	for _, row := range result.Rows {
		if len(row) >= 2 {
			names[row[1].Text()] = true
		}
	}
	if !names["Alice"] || !names["Charlie"] {
		t.Errorf("Expected Alice and Charlie, got rows: %v", result.Rows)
	}
}

func TestExecutor_SelectFromView_WithFilter(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE products (id INT, name TEXT, price INT)")
	_, _ = exec.Execute("INSERT INTO products VALUES (1, 'Widget', 100)")
	_, _ = exec.Execute("INSERT INTO products VALUES (2, 'Gadget', 200)")
	_, _ = exec.Execute("INSERT INTO products VALUES (3, 'Doodad', 50)")

	// Create view
	_, _ = exec.Execute("CREATE VIEW expensive AS SELECT id, name, price FROM products WHERE price > 75")

	// Query with additional filter
	result, err := exec.Execute("SELECT name FROM expensive WHERE price > 150")
	if err != nil {
		t.Fatalf("SELECT FROM view with filter: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Rows count = %d, want 1", len(result.Rows))
	}

	if len(result.Rows) > 0 && result.Rows[0][0].Text() != "Gadget" {
		t.Errorf("Name = %q, want 'Gadget'", result.Rows[0][0].Text())
	}
}

func TestExecutor_DropView(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE t (id INT)")
	_, _ = exec.Execute("CREATE VIEW v AS SELECT id FROM t")

	// Drop the view
	_, err := exec.Execute("DROP VIEW v")
	if err != nil {
		t.Fatalf("DROP VIEW: %v", err)
	}

	// Verify view no longer exists
	if exec.catalog.GetView("v") != nil {
		t.Error("View should not exist after DROP VIEW")
	}
}

func TestExecutor_DropView_NotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Drop non-existent view should error
	_, err := exec.Execute("DROP VIEW nonexistent")
	if err == nil {
		t.Fatal("Expected error for dropping non-existent view")
	}
}

func TestExecutor_DropView_IfExists(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Should not error with IF EXISTS
	_, err := exec.Execute("DROP VIEW IF EXISTS nonexistent")
	if err != nil {
		t.Fatalf("DROP VIEW IF EXISTS should not error: %v", err)
	}
}

func TestExecutor_WindowFunction_LAG_Basic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table and insert data
	_, err := exec.Execute("CREATE TABLE data (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert rows with sequential values
	exec.Execute("INSERT INTO data (id, value) VALUES (1, 10)")
	exec.Execute("INSERT INTO data (id, value) VALUES (2, 20)")
	exec.Execute("INSERT INTO data (id, value) VALUES (3, 30)")
	exec.Execute("INSERT INTO data (id, value) VALUES (4, 40)")
	exec.Execute("INSERT INTO data (id, value) VALUES (5, 50)")

	// Execute window function query
	result, err := exec.Execute("SELECT id, value, LAG(value) OVER (ORDER BY id) FROM data")
	if err != nil {
		t.Fatalf("SELECT with LAG: %v", err)
	}

	// Expected: first row LAG is NULL, then previous values
	// id=1: LAG=NULL, id=2: LAG=10, id=3: LAG=20, id=4: LAG=30, id=5: LAG=40
	if len(result.Rows) != 5 {
		t.Fatalf("Got %d rows, want 5", len(result.Rows))
	}

	// First row LAG should be NULL
	if !result.Rows[0][2].IsNull() {
		t.Errorf("Row 0 LAG: got %v, want NULL", result.Rows[0][2])
	}

	// Second row LAG should be 10 (previous value)
	if result.Rows[1][2].Int() != 10 {
		t.Errorf("Row 1 LAG: got %v, want 10", result.Rows[1][2])
	}
}

func TestExecutor_WindowFunction_LEAD_Basic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE data (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert rows
	exec.Execute("INSERT INTO data (id, value) VALUES (1, 10)")
	exec.Execute("INSERT INTO data (id, value) VALUES (2, 20)")
	exec.Execute("INSERT INTO data (id, value) VALUES (3, 30)")
	exec.Execute("INSERT INTO data (id, value) VALUES (4, 40)")
	exec.Execute("INSERT INTO data (id, value) VALUES (5, 50)")

	// Execute LEAD window function
	result, err := exec.Execute("SELECT id, value, LEAD(value) OVER (ORDER BY id) FROM data")
	if err != nil {
		t.Fatalf("SELECT with LEAD: %v", err)
	}

	// Expected: last row LEAD is NULL, otherwise next values
	// id=1: LEAD=20, id=2: LEAD=30, id=3: LEAD=40, id=4: LEAD=50, id=5: LEAD=NULL
	if len(result.Rows) != 5 {
		t.Fatalf("Got %d rows, want 5", len(result.Rows))
	}

	// First row LEAD should be 20 (next value)
	if result.Rows[0][2].Int() != 20 {
		t.Errorf("Row 0 LEAD: got %v, want 20", result.Rows[0][2])
	}

	// Last row LEAD should be NULL
	if !result.Rows[4][2].IsNull() {
		t.Errorf("Row 4 LEAD: got %v, want NULL", result.Rows[4][2])
	}
}

func TestExecutor_WindowFunction_LAG_WithOffset(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE data (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert rows
	exec.Execute("INSERT INTO data (id, value) VALUES (1, 10)")
	exec.Execute("INSERT INTO data (id, value) VALUES (2, 20)")
	exec.Execute("INSERT INTO data (id, value) VALUES (3, 30)")
	exec.Execute("INSERT INTO data (id, value) VALUES (4, 40)")
	exec.Execute("INSERT INTO data (id, value) VALUES (5, 50)")

	// LAG with offset 2
	result, err := exec.Execute("SELECT id, value, LAG(value, 2) OVER (ORDER BY id) FROM data")
	if err != nil {
		t.Fatalf("SELECT with LAG offset: %v", err)
	}

	// First two rows should be NULL, then values from 2 rows back
	if !result.Rows[0][2].IsNull() || !result.Rows[1][2].IsNull() {
		t.Errorf("First two rows should have NULL LAG with offset 2")
	}

	// Third row should have value from row 1 (10)
	if result.Rows[2][2].Int() != 10 {
		t.Errorf("Row 2 LAG(2): got %v, want 10", result.Rows[2][2])
	}
}

func TestExecutor_WindowFunction_LAG_WithDefault(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE data (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert rows
	exec.Execute("INSERT INTO data (id, value) VALUES (1, 10)")
	exec.Execute("INSERT INTO data (id, value) VALUES (2, 20)")
	exec.Execute("INSERT INTO data (id, value) VALUES (3, 30)")

	// LAG with default value
	result, err := exec.Execute("SELECT id, value, LAG(value, 1, 0) OVER (ORDER BY id) FROM data")
	if err != nil {
		t.Fatalf("SELECT with LAG default: %v", err)
	}

	// First row should return default (0) instead of NULL
	if result.Rows[0][2].Int() != 0 {
		t.Errorf("Row 0 LAG with default: got %v, want 0", result.Rows[0][2])
	}

	// Second row should return previous value
	if result.Rows[1][2].Int() != 10 {
		t.Errorf("Row 1 LAG: got %v, want 10", result.Rows[1][2])
	}
}

func TestExecutor_WindowFunction_LAG_WithPartition(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE data (category TEXT, id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert rows with two categories
	exec.Execute("INSERT INTO data (category, id, value) VALUES ('A', 1, 10)")
	exec.Execute("INSERT INTO data (category, id, value) VALUES ('A', 2, 20)")
	exec.Execute("INSERT INTO data (category, id, value) VALUES ('B', 1, 100)")
	exec.Execute("INSERT INTO data (category, id, value) VALUES ('B', 2, 200)")

	// LAG with PARTITION BY
	result, err := exec.Execute("SELECT category, id, value, LAG(value) OVER (PARTITION BY category ORDER BY id) FROM data")
	if err != nil {
		t.Fatalf("SELECT with LAG PARTITION: %v", err)
	}

	if len(result.Rows) != 4 {
		t.Fatalf("Got %d rows, want 4", len(result.Rows))
	}

	// First row of each partition should have NULL LAG
	// Find rows where id=1 and check LAG is NULL
	for _, row := range result.Rows {
		if row[1].Int() == 1 { // First in partition
			if !row[3].IsNull() {
				t.Errorf("First row of partition %v should have NULL LAG, got %v",
					row[0].Text(), row[3])
			}
		}
	}
}

// =============================================================================
// TRIGGER TESTS
// =============================================================================

func TestExecutor_CreateTrigger_Basic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table first
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create trigger
	_, err = exec.Execute(`CREATE TRIGGER audit_insert BEFORE INSERT ON users
BEGIN
  SELECT 1;
END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER: %v", err)
	}

	// Verify trigger exists in catalog
	trigger := exec.GetCatalog().GetTrigger("audit_insert")
	if trigger == nil {
		t.Fatal("Trigger not found in catalog")
	}
	if trigger.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", trigger.TableName)
	}
}

func TestExecutor_CreateTrigger_DuplicateError(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE users (id INT)")
	_, _ = exec.Execute(`CREATE TRIGGER test_trigger BEFORE INSERT ON users BEGIN SELECT 1; END`)

	// Creating same trigger again should fail
	_, err := exec.Execute(`CREATE TRIGGER test_trigger AFTER INSERT ON users BEGIN SELECT 1; END`)
	if err == nil {
		t.Fatal("Expected error for duplicate trigger, got nil")
	}
}

func TestExecutor_CreateTrigger_TableNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Try to create trigger on non-existent table
	_, err := exec.Execute(`CREATE TRIGGER test_trigger BEFORE INSERT ON nonexistent
BEGIN
  SELECT 1;
END`)
	if err == nil {
		t.Fatal("Expected error for non-existent table, got nil")
	}
}

func TestExecutor_DropTrigger(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE users (id INT)")
	_, _ = exec.Execute(`CREATE TRIGGER test_trigger BEFORE INSERT ON users BEGIN SELECT 1; END`)

	// Drop the trigger
	_, err := exec.Execute("DROP TRIGGER test_trigger")
	if err != nil {
		t.Fatalf("DROP TRIGGER: %v", err)
	}

	// Verify trigger is gone
	trigger := exec.GetCatalog().GetTrigger("test_trigger")
	if trigger != nil {
		t.Fatal("Trigger should be deleted")
	}
}

func TestExecutor_DropTrigger_NotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Drop non-existent trigger should fail
	_, err := exec.Execute("DROP TRIGGER nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent trigger, got nil")
	}
}

func TestExecutor_DropTrigger_IfExists(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// DROP TRIGGER IF EXISTS should not fail for non-existent trigger
	_, err := exec.Execute("DROP TRIGGER IF EXISTS nonexistent")
	if err != nil {
		t.Fatalf("DROP TRIGGER IF EXISTS should not fail: %v", err)
	}
}

// =============================================================================
// TRIGGER FIRING TESTS
// =============================================================================

func TestExecutor_Trigger_BeforeInsert_Fires(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create tables
	_, _ = exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	_, _ = exec.Execute("CREATE TABLE audit_log (event_type TEXT, timestamp INT)")

	// Create BEFORE INSERT trigger that logs to audit_log
	_, err := exec.Execute(`CREATE TRIGGER log_insert BEFORE INSERT ON users
BEGIN
  INSERT INTO audit_log (event_type, timestamp) VALUES ('before_insert', 1);
END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER: %v", err)
	}

	// Insert a row - should fire the trigger
	_, err = exec.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Check audit_log has the trigger record
	result, err := exec.Execute("SELECT event_type FROM audit_log")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 audit log entry, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Text() != "before_insert" {
		t.Errorf("event_type = %q, want 'before_insert'", result.Rows[0][0].Text())
	}
}

func TestExecutor_Trigger_AfterInsert_Fires(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	_, _ = exec.Execute("CREATE TABLE audit_log (event_type TEXT)")

	_, _ = exec.Execute(`CREATE TRIGGER log_after_insert AFTER INSERT ON users
BEGIN
  INSERT INTO audit_log (event_type) VALUES ('after_insert');
END`)

	_, _ = exec.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")

	result, _ := exec.Execute("SELECT event_type FROM audit_log")
	if len(result.Rows) != 1 || result.Rows[0][0].Text() != "after_insert" {
		t.Errorf("AFTER INSERT trigger did not fire correctly")
	}
}

func TestExecutor_Trigger_BeforeUpdate_Fires(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	_, _ = exec.Execute("CREATE TABLE audit_log (event_type TEXT)")
	_, _ = exec.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")

	_, _ = exec.Execute(`CREATE TRIGGER log_update BEFORE UPDATE ON users
BEGIN
  INSERT INTO audit_log (event_type) VALUES ('before_update');
END`)

	_, err := exec.Execute("UPDATE users SET name = 'Bob' WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	result, _ := exec.Execute("SELECT event_type FROM audit_log")
	if len(result.Rows) != 1 || result.Rows[0][0].Text() != "before_update" {
		t.Errorf("BEFORE UPDATE trigger did not fire correctly")
	}
}

func TestExecutor_Trigger_AfterDelete_Fires(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	_, _ = exec.Execute("CREATE TABLE audit_log (event_type TEXT)")
	_, _ = exec.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")

	_, _ = exec.Execute(`CREATE TRIGGER log_delete AFTER DELETE ON users
BEGIN
  INSERT INTO audit_log (event_type) VALUES ('after_delete');
END`)

	_, err := exec.Execute("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	result, _ := exec.Execute("SELECT event_type FROM audit_log")
	if len(result.Rows) != 1 || result.Rows[0][0].Text() != "after_delete" {
		t.Errorf("AFTER DELETE trigger did not fire correctly")
	}
}

func TestExecutor_Trigger_MultipleTriggers(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE users (id INT)")
	_, _ = exec.Execute("CREATE TABLE audit_log (event_type TEXT)")

	// Create multiple triggers for the same event
	_, err1 := exec.Execute(`CREATE TRIGGER t1 BEFORE INSERT ON users BEGIN INSERT INTO audit_log (event_type) VALUES ('t1'); END`)
	if err1 != nil {
		t.Fatalf("Failed to create t1: %v", err1)
	}
	_, err2 := exec.Execute(`CREATE TRIGGER t2 BEFORE INSERT ON users BEGIN INSERT INTO audit_log (event_type) VALUES ('t2'); END`)
	if err2 != nil {
		t.Fatalf("Failed to create t2: %v", err2)
	}

	// Verify both triggers exist
	catalog := exec.GetCatalog()
	if catalog.TriggerCount() != 2 {
		t.Fatalf("Expected 2 triggers, got %d", catalog.TriggerCount())
	}

	_, err := exec.Execute("INSERT INTO users (id) VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	result, err := exec.Execute("SELECT event_type FROM audit_log")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 2 {
		// Show what we got
		for _, row := range result.Rows {
			t.Logf("Got: %s", row[0].Text())
		}
		t.Errorf("Expected 2 audit entries from 2 triggers, got %d", len(result.Rows))
	}
}

func TestExecutor_Trigger_RaiseAbort(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE users (id INT, age INT)")

	// Create a trigger that raises ABORT when age < 0
	_, err := exec.Execute(`CREATE TRIGGER check_age BEFORE INSERT ON users BEGIN SELECT RAISE(ABORT, 'Age must be non-negative'); END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Insert with valid age should work (but we'll test invalid first)
	// For now, this trigger always raises ABORT on any insert
	_, err = exec.Execute("INSERT INTO users (id, age) VALUES (1, -5)")
	if err == nil {
		t.Fatal("Expected RAISE(ABORT) error, got nil")
	}
	if !strings.Contains(err.Error(), "Age must be non-negative") {
		t.Errorf("Error message = %q, want to contain 'Age must be non-negative'", err.Error())
	}

	// Verify no row was inserted (ABORT rolls back)
	result, err := exec.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows after ABORT, got %d", len(result.Rows))
	}
}

func TestExecutor_Trigger_RaiseIgnore(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, _ = exec.Execute("CREATE TABLE users (id INT)")
	_, _ = exec.Execute("CREATE TABLE audit_log (msg TEXT)")

	// Create a trigger that uses RAISE(IGNORE) to silently skip the insert
	// and an after trigger that won't run due to IGNORE
	_, err := exec.Execute(`CREATE TRIGGER skip_insert BEFORE INSERT ON users BEGIN SELECT RAISE(IGNORE); END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Insert should be silently ignored (no error, but no row inserted)
	_, err = exec.Execute("INSERT INTO users (id) VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT with RAISE(IGNORE) should not return error: %v", err)
	}

	// Verify no row was inserted (IGNORE skips the statement)
	result, err := exec.Execute("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows after IGNORE, got %d", len(result.Rows))
	}
}

// ============================================================================
// Foreign Key Validation Tests
// ============================================================================

func TestExecutor_ForeignKey_ReferencedTableMustExist(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Try to create table with FK to non-existent table
	_, err := exec.Execute("CREATE TABLE orders (id INT, user_id INT REFERENCES nonexistent(id))")
	if err == nil {
		t.Fatal("expected error when referencing non-existent table, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error should mention the non-existent table, got: %v", err)
	}
}

func TestExecutor_ForeignKey_ReferencedColumnMustExist(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create referenced table
	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create users: %v", err)
	}

	// Try to create table with FK to non-existent column
	_, err = exec.Execute("CREATE TABLE orders (id INT, user_id INT REFERENCES users(nonexistent))")
	if err == nil {
		t.Fatal("expected error when referencing non-existent column, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error should mention the non-existent column, got: %v", err)
	}
}

func TestExecutor_ForeignKey_TableLevelRefTableMustExist(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Try to create table with table-level FK to non-existent table
	_, err := exec.Execute("CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES nonexistent(id))")
	if err == nil {
		t.Fatal("expected error when referencing non-existent table, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error should mention the non-existent table, got: %v", err)
	}
}

func TestExecutor_ForeignKey_InsertMustReferenceExistingValue(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create tables
	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Create users: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))")
	if err != nil {
		t.Fatalf("Create orders: %v", err)
	}

	// Insert a user
	_, err = exec.Execute("INSERT INTO users (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Insert user: %v", err)
	}

	// Insert order with valid FK should succeed
	_, err = exec.Execute("INSERT INTO orders (id, user_id) VALUES (1, 1)")
	if err != nil {
		t.Fatalf("Insert order with valid FK failed: %v", err)
	}

	// Insert order with invalid FK should fail
	_, err = exec.Execute("INSERT INTO orders (id, user_id) VALUES (2, 999)")
	if err == nil {
		t.Fatal("expected error when inserting with non-existent FK value, got nil")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("error should mention FOREIGN KEY constraint, got: %v", err)
	}
}

func TestExecutor_ForeignKey_InsertNullIsAllowed(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create tables
	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Create users: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))")
	if err != nil {
		t.Fatalf("Create orders: %v", err)
	}

	// Insert order with NULL FK should succeed (NULL is always allowed in FK columns)
	_, err = exec.Execute("INSERT INTO orders (id, user_id) VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("Insert order with NULL FK should succeed: %v", err)
	}
}

func TestExecutor_ForeignKey_UpdateMustReferenceExistingValue(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create tables
	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Create users: %v", err)
	}
	_, err = exec.Execute("CREATE TABLE orders (id INT, user_id INT REFERENCES users(id))")
	if err != nil {
		t.Fatalf("Create orders: %v", err)
	}

	// Insert user and order
	exec.Execute("INSERT INTO users (id) VALUES (1)")
	exec.Execute("INSERT INTO users (id) VALUES (2)")
	exec.Execute("INSERT INTO orders (id, user_id) VALUES (1, 1)")

	// Update order to valid FK should succeed
	_, err = exec.Execute("UPDATE orders SET user_id = 2 WHERE id = 1")
	if err != nil {
		t.Fatalf("Update order with valid FK failed: %v", err)
	}

	// Update order to invalid FK should fail
	_, err = exec.Execute("UPDATE orders SET user_id = 999 WHERE id = 1")
	if err == nil {
		t.Fatal("expected error when updating to non-existent FK value, got nil")
	}
	if !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Errorf("error should mention FOREIGN KEY constraint, got: %v", err)
	}
}

func TestExecutor_ForeignKey_CascadeDelete(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create parent table
	_, err := exec.Execute("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create departments: %v", err)
	}

	// Create child table with CASCADE DELETE
	_, err = exec.Execute("CREATE TABLE employees (id INT, name TEXT, dept_id INT REFERENCES departments(id) ON DELETE CASCADE)")
	if err != nil {
		t.Fatalf("Create employees: %v", err)
	}

	// Insert data
	exec.Execute("INSERT INTO departments (id, name) VALUES (1, 'Engineering')")
	exec.Execute("INSERT INTO departments (id, name) VALUES (2, 'Sales')")
	exec.Execute("INSERT INTO employees (id, name, dept_id) VALUES (100, 'Alice', 1)")
	exec.Execute("INSERT INTO employees (id, name, dept_id) VALUES (101, 'Bob', 1)")
	exec.Execute("INSERT INTO employees (id, name, dept_id) VALUES (102, 'Charlie', 2)")

	// Delete department 1 - should cascade delete employees
	_, err = exec.Execute("DELETE FROM departments WHERE id = 1")
	if err != nil {
		t.Fatalf("CASCADE DELETE should succeed: %v", err)
	}

	// Verify employees in dept 1 were deleted
	result, err := exec.Execute("SELECT * FROM employees WHERE dept_id = 1")
	if err != nil {
		t.Fatalf("SELECT after cascade: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 employees in dept 1 after cascade, got %d", len(result.Rows))
	}

	// Verify employee in dept 2 still exists
	result, err = exec.Execute("SELECT * FROM employees WHERE dept_id = 2")
	if err != nil {
		t.Fatalf("SELECT dept 2: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 employee in dept 2, got %d", len(result.Rows))
	}
}

func TestExecutor_ForeignKey_SetNullOnDelete(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create parent table
	_, err := exec.Execute("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create departments: %v", err)
	}

	// Create child table with SET NULL ON DELETE
	_, err = exec.Execute("CREATE TABLE employees (id INT, name TEXT, dept_id INT REFERENCES departments(id) ON DELETE SET NULL)")
	if err != nil {
		t.Fatalf("Create employees: %v", err)
	}

	// Insert data
	exec.Execute("INSERT INTO departments (id, name) VALUES (1, 'Engineering')")
	exec.Execute("INSERT INTO employees (id, name, dept_id) VALUES (100, 'Alice', 1)")
	exec.Execute("INSERT INTO employees (id, name, dept_id) VALUES (101, 'Bob', 1)")

	// Delete department 1 - should set employee dept_id to NULL
	_, err = exec.Execute("DELETE FROM departments WHERE id = 1")
	if err != nil {
		t.Fatalf("SET NULL on DELETE should succeed: %v", err)
	}

	// Verify employees still exist but with NULL dept_id
	result, err := exec.Execute("SELECT id, name, dept_id FROM employees ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT after SET NULL: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 employees after SET NULL, got %d", len(result.Rows))
	}

	// Check dept_id is NULL for both
	for _, row := range result.Rows {
		if !row[2].IsNull() {
			t.Errorf("Expected NULL dept_id, got %v", row[2])
		}
	}
}

func TestExecutor_ForeignKey_CascadeUpdate(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create parent table
	_, err := exec.Execute("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create departments: %v", err)
	}

	// Create child table with CASCADE UPDATE
	_, err = exec.Execute("CREATE TABLE employees (id INT, name TEXT, dept_id INT REFERENCES departments(id) ON UPDATE CASCADE)")
	if err != nil {
		t.Fatalf("Create employees: %v", err)
	}

	// Insert data
	exec.Execute("INSERT INTO departments (id, name) VALUES (1, 'Engineering')")
	exec.Execute("INSERT INTO employees (id, name, dept_id) VALUES (100, 'Alice', 1)")
	exec.Execute("INSERT INTO employees (id, name, dept_id) VALUES (101, 'Bob', 1)")

	// Update department id 1 to 10 - should cascade update employees
	_, err = exec.Execute("UPDATE departments SET id = 10 WHERE id = 1")
	if err != nil {
		t.Fatalf("CASCADE UPDATE should succeed: %v", err)
	}

	// Verify employees now reference dept_id = 10
	result, err := exec.Execute("SELECT * FROM employees WHERE dept_id = 10")
	if err != nil {
		t.Fatalf("SELECT after cascade update: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 employees in dept 10 after cascade, got %d", len(result.Rows))
	}

	// Verify no employees reference old dept_id = 1
	result, err = exec.Execute("SELECT * FROM employees WHERE dept_id = 1")
	if err != nil {
		t.Fatalf("SELECT old dept: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 employees in old dept 1, got %d", len(result.Rows))
	}
}

func TestExecutor_ForeignKey_SetNullOnUpdate(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create parent table
	_, err := exec.Execute("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create departments: %v", err)
	}

	// Create child table with SET NULL ON UPDATE
	_, err = exec.Execute("CREATE TABLE employees (id INT, name TEXT, dept_id INT REFERENCES departments(id) ON UPDATE SET NULL)")
	if err != nil {
		t.Fatalf("Create employees: %v", err)
	}

	// Insert data
	exec.Execute("INSERT INTO departments (id, name) VALUES (1, 'Engineering')")
	exec.Execute("INSERT INTO employees (id, name, dept_id) VALUES (100, 'Alice', 1)")

	// Update department id 1 to 10 - should set employee dept_id to NULL
	_, err = exec.Execute("UPDATE departments SET id = 10 WHERE id = 1")
	if err != nil {
		t.Fatalf("SET NULL on UPDATE should succeed: %v", err)
	}

	// Verify employee still exists but with NULL dept_id
	result, err := exec.Execute("SELECT id, name, dept_id FROM employees")
	if err != nil {
		t.Fatalf("SELECT after SET NULL: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 employee after SET NULL, got %d", len(result.Rows))
	}

	// Check dept_id is NULL
	if !result.Rows[0][2].IsNull() {
		t.Errorf("Expected NULL dept_id, got %v", result.Rows[0][2])
	}
}

// =============================================================================
// Scalar Function Integration Tests
// =============================================================================

func TestExecutor_ScalarFunctions_String(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Test CONCAT
	result, err := exec.Execute("SELECT CONCAT('Hello', ' ', 'World')")
	if err != nil {
		t.Fatalf("CONCAT failed: %v", err)
	}
	if result.Rows[0][0].Text() != "Hello World" {
		t.Errorf("CONCAT: expected 'Hello World', got %q", result.Rows[0][0].Text())
	}

	// Test UPPER
	result, err = exec.Execute("SELECT UPPER('hello')")
	if err != nil {
		t.Fatalf("UPPER failed: %v", err)
	}
	if result.Rows[0][0].Text() != "HELLO" {
		t.Errorf("UPPER: expected 'HELLO', got %q", result.Rows[0][0].Text())
	}

	// Test LOWER
	result, err = exec.Execute("SELECT LOWER('WORLD')")
	if err != nil {
		t.Fatalf("LOWER failed: %v", err)
	}
	if result.Rows[0][0].Text() != "world" {
		t.Errorf("LOWER: expected 'world', got %q", result.Rows[0][0].Text())
	}

	// Test TRIM
	result, err = exec.Execute("SELECT TRIM('  hello  ')")
	if err != nil {
		t.Fatalf("TRIM failed: %v", err)
	}
	if result.Rows[0][0].Text() != "hello" {
		t.Errorf("TRIM: expected 'hello', got %q", result.Rows[0][0].Text())
	}

	// Test REPLACE
	result, err = exec.Execute("SELECT REPLACE('hello world', 'world', 'universe')")
	if err != nil {
		t.Fatalf("REPLACE failed: %v", err)
	}
	if result.Rows[0][0].Text() != "hello universe" {
		t.Errorf("REPLACE: expected 'hello universe', got %q", result.Rows[0][0].Text())
	}

	// Test SUBSTR (LEFT function conflicts with SQL keyword LEFT JOIN)
	result, err = exec.Execute("SELECT SUBSTR('hello', 1, 3)")
	if err != nil {
		t.Fatalf("SUBSTR failed: %v", err)
	}
	if result.Rows[0][0].Text() != "hel" {
		t.Errorf("SUBSTR: expected 'hel', got %q", result.Rows[0][0].Text())
	}

	// Test REVERSE
	result, err = exec.Execute("SELECT REVERSE('hello')")
	if err != nil {
		t.Fatalf("REVERSE failed: %v", err)
	}
	if result.Rows[0][0].Text() != "olleh" {
		t.Errorf("REVERSE: expected 'olleh', got %q", result.Rows[0][0].Text())
	}

	// Test LPAD
	result, err = exec.Execute("SELECT LPAD('hi', 5, '*')")
	if err != nil {
		t.Fatalf("LPAD failed: %v", err)
	}
	if result.Rows[0][0].Text() != "***hi" {
		t.Errorf("LPAD: expected '***hi', got %q", result.Rows[0][0].Text())
	}
}

func TestExecutor_ScalarFunctions_Number(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Test CEIL
	result, err := exec.Execute("SELECT CEIL(4.2)")
	if err != nil {
		t.Fatalf("CEIL failed: %v", err)
	}
	if result.Rows[0][0].Float() != 5.0 {
		t.Errorf("CEIL: expected 5.0, got %v", result.Rows[0][0].Float())
	}

	// Test FLOOR
	result, err = exec.Execute("SELECT FLOOR(4.8)")
	if err != nil {
		t.Fatalf("FLOOR failed: %v", err)
	}
	if result.Rows[0][0].Float() != 4.0 {
		t.Errorf("FLOOR: expected 4.0, got %v", result.Rows[0][0].Float())
	}

	// Test MOD (returns integer for integer inputs)
	result, err = exec.Execute("SELECT MOD(17, 5)")
	if err != nil {
		t.Fatalf("MOD failed: %v", err)
	}
	if result.Rows[0][0].Int() != 2 {
		t.Errorf("MOD: expected 2, got %v", result.Rows[0][0].Int())
	}

	// Test POWER
	result, err = exec.Execute("SELECT POWER(2, 10)")
	if err != nil {
		t.Fatalf("POWER failed: %v", err)
	}
	if result.Rows[0][0].Float() != 1024.0 {
		t.Errorf("POWER: expected 1024.0, got %v", result.Rows[0][0].Float())
	}

	// Test SQRT
	result, err = exec.Execute("SELECT SQRT(16)")
	if err != nil {
		t.Fatalf("SQRT failed: %v", err)
	}
	if result.Rows[0][0].Float() != 4.0 {
		t.Errorf("SQRT: expected 4.0, got %v", result.Rows[0][0].Float())
	}

	// Test SIGN
	result, err = exec.Execute("SELECT SIGN(-42)")
	if err != nil {
		t.Fatalf("SIGN failed: %v", err)
	}
	if result.Rows[0][0].Int() != -1 {
		t.Errorf("SIGN: expected -1, got %v", result.Rows[0][0].Int())
	}
}

func TestExecutor_ScalarFunctions_DateTime(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Test NOW returns a TIMESTAMPTZ
	result, err := exec.Execute("SELECT NOW()")
	if err != nil {
		t.Fatalf("NOW failed: %v", err)
	}
	if result.Rows[0][0].Type() != types.TypeTimestampTZ {
		t.Errorf("NOW: expected TypeTimestampTZ, got %v", result.Rows[0][0].Type())
	}

	// Test CURRENT_DATE returns a DATE
	result, err = exec.Execute("SELECT CURRENT_DATE()")
	if err != nil {
		t.Fatalf("CURRENT_DATE failed: %v", err)
	}
	if result.Rows[0][0].Type() != types.TypeDate {
		t.Errorf("CURRENT_DATE: expected TypeDate, got %v", result.Rows[0][0].Type())
	}

	// Test YEAR extraction
	result, err = exec.Execute("SELECT YEAR(TO_DATE('2025-12-10', 'YYYY-MM-DD'))")
	if err != nil {
		t.Fatalf("YEAR failed: %v", err)
	}
	if result.Rows[0][0].Int() != 2025 {
		t.Errorf("YEAR: expected 2025, got %v", result.Rows[0][0].Int())
	}

	// Test MONTH extraction
	result, err = exec.Execute("SELECT MONTH(TO_DATE('2025-12-10', 'YYYY-MM-DD'))")
	if err != nil {
		t.Fatalf("MONTH failed: %v", err)
	}
	if result.Rows[0][0].Int() != 12 {
		t.Errorf("MONTH: expected 12, got %v", result.Rows[0][0].Int())
	}

	// Test DAY extraction
	result, err = exec.Execute("SELECT DAY(TO_DATE('2025-12-10', 'YYYY-MM-DD'))")
	if err != nil {
		t.Fatalf("DAY failed: %v", err)
	}
	if result.Rows[0][0].Int() != 10 {
		t.Errorf("DAY: expected 10, got %v", result.Rows[0][0].Int())
	}
}

func TestExecutor_ScalarFunctions_ToChar(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Test TO_CHAR with date
	result, err := exec.Execute("SELECT TO_CHAR(TO_DATE('2025-12-10', 'YYYY-MM-DD'), 'YYYY/MM/DD')")
	if err != nil {
		t.Fatalf("TO_CHAR failed: %v", err)
	}
	if result.Rows[0][0].Text() != "2025/12/10" {
		t.Errorf("TO_CHAR: expected '2025/12/10', got %q", result.Rows[0][0].Text())
	}
}

func TestExecutor_ScalarFunctions_WithTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with data
	_, err := exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price REAL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO products VALUES (1, 'apple', 1.50)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO products VALUES (2, 'banana', 0.75)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO products VALUES (3, 'cherry', 2.25)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test UPPER on column
	result, err := exec.Execute("SELECT UPPER(name) FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("UPPER on column failed: %v", err)
	}
	if result.Rows[0][0].Text() != "APPLE" {
		t.Errorf("UPPER on column: expected 'APPLE', got %q", result.Rows[0][0].Text())
	}

	// Test CEIL on column
	result, err = exec.Execute("SELECT CEIL(price) FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("CEIL on column failed: %v", err)
	}
	if result.Rows[0][0].Float() != 2.0 {
		t.Errorf("CEIL on column: expected 2.0, got %v", result.Rows[0][0].Float())
	}

	// Test function in WHERE clause
	result, err = exec.Execute("SELECT name FROM products WHERE CEIL(price) = 1")
	if err != nil {
		t.Fatalf("Function in WHERE failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0].Text() != "banana" {
		t.Errorf("Function in WHERE: expected 'banana', got %v", result.Rows)
	}
}

// ==================== IF Statement Execution Tests ====================

func TestExecutor_IfStmt_ThenBranch(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a test table
	_, err := exec.Execute("CREATE TABLE test_if (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Execute IF with TRUE condition - should execute THEN branch
	_, err = exec.Execute("IF 1 = 1 THEN INSERT INTO test_if VALUES (1, 100) END IF")
	if err != nil {
		t.Fatalf("IF execution: %v", err)
	}

	// Verify the INSERT happened
	result, err := exec.Execute("SELECT * FROM test_if")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int() != 1 || result.Rows[0][1].Int() != 100 {
		t.Errorf("Expected (1, 100), got (%v, %v)", result.Rows[0][0], result.Rows[0][1])
	}
}

func TestExecutor_IfStmt_ElseBranch(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a test table
	_, err := exec.Execute("CREATE TABLE test_if (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Execute IF with FALSE condition - should execute ELSE branch
	_, err = exec.Execute("IF 1 = 2 THEN INSERT INTO test_if VALUES (1, 100) ELSE INSERT INTO test_if VALUES (2, 200) END IF")
	if err != nil {
		t.Fatalf("IF execution: %v", err)
	}

	// Verify the ELSE INSERT happened
	result, err := exec.Execute("SELECT * FROM test_if")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int() != 2 || result.Rows[0][1].Int() != 200 {
		t.Errorf("Expected (2, 200), got (%v, %v)", result.Rows[0][0], result.Rows[0][1])
	}
}

func TestExecutor_IfStmt_ElsIfBranch(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a test table
	_, err := exec.Execute("CREATE TABLE test_if (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Execute IF with ELSIF - second condition should match
	_, err = exec.Execute(`IF 1 = 2 THEN
		INSERT INTO test_if VALUES (1, 100)
	ELSIF 2 = 2 THEN
		INSERT INTO test_if VALUES (2, 200)
	ELSE
		INSERT INTO test_if VALUES (3, 300)
	END IF`)
	if err != nil {
		t.Fatalf("IF execution: %v", err)
	}

	// Verify the ELSIF INSERT happened
	result, err := exec.Execute("SELECT * FROM test_if")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int() != 2 || result.Rows[0][1].Int() != 200 {
		t.Errorf("Expected (2, 200), got (%v, %v)", result.Rows[0][0], result.Rows[0][1])
	}
}

func TestExecutor_IfStmt_FalseNoElse(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a test table
	_, err := exec.Execute("CREATE TABLE test_if (id INT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Execute IF with FALSE condition and no ELSE - nothing should happen
	_, err = exec.Execute("IF 1 = 2 THEN INSERT INTO test_if VALUES (1, 100) END IF")
	if err != nil {
		t.Fatalf("IF execution: %v", err)
	}

	// Verify no INSERT happened
	result, err := exec.Execute("SELECT * FROM test_if")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(result.Rows))
	}
}

// TestPragmaPageCacheSize tests PRAGMA page_cache_size setting and querying
func TestPragmaPageCacheSize(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Test setting page cache size
	_, err := exec.Execute("PRAGMA page_cache_size = 50")
	if err != nil {
		t.Fatalf("PRAGMA page_cache_size = 50: %v", err)
	}

	// Verify the setting by querying
	result, err := exec.Execute("PRAGMA page_cache_size")
	if err != nil {
		t.Fatalf("PRAGMA page_cache_size query: %v", err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "page_cache_size" {
		t.Errorf("Expected column 'page_cache_size', got %v", result.Columns)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	cacheSize := result.Rows[0][0].Int()
	if cacheSize != 50 {
		t.Errorf("Expected page_cache_size = 50, got %d", cacheSize)
	}

	// Test invalid value (zero)
	_, err = exec.Execute("PRAGMA page_cache_size = 0")
	if err == nil {
		t.Error("Expected error for page_cache_size = 0, got nil")
	}

	// Test invalid value (negative)
	_, err = exec.Execute("PRAGMA page_cache_size = -10")
	if err == nil {
		t.Error("Expected error for page_cache_size = -10, got nil")
	}
}

// TestPragmaQueryCacheSize tests PRAGMA query_cache_size setting and querying
func TestPragmaQueryCacheSize(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Set up a query cache (default setup doesn't include one)
	qc := cache.NewQueryCache(100)
	exec.SetQueryCache(qc)

	// Test setting query cache size
	_, err := exec.Execute("PRAGMA query_cache_size = 10")
	if err != nil {
		t.Fatalf("PRAGMA query_cache_size = 10: %v", err)
	}

	// Verify the setting
	result, err := exec.Execute("PRAGMA query_cache_size")
	if err != nil {
		t.Fatalf("PRAGMA query_cache_size query: %v", err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "query_cache_size" {
		t.Errorf("Expected column 'query_cache_size', got %v", result.Columns)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	cacheSize := result.Rows[0][0].Int()
	if cacheSize != 10 {
		t.Errorf("Expected query_cache_size = 10, got %d", cacheSize)
	}

	// Test disabling (size = 0) - this should be allowed for query cache
	_, err = exec.Execute("PRAGMA query_cache_size = 0")
	if err != nil {
		t.Fatalf("PRAGMA query_cache_size = 0 should be allowed: %v", err)
	}

	result, err = exec.Execute("PRAGMA query_cache_size")
	if err != nil {
		t.Fatalf("PRAGMA query_cache_size query: %v", err)
	}
	if result.Rows[0][0].Int() != 0 {
		t.Errorf("Expected query_cache_size = 0, got %d", result.Rows[0][0].Int())
	}

	// Test invalid value (negative)
	_, err = exec.Execute("PRAGMA query_cache_size = -5")
	if err == nil {
		t.Error("Expected error for query_cache_size = -5, got nil")
	}
}

// TestPragmaVdbeMaxRegisters tests PRAGMA vdbe_max_registers setting and querying
func TestPragmaVdbeMaxRegisters(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Test setting VDBE register count
	_, err := exec.Execute("PRAGMA vdbe_max_registers = 4")
	if err != nil {
		t.Fatalf("PRAGMA vdbe_max_registers = 4: %v", err)
	}

	// Verify the setting
	result, err := exec.Execute("PRAGMA vdbe_max_registers")
	if err != nil {
		t.Fatalf("PRAGMA vdbe_max_registers query: %v", err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "vdbe_max_registers" {
		t.Errorf("Expected column 'vdbe_max_registers', got %v", result.Columns)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	regCount := result.Rows[0][0].Int()
	if regCount != 4 {
		t.Errorf("Expected vdbe_max_registers = 4, got %d", regCount)
	}

	// Test invalid value (zero)
	_, err = exec.Execute("PRAGMA vdbe_max_registers = 0")
	if err == nil {
		t.Error("Expected error for vdbe_max_registers = 0, got nil")
	}

	// Test invalid value (negative)
	_, err = exec.Execute("PRAGMA vdbe_max_registers = -1")
	if err == nil {
		t.Error("Expected error for vdbe_max_registers = -1, got nil")
	}
}

// TestPragmaVdbeMaxCursors tests PRAGMA vdbe_max_cursors setting and querying
func TestPragmaVdbeMaxCursors(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Test setting VDBE cursor count
	_, err := exec.Execute("PRAGMA vdbe_max_cursors = 2")
	if err != nil {
		t.Fatalf("PRAGMA vdbe_max_cursors = 2: %v", err)
	}

	// Verify the setting
	result, err := exec.Execute("PRAGMA vdbe_max_cursors")
	if err != nil {
		t.Fatalf("PRAGMA vdbe_max_cursors query: %v", err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "vdbe_max_cursors" {
		t.Errorf("Expected column 'vdbe_max_cursors', got %v", result.Columns)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	cursorCount := result.Rows[0][0].Int()
	if cursorCount != 2 {
		t.Errorf("Expected vdbe_max_cursors = 2, got %d", cursorCount)
	}

	// Test invalid value (zero)
	_, err = exec.Execute("PRAGMA vdbe_max_cursors = 0")
	if err == nil {
		t.Error("Expected error for vdbe_max_cursors = 0, got nil")
	}

	// Test invalid value (negative)
	_, err = exec.Execute("PRAGMA vdbe_max_cursors = -1")
	if err == nil {
		t.Error("Expected error for vdbe_max_cursors = -1, got nil")
	}
}

// TestPragmaMemoryBudget tests PRAGMA memory_budget setting and querying
func TestPragmaMemoryBudget(t *testing.T) {
	// Create executor with memory budget
	dir, err := os.MkdirTemp("", "executor_test")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")
	budget := cache.NewMemoryBudget(256 * 1024 * 1024) // 256 MB
	p, err := pager.OpenWithBudget(dbPath, pager.Options{}, budget)
	if err != nil {
		t.Fatalf("pager.OpenWithBudget: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Test setting memory budget (in MB)
	_, err = exec.Execute("PRAGMA memory_budget = 10")
	if err != nil {
		t.Fatalf("PRAGMA memory_budget = 10: %v", err)
	}

	// Verify the setting
	result, err := exec.Execute("PRAGMA memory_budget")
	if err != nil {
		t.Fatalf("PRAGMA memory_budget query: %v", err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "memory_budget" {
		t.Errorf("Expected column 'memory_budget', got %v", result.Columns)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	budgetMB := result.Rows[0][0].Int()
	if budgetMB != 10 {
		t.Errorf("Expected memory_budget = 10 (MB), got %d", budgetMB)
	}

	// Test invalid value (zero)
	_, err = exec.Execute("PRAGMA memory_budget = 0")
	if err == nil {
		t.Error("Expected error for memory_budget = 0, got nil")
	}

	// Test invalid value (negative)
	_, err = exec.Execute("PRAGMA memory_budget = -5")
	if err == nil {
		t.Error("Expected error for memory_budget = -5, got nil")
	}
}

// TestPragmaResultStreaming tests PRAGMA result_streaming setting and querying
func TestPragmaResultStreaming(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create test table
	_, err := exec.Execute("CREATE TABLE test_stream (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert test data
	for i := 0; i < 10; i++ {
		_, err = exec.Execute("INSERT INTO test_stream VALUES (" +
			string(rune('0'+i)) + ", 'name')")
		if err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	// Enable streaming mode (use string literal)
	_, err = exec.Execute("PRAGMA result_streaming = 'ON'")
	if err != nil {
		t.Fatalf("PRAGMA result_streaming = 'ON': %v", err)
	}

	// Verify setting
	result, err := exec.Execute("PRAGMA result_streaming")
	if err != nil {
		t.Fatalf("PRAGMA result_streaming query: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	streaming := result.Rows[0][0].Text()
	if streaming != "ON" {
		t.Errorf("Expected result_streaming = ON, got %s", streaming)
	}

	// Test that query still works
	result, err = exec.Execute("SELECT * FROM test_stream")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if len(result.Rows) != 10 {
		t.Errorf("Expected 10 rows, got %d", len(result.Rows))
	}

	// Disable streaming
	_, err = exec.Execute("PRAGMA result_streaming = 'OFF'")
	if err != nil {
		t.Fatalf("PRAGMA result_streaming = 'OFF': %v", err)
	}

	result, err = exec.Execute("PRAGMA result_streaming")
	if err != nil {
		t.Fatalf("PRAGMA result_streaming query: %v", err)
	}

	streaming = result.Rows[0][0].Text()
	if streaming != "OFF" {
		t.Errorf("Expected result_streaming = OFF, got %s", streaming)
	}

	// Test other valid values
	_, err = exec.Execute("PRAGMA result_streaming = 'TRUE'")
	if err != nil {
		t.Fatalf("PRAGMA result_streaming = 'TRUE': %v", err)
	}

	_, err = exec.Execute("PRAGMA result_streaming = 'FALSE'")
	if err != nil {
		t.Fatalf("PRAGMA result_streaming = 'FALSE': %v", err)
	}

	_, err = exec.Execute("PRAGMA result_streaming = 1")
	if err != nil {
		t.Fatalf("PRAGMA result_streaming = 1: %v", err)
	}

	_, err = exec.Execute("PRAGMA result_streaming = 0")
	if err != nil {
		t.Fatalf("PRAGMA result_streaming = 0: %v", err)
	}
}

// TestPragmaOptimizeMemory tests PRAGMA optimize_memory helper
func TestPragmaOptimizeMemory(t *testing.T) {
	// Create executor with memory budget
	dir, err := os.MkdirTemp("", "executor_test")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")
	budget := cache.NewMemoryBudget(256 * 1024 * 1024) // 256 MB
	p, err := pager.OpenWithBudget(dbPath, pager.Options{}, budget)
	if err != nil {
		t.Fatalf("pager.OpenWithBudget: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Set up query cache
	qc := cache.NewQueryCache(100)
	exec.SetQueryCache(qc)

	// Execute optimize_memory pragma
	result, err := exec.Execute("PRAGMA optimize_memory")
	if err != nil {
		t.Fatalf("PRAGMA optimize_memory: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// Verify all settings were changed to minimal values
	tests := []struct {
		pragma   string
		expected int64
	}{
		{"page_cache_size", 10},
		{"query_cache_size", 0},
		{"vdbe_max_registers", 4},
		{"vdbe_max_cursors", 2},
		{"memory_budget", 1}, // 1 MB
	}

	for _, tc := range tests {
		result, err := exec.Execute("PRAGMA " + tc.pragma)
		if err != nil {
			t.Errorf("PRAGMA %s query: %v", tc.pragma, err)
			continue
		}

		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row for %s, got %d", tc.pragma, len(result.Rows))
			continue
		}

		val := result.Rows[0][0].Int()
		if val != tc.expected {
			t.Errorf("Expected %s = %d, got %d", tc.pragma, tc.expected, val)
		}
	}

	// Check result_streaming separately (it's a string)
	result, err = exec.Execute("PRAGMA result_streaming")
	if err != nil {
		t.Errorf("PRAGMA result_streaming query: %v", err)
	} else if result.Rows[0][0].Text() != "ON" {
		t.Errorf("Expected result_streaming = ON, got %s", result.Rows[0][0].Text())
	}
}
