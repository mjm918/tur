package executor

import (
	"os"
	"path/filepath"
	"testing"

	"tur/pkg/pager"
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
	if row[0].Type() != types.TypeInt || row[0].Int() != 42 {
		t.Errorf("Row[0] = %v, want Int(42)", row[0])
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
