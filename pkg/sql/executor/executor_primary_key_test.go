package executor

import (
	"strings"
	"testing"
)

// Test Task 1: Create unique index on primary key column during CREATE TABLE

func TestPrimaryKey_AutoCreatesUniqueIndex_ColumnLevel(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with column-level PRIMARY KEY
	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Check that a unique index was automatically created for the primary key
	// The index should be named with a convention like "pk_<table>_<column>"
	pkIndex := exec.catalog.GetIndex("pk_users_id")
	if pkIndex == nil {
		// Try alternative naming convention
		pkIndex = exec.catalog.GetIndex("users_pk")
	}
	if pkIndex == nil {
		t.Fatal("Expected automatic unique index on PRIMARY KEY column, but none found")
	}

	if !pkIndex.Unique {
		t.Error("PRIMARY KEY index should be marked as UNIQUE")
	}

	if len(pkIndex.Columns) != 1 || pkIndex.Columns[0] != "id" {
		t.Errorf("Expected index on column 'id', got columns: %v", pkIndex.Columns)
	}
}

func TestPrimaryKey_AutoCreatesUniqueIndex_CompositeKey(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with table-level PRIMARY KEY (composite)
	_, err := exec.Execute("CREATE TABLE order_items (order_id INT, product_id INT, quantity INT, PRIMARY KEY (order_id, product_id))")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Check that a unique index was automatically created for the composite primary key
	pkIndex := exec.catalog.GetIndex("pk_order_items")
	if pkIndex == nil {
		pkIndex = exec.catalog.GetIndex("order_items_pk")
	}
	if pkIndex == nil {
		t.Fatal("Expected automatic unique index on composite PRIMARY KEY, but none found")
	}

	if !pkIndex.Unique {
		t.Error("Composite PRIMARY KEY index should be marked as UNIQUE")
	}

	if len(pkIndex.Columns) != 2 {
		t.Fatalf("Expected 2 columns in composite PK index, got %d", len(pkIndex.Columns))
	}
	if pkIndex.Columns[0] != "order_id" || pkIndex.Columns[1] != "product_id" {
		t.Errorf("Expected columns [order_id, product_id], got %v", pkIndex.Columns)
	}
}

// Test Task 2: Validate primary key uniqueness on insert

func TestPrimaryKey_RejectsDuplicateOnInsert(t *testing.T) {
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

	// Second insert with same primary key should fail
	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Bob')")
	if err == nil {
		t.Fatal("Expected error for duplicate PRIMARY KEY, but got none")
	}

	// Error message should mention primary key or unique constraint
	errMsg := strings.ToLower(err.Error())
	if !strings.Contains(errMsg, "primary key") && !strings.Contains(errMsg, "unique") && !strings.Contains(errMsg, "duplicate") {
		t.Errorf("Error message should mention primary key violation, got: %s", err.Error())
	}
}

func TestPrimaryKey_RejectsDuplicateCompositeKey(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE order_items (order_id INT, product_id INT, qty INT, PRIMARY KEY (order_id, product_id))")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// First insert should succeed
	_, err = exec.Execute("INSERT INTO order_items VALUES (1, 100, 5)")
	if err != nil {
		t.Fatalf("First INSERT: %v", err)
	}

	// Different composite key should succeed
	_, err = exec.Execute("INSERT INTO order_items VALUES (1, 101, 3)")
	if err != nil {
		t.Fatalf("Second INSERT (different key): %v", err)
	}

	_, err = exec.Execute("INSERT INTO order_items VALUES (2, 100, 2)")
	if err != nil {
		t.Fatalf("Third INSERT (different key): %v", err)
	}

	// Duplicate composite key should fail
	_, err = exec.Execute("INSERT INTO order_items VALUES (1, 100, 10)")
	if err == nil {
		t.Fatal("Expected error for duplicate composite PRIMARY KEY, but got none")
	}
}

// Test Task 3: AUTOINCREMENT for INTEGER PRIMARY KEY

func TestPrimaryKey_AutoIncrement_GeneratesRowID(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert without specifying id - should auto-generate
	_, err = exec.Execute("INSERT INTO users (name) VALUES ('Alice')")
	if err != nil {
		t.Fatalf("INSERT without id: %v", err)
	}

	// Second insert should get next id
	_, err = exec.Execute("INSERT INTO users (name) VALUES ('Bob')")
	if err != nil {
		t.Fatalf("Second INSERT without id: %v", err)
	}

	// Query to verify auto-generated IDs
	result, err := exec.Execute("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(result.Rows))
	}

	// First row should have id=1
	id1 := result.Rows[0][0].Int()
	if id1 != 1 {
		t.Errorf("First row should have id=1, got %d", id1)
	}

	// Second row should have id=2
	id2 := result.Rows[1][0].Int()
	if id2 != 2 {
		t.Errorf("Second row should have id=2, got %d", id2)
	}
}

func TestPrimaryKey_AutoIncrement_ContinuesAfterExplicitID(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert with explicit high ID
	_, err = exec.Execute("INSERT INTO users VALUES (100, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT with explicit id: %v", err)
	}

	// Next auto-generated ID should be 101
	_, err = exec.Execute("INSERT INTO users (name) VALUES ('Bob')")
	if err != nil {
		t.Fatalf("INSERT without id: %v", err)
	}

	result, err := exec.Execute("SELECT id, name FROM users WHERE name = 'Bob'")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	bobID := result.Rows[0][0].Int()
	if bobID != 101 {
		t.Errorf("Bob's ID should be 101 (after Alice's 100), got %d", bobID)
	}
}

func TestPrimaryKey_AutoIncrement_WithNULL(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert with NULL id - should auto-generate (SQLite behavior)
	_, err = exec.Execute("INSERT INTO users VALUES (NULL, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT with NULL id: %v", err)
	}

	result, err := exec.Execute("SELECT id FROM users")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// ID should not be NULL
	if result.Rows[0][0].IsNull() {
		t.Error("Auto-generated ID should not be NULL")
	}
}

// Test Task 4: Track max rowid in table metadata

func TestPrimaryKey_MaxRowID_TrackedAcrossInserts(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert with explicit high ID
	_, err = exec.Execute("INSERT INTO users VALUES (50, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}

	// Insert with lower explicit ID (should still track max as 50)
	_, err = exec.Execute("INSERT INTO users VALUES (25, 'Bob')")
	if err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}

	// Auto-generated ID should be 51, not 26
	_, err = exec.Execute("INSERT INTO users (name) VALUES ('Charlie')")
	if err != nil {
		t.Fatalf("INSERT 3: %v", err)
	}

	result, err := exec.Execute("SELECT id FROM users WHERE name = 'Charlie'")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected at least 1 row for Charlie")
	}

	charlieID := result.Rows[0][0].Int()
	if charlieID != 51 {
		t.Errorf("Charlie's ID should be 51 (max+1), got %d", charlieID)
	}
}

// Test PRIMARY KEY with NOT NULL behavior (PRIMARY KEY implies NOT NULL)

func TestPrimaryKey_ImpliesNotNull(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Note: For non-INTEGER PRIMARY KEY, NULL should be rejected
	_, err := exec.Execute("CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Inserting NULL for TEXT PRIMARY KEY should fail
	_, err = exec.Execute("INSERT INTO products VALUES (NULL, 'Widget')")
	if err == nil {
		t.Fatal("Expected error for NULL in TEXT PRIMARY KEY, but got none")
	}
}
