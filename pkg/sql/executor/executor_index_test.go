package executor

import (
	"encoding/binary"
	"testing"
	"tur/pkg/record"
	"tur/pkg/types"
)

func TestExecutor_Insert_UpdatesIndexes(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("CREATE INDEX idx_name ON users (name)")

	// Insert row
	_, err := exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Verify index entry exists
	// We need to inspect the index B-tree directly or via future plan to usage index
	// For now, we inspect e.trees["index:idx_name"]

	tree := exec.trees["index:idx_name"]
	if tree == nil {
		t.Fatal("Index tree not found in memory")
	}

	// Construct key for 'Alice'
	// Key = Encode('Alice') + RowID(1) since it's non-unique
	nameVal := types.NewText("Alice")
	rowIDVal := types.NewInt(1)
	key := record.Encode([]types.Value{nameVal, rowIDVal})

	// Check existence
	_, err = tree.Get(key)
	if err != nil {
		t.Errorf("Index entry not found: %v", err)
	}
}

func TestExecutor_Insert_UpdatesUniqueIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (email TEXT)")
	exec.Execute("CREATE UNIQUE INDEX idx_email ON users (email)")

	// Insert row
	_, err := exec.Execute("INSERT INTO users VALUES ('alice@example.com')")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Verify index entry
	tree := exec.trees["index:idx_email"]
	if tree == nil {
		t.Fatal("Index tree not found")
	}

	// Construct key
	key := record.Encode([]types.Value{types.NewText("alice@example.com")})

	// Check existence
	val, err := tree.Get(key)
	if err != nil {
		t.Errorf("Index entry not found: %v", err)
	}
	if len(val) != 8 { // Value is RowID
		t.Errorf("Expected 8 bytes value (rowid), got %d", len(val))
	}

	// Insert duplicate
	_, err = exec.Execute("INSERT INTO users VALUES ('alice@example.com')")
	if err == nil {
		t.Error("Expected error for duplicate unique index key")
	}
}

// TestCreateIndex_PopulatesExistingData tests that CREATE INDEX populates
// the index with existing data from the table.
func TestCreateIndex_PopulatesExistingData(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table and insert data BEFORE creating index
	_, err := exec.Execute("CREATE TABLE products (id INT, name TEXT, price INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert several rows
	_, err = exec.Execute("INSERT INTO products VALUES (1, 'Apple', 100)")
	if err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}
	_, err = exec.Execute("INSERT INTO products VALUES (2, 'Banana', 50)")
	if err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}
	_, err = exec.Execute("INSERT INTO products VALUES (3, 'Cherry', 200)")
	if err != nil {
		t.Fatalf("INSERT 3: %v", err)
	}

	// Now create index on existing data
	_, err = exec.Execute("CREATE INDEX idx_price ON products (price)")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Verify index tree exists
	tree := exec.trees["index:idx_price"]
	if tree == nil {
		t.Fatal("Index tree not found in memory")
	}

	// Verify all three rows are in the index
	// Non-unique index: Key = Columns + RowID
	// Check entry for price=100, rowid=1
	key1 := record.Encode([]types.Value{types.NewInt(100), types.NewInt(1)})
	_, err = tree.Get(key1)
	if err != nil {
		t.Errorf("Index entry for price=100, rowid=1 not found: %v", err)
	}

	// Check entry for price=50, rowid=2
	key2 := record.Encode([]types.Value{types.NewInt(50), types.NewInt(2)})
	_, err = tree.Get(key2)
	if err != nil {
		t.Errorf("Index entry for price=50, rowid=2 not found: %v", err)
	}

	// Check entry for price=200, rowid=3
	key3 := record.Encode([]types.Value{types.NewInt(200), types.NewInt(3)})
	_, err = tree.Get(key3)
	if err != nil {
		t.Errorf("Index entry for price=200, rowid=3 not found: %v", err)
	}
}

// TestCreateUniqueIndex_PopulatesExistingData tests that CREATE UNIQUE INDEX
// populates the index with existing data.
func TestCreateUniqueIndex_PopulatesExistingData(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table and insert data BEFORE creating index
	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (1, 'alice@test.com')")
	if err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}
	_, err = exec.Execute("INSERT INTO users VALUES (2, 'bob@test.com')")
	if err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}

	// Create unique index on existing data
	_, err = exec.Execute("CREATE UNIQUE INDEX idx_email ON users (email)")
	if err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}

	// Verify index tree exists
	tree := exec.trees["index:idx_email"]
	if tree == nil {
		t.Fatal("Index tree not found in memory")
	}

	// Unique index: Key = Columns, Value = RowID
	// Check entry for email='alice@test.com'
	key1 := record.Encode([]types.Value{types.NewText("alice@test.com")})
	val1, err := tree.Get(key1)
	if err != nil {
		t.Errorf("Index entry for alice@test.com not found: %v", err)
	}
	if len(val1) == 8 {
		rowid := binary.BigEndian.Uint64(val1)
		if rowid != 1 {
			t.Errorf("Expected rowid 1 for alice, got %d", rowid)
		}
	}

	// Check entry for email='bob@test.com'
	key2 := record.Encode([]types.Value{types.NewText("bob@test.com")})
	val2, err := tree.Get(key2)
	if err != nil {
		t.Errorf("Index entry for bob@test.com not found: %v", err)
	}
	if len(val2) == 8 {
		rowid := binary.BigEndian.Uint64(val2)
		if rowid != 2 {
			t.Errorf("Expected rowid 2 for bob, got %d", rowid)
		}
	}
}

// TestCreateUniqueIndex_FailsOnDuplicates tests that CREATE UNIQUE INDEX
// fails if the table already contains duplicate values in the indexed column.
func TestCreateUniqueIndex_FailsOnDuplicates(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table and insert duplicate data BEFORE creating index
	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (1, 'same@test.com')")
	if err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}
	_, err = exec.Execute("INSERT INTO users VALUES (2, 'same@test.com')")
	if err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}

	// Attempt to create unique index - should fail
	_, err = exec.Execute("CREATE UNIQUE INDEX idx_email ON users (email)")
	if err == nil {
		t.Error("Expected error when creating unique index on duplicate data")
	}
}

// TestUniqueIndex_AllowsMultipleNulls tests that unique indexes allow
// multiple NULL values (standard SQL behavior).
func TestUniqueIndex_AllowsMultipleNulls(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with unique index
	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = exec.Execute("CREATE UNIQUE INDEX idx_email ON users (email)")
	if err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}

	// Insert first row with NULL email - should succeed
	_, err = exec.Execute("INSERT INTO users VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("INSERT with NULL 1: %v", err)
	}

	// Insert second row with NULL email - should ALSO succeed (multiple NULLs allowed)
	_, err = exec.Execute("INSERT INTO users VALUES (2, NULL)")
	if err != nil {
		t.Errorf("INSERT with NULL 2 should succeed but got: %v", err)
	}

	// Insert third row with NULL email - should also succeed
	_, err = exec.Execute("INSERT INTO users VALUES (3, NULL)")
	if err != nil {
		t.Errorf("INSERT with NULL 3 should succeed but got: %v", err)
	}

	// But non-NULL duplicate should still fail
	_, err = exec.Execute("INSERT INTO users VALUES (4, 'test@example.com')")
	if err != nil {
		t.Fatalf("INSERT with value: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (5, 'test@example.com')")
	if err == nil {
		t.Error("INSERT with duplicate non-NULL value should fail")
	}
}

// TestUniqueIndex_MultiColumnWithNulls tests unique index behavior with
// multiple columns where some are NULL.
func TestUniqueIndex_MultiColumnWithNulls(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with multi-column unique index
	_, err := exec.Execute("CREATE TABLE orders (id INT, customer_id INT, order_date TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = exec.Execute("CREATE UNIQUE INDEX idx_cust_date ON orders (customer_id, order_date)")
	if err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}

	// Insert row with first column NULL
	_, err = exec.Execute("INSERT INTO orders VALUES (1, NULL, '2024-01-01')")
	if err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}

	// Insert another row with same first column NULL - should succeed (NULL != NULL)
	_, err = exec.Execute("INSERT INTO orders VALUES (2, NULL, '2024-01-01')")
	if err != nil {
		t.Errorf("INSERT 2 with NULL should succeed but got: %v", err)
	}

	// Insert row with second column NULL
	_, err = exec.Execute("INSERT INTO orders VALUES (3, 100, NULL)")
	if err != nil {
		t.Fatalf("INSERT 3: %v", err)
	}

	// Insert another row with same second column NULL - should succeed
	_, err = exec.Execute("INSERT INTO orders VALUES (4, 100, NULL)")
	if err != nil {
		t.Errorf("INSERT 4 with NULL should succeed but got: %v", err)
	}

	// But fully non-NULL duplicates should still fail
	_, err = exec.Execute("INSERT INTO orders VALUES (5, 200, '2024-02-01')")
	if err != nil {
		t.Fatalf("INSERT 5: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (6, 200, '2024-02-01')")
	if err == nil {
		t.Error("INSERT with duplicate non-NULL values should fail")
	}
}
