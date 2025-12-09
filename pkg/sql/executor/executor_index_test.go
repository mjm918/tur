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

// TestCreatePartialIndex tests creating a partial index with WHERE clause
func TestCreatePartialIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with an active flag
	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT, active INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create partial index on email for active users only
	_, err = exec.Execute("CREATE INDEX idx_active_email ON users (email) WHERE active = 1")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Verify the index was created with the WHERE clause stored
	idx := exec.catalog.GetIndex("idx_active_email")
	if idx == nil {
		t.Fatal("Index not found in catalog")
	}

	if !idx.IsPartial() {
		t.Error("IsPartial() = false, want true")
	}

	if idx.WhereClause != "active = 1" {
		t.Errorf("WhereClause = %q, want 'active = 1'", idx.WhereClause)
	}
}

// TestCreatePartialIndex_ComplexPredicate tests partial index with complex WHERE
func TestCreatePartialIndex_ComplexPredicate(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE products (id INT, status TEXT, price INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create partial index with AND predicate
	_, err = exec.Execute("CREATE INDEX idx_available ON products (price) WHERE status = 'available' AND price > 0")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	idx := exec.catalog.GetIndex("idx_available")
	if idx == nil {
		t.Fatal("Index not found")
	}

	if !idx.IsPartial() {
		t.Error("IsPartial() = false, want true")
	}

	// The predicate should contain both conditions
	if idx.WhereClause == "" {
		t.Error("WhereClause should not be empty")
	}
}

// TestCreateUniquePartialIndex tests unique partial index
func TestCreateUniquePartialIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE accounts (id INT, email TEXT, deleted INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create unique partial index - unique constraint only on non-deleted rows
	_, err = exec.Execute("CREATE UNIQUE INDEX idx_unique_email ON accounts (email) WHERE deleted = 0")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	idx := exec.catalog.GetIndex("idx_unique_email")
	if idx == nil {
		t.Fatal("Index not found")
	}

	if !idx.Unique {
		t.Error("Unique = false, want true")
	}

	if !idx.IsPartial() {
		t.Error("IsPartial() = false, want true")
	}

	if idx.WhereClause != "deleted = 0" {
		t.Errorf("WhereClause = %q, want 'deleted = 0'", idx.WhereClause)
	}
}

// TestPartialIndex_InsertMatchingRows tests that rows matching the predicate are indexed
func TestPartialIndex_InsertMatchingRows(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT, active INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create partial index for active users only
	_, err = exec.Execute("CREATE INDEX idx_active ON users (email) WHERE active = 1")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Insert row that matches predicate (active = 1)
	_, err = exec.Execute("INSERT INTO users VALUES (1, 'alice@test.com', 1)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Insert row that does NOT match predicate (active = 0)
	_, err = exec.Execute("INSERT INTO users VALUES (2, 'bob@test.com', 0)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Check the index tree - should only have 1 entry (Alice)
	tree := exec.trees["index:idx_active"]
	if tree == nil {
		t.Fatal("Index tree not found")
	}

	// Count entries in the index
	count := 0
	cursor := tree.Cursor()
	defer cursor.Close()
	for cursor.First(); cursor.Valid(); cursor.Next() {
		count++
	}

	if count != 1 {
		t.Errorf("Index entries = %d, want 1 (only matching row)", count)
	}
}

// TestPartialIndex_InsertNonMatchingRows tests that non-matching rows are not indexed
func TestPartialIndex_InsertNonMatchingRows(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE products (id INT, name TEXT, price INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create partial index for products with price > 100
	_, err = exec.Execute("CREATE INDEX idx_expensive ON products (name) WHERE price > 100")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Insert cheap products (price <= 100) - should NOT be indexed
	_, err = exec.Execute("INSERT INTO products VALUES (1, 'Widget', 50)")
	if err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}
	_, err = exec.Execute("INSERT INTO products VALUES (2, 'Gadget', 100)")
	if err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}

	// Insert expensive product (price > 100) - should be indexed
	_, err = exec.Execute("INSERT INTO products VALUES (3, 'Luxury', 200)")
	if err != nil {
		t.Fatalf("INSERT 3: %v", err)
	}

	// Check index - should have only 1 entry
	tree := exec.trees["index:idx_expensive"]
	if tree == nil {
		t.Fatal("Index tree not found")
	}

	count := 0
	cursor := tree.Cursor()
	defer cursor.Close()
	for cursor.First(); cursor.Valid(); cursor.Next() {
		count++
	}

	if count != 1 {
		t.Errorf("Index entries = %d, want 1 (only expensive product)", count)
	}
}

// TestPartialIndex_UniqueEnforcementOnlyForMatching tests unique constraint
// is only enforced for rows matching the partial index predicate
func TestPartialIndex_UniqueEnforcementOnlyForMatching(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE accounts (id INT, email TEXT, deleted INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create unique partial index - unique only for non-deleted accounts
	_, err = exec.Execute("CREATE UNIQUE INDEX idx_email ON accounts (email) WHERE deleted = 0")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Insert first active account
	_, err = exec.Execute("INSERT INTO accounts VALUES (1, 'test@test.com', 0)")
	if err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}

	// Insert deleted account with same email - should succeed (not in index)
	_, err = exec.Execute("INSERT INTO accounts VALUES (2, 'test@test.com', 1)")
	if err != nil {
		t.Errorf("INSERT duplicate email for deleted account should succeed, got: %v", err)
	}

	// Insert another active account with same email - should fail (unique violation)
	_, err = exec.Execute("INSERT INTO accounts VALUES (3, 'test@test.com', 0)")
	if err == nil {
		t.Error("INSERT duplicate email for active account should fail")
	}
}

// ========== Expression Index Tests ==========

func TestCreateIndex_Expression_StoresMetadata(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create expression index on UPPER(name)
	_, err = exec.Execute("CREATE INDEX idx_upper_name ON users (UPPER(name))")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Verify the index was created with expression metadata
	idx := exec.catalog.GetIndex("idx_upper_name")
	if idx == nil {
		t.Fatal("Index not found in catalog")
	}

	// Should have no plain columns
	if len(idx.Columns) != 0 {
		t.Errorf("Columns: got %v, want empty", idx.Columns)
	}

	// Should have one expression
	if len(idx.Expressions) != 1 {
		t.Fatalf("Expressions: got %d, want 1", len(idx.Expressions))
	}

	// The expression should be stored as SQL text
	if idx.Expressions[0] != "UPPER(name)" {
		t.Errorf("Expressions[0]: got %q, want 'UPPER(name)'", idx.Expressions[0])
	}

	// Should be marked as expression index
	if !idx.IsExpressionIndex() {
		t.Error("IsExpressionIndex: expected true")
	}
}

func TestCreateIndex_Expression_BinaryExpr(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE orders (id INT, price INT, quantity INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create expression index on (price * quantity)
	_, err = exec.Execute("CREATE INDEX idx_total ON orders ((price * quantity))")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Verify the index was created with expression metadata
	idx := exec.catalog.GetIndex("idx_total")
	if idx == nil {
		t.Fatal("Index not found in catalog")
	}

	if len(idx.Expressions) != 1 {
		t.Fatalf("Expressions: got %d, want 1", len(idx.Expressions))
	}

	// The expression should be stored
	expr := idx.Expressions[0]
	// Note: The exact format may vary depending on how we serialize it
	if expr != "(price * quantity)" && expr != "price * quantity" {
		t.Errorf("Expressions[0]: got %q, want '(price * quantity)' or 'price * quantity'", expr)
	}
}

func TestCreateIndex_Expression_Mixed(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE users (id INT, status TEXT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create index with both plain column and expression
	_, err = exec.Execute("CREATE INDEX idx_mixed ON users (status, LOWER(name))")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Verify the index
	idx := exec.catalog.GetIndex("idx_mixed")
	if idx == nil {
		t.Fatal("Index not found in catalog")
	}

	// Should have one plain column
	if len(idx.Columns) != 1 || idx.Columns[0] != "status" {
		t.Errorf("Columns: got %v, want ['status']", idx.Columns)
	}

	// Should have one expression
	if len(idx.Expressions) != 1 {
		t.Fatalf("Expressions: got %d, want 1", len(idx.Expressions))
	}
	if idx.Expressions[0] != "LOWER(name)" {
		t.Errorf("Expressions[0]: got %q, want 'LOWER(name)'", idx.Expressions[0])
	}
}

// ========== Expression Index Insert/Update Tests ==========

func TestExpressionIndex_Insert_UpdatesIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create expression index on UPPER(name)
	_, err = exec.Execute("CREATE INDEX idx_upper_name ON users (UPPER(name))")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Insert a row
	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Verify the index contains entry for UPPER('Alice') = 'ALICE'
	tree := exec.trees["index:idx_upper_name"]
	if tree == nil {
		t.Fatal("Index tree not found")
	}

	// For non-unique index, key = encoded(exprValue, rowID)
	// exprValue = UPPER('Alice') = 'ALICE'
	key := record.Encode([]types.Value{types.NewText("ALICE"), types.NewInt(1)})

	_, err = tree.Get(key)
	if err != nil {
		t.Errorf("Index entry not found for UPPER('Alice'): %v", err)
	}
}

func TestExpressionIndex_Insert_BinaryExpr(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE orders (id INT, price INT, quantity INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create expression index on (price * quantity)
	_, err = exec.Execute("CREATE INDEX idx_total ON orders ((price * quantity))")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Insert a row: price=100, quantity=5 -> total=500
	_, err = exec.Execute("INSERT INTO orders VALUES (1, 100, 5)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Verify the index contains entry for 100 * 5 = 500
	tree := exec.trees["index:idx_total"]
	if tree == nil {
		t.Fatal("Index tree not found")
	}

	// For non-unique index, key = encoded(exprValue, rowID)
	key := record.Encode([]types.Value{types.NewInt(500), types.NewInt(1)})

	_, err = tree.Get(key)
	if err != nil {
		t.Errorf("Index entry not found for price*quantity=500: %v", err)
	}
}

func TestExpressionIndex_Insert_MultipleRows(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create expression index on LOWER(name)
	_, err = exec.Execute("CREATE INDEX idx_lower_name ON users (LOWER(name))")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Insert multiple rows
	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}
	_, err = exec.Execute("INSERT INTO users VALUES (2, 'BOB')")
	if err != nil {
		t.Fatalf("INSERT 2: %v", err)
	}
	_, err = exec.Execute("INSERT INTO users VALUES (3, 'Charlie')")
	if err != nil {
		t.Fatalf("INSERT 3: %v", err)
	}

	tree := exec.trees["index:idx_lower_name"]
	if tree == nil {
		t.Fatal("Index tree not found")
	}

	// Check each entry
	tests := []struct {
		name  string
		rowID int64
	}{
		{"alice", 1},
		{"bob", 2},
		{"charlie", 3},
	}

	for _, tc := range tests {
		key := record.Encode([]types.Value{types.NewText(tc.name), types.NewInt(tc.rowID)})
		_, err = tree.Get(key)
		if err != nil {
			t.Errorf("Index entry not found for LOWER('%s'): %v", tc.name, err)
		}
	}
}

func TestExpressionIndex_UniqueConstraint(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Create UNIQUE expression index on LOWER(email)
	_, err = exec.Execute("CREATE UNIQUE INDEX idx_lower_email ON users (LOWER(email))")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Insert first row
	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice@Example.com')")
	if err != nil {
		t.Fatalf("INSERT 1: %v", err)
	}

	// Insert row with same email in different case - should fail
	_, err = exec.Execute("INSERT INTO users VALUES (2, 'alice@example.com')")
	if err == nil {
		t.Error("Expected unique constraint violation for case-insensitive duplicate email")
	}

	// Insert row with different email - should succeed
	_, err = exec.Execute("INSERT INTO users VALUES (3, 'Bob@Example.com')")
	if err != nil {
		t.Errorf("INSERT 3 should succeed: %v", err)
	}
}
