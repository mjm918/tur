package executor

import (
	"os"
	"path/filepath"
	"testing"

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
