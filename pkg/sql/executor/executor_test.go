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
