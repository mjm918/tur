package executor

import (
	"os"
	"testing"
	"tur/pkg/pager"
)

func TestExecutor_DeleteRemovesIndexEntries(t *testing.T) {
	// Setup
	tmpFile := "test_delete_index.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create table with unique index
	mustExec(t, exec, `CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT);`)
	mustExec(t, exec, `CREATE UNIQUE INDEX idx_products_name ON products (name);`)

	// Insert data
	mustExec(t, exec, `INSERT INTO products VALUES (1, 'Widget', 100);`)
	mustExec(t, exec, `INSERT INTO products VALUES (2, 'Gadget', 200);`)
	mustExec(t, exec, `INSERT INTO products VALUES (3, 'Gizmo', 150);`)

	// Delete a row
	res, err := exec.Execute(`DELETE FROM products WHERE id = 2;`)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row deleted, got %d", res.RowsAffected)
	}

	// Verify row is deleted
	res, err = exec.Execute(`SELECT * FROM products WHERE id = 2;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Errorf("expected 0 rows after delete, got %d", len(res.Rows))
	}

	// Now try to insert another row with the same name 'Gadget'
	// This should succeed because the index entry was deleted
	_, err = exec.Execute(`INSERT INTO products VALUES (4, 'Gadget', 250);`)
	if err != nil {
		t.Fatalf("insert after delete should succeed: %v", err)
	}

	// Verify the new row exists
	res, err = exec.Execute(`SELECT name, price FROM products WHERE id = 4;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0].Text() != "Gadget" || res.Rows[0][1].Int() != 250 {
		t.Errorf("unexpected values: %v", res.Rows[0])
	}
}

func TestExecutor_DeleteWithNonUniqueIndex(t *testing.T) {
	// Setup
	tmpFile := "test_delete_nonunique_index.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create table with non-unique index
	mustExec(t, exec, `CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT);`)
	mustExec(t, exec, `CREATE INDEX idx_orders_customer ON orders (customer_id);`)

	// Insert data - multiple rows with same customer_id
	mustExec(t, exec, `INSERT INTO orders VALUES (1, 100, 50);`)
	mustExec(t, exec, `INSERT INTO orders VALUES (2, 100, 75);`)
	mustExec(t, exec, `INSERT INTO orders VALUES (3, 200, 100);`)

	// Delete one order for customer 100
	res, err := exec.Execute(`DELETE FROM orders WHERE id = 1;`)
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row deleted, got %d", res.RowsAffected)
	}

	// Verify remaining orders
	res, err = exec.Execute(`SELECT id, customer_id FROM orders ORDER BY id;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows remaining, got %d", len(res.Rows))
	}

	// Check order 2 is still there
	found := false
	for _, row := range res.Rows {
		if row[0].Int() == 2 && row[1].Int() == 100 {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected order id=2 to still exist")
	}
}

func TestExecutor_DeleteAllRows(t *testing.T) {
	// Setup
	tmpFile := "test_delete_all.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create table with index
	mustExec(t, exec, `CREATE TABLE items (id INT PRIMARY KEY, name TEXT);`)
	mustExec(t, exec, `CREATE UNIQUE INDEX idx_items_name ON items (name);`)

	// Insert data
	mustExec(t, exec, `INSERT INTO items VALUES (1, 'A');`)
	mustExec(t, exec, `INSERT INTO items VALUES (2, 'B');`)
	mustExec(t, exec, `INSERT INTO items VALUES (3, 'C');`)

	// Delete all rows (no WHERE clause)
	res, err := exec.Execute(`DELETE FROM items;`)
	if err != nil {
		t.Fatalf("delete all failed: %v", err)
	}
	if res.RowsAffected != 3 {
		t.Errorf("expected 3 rows deleted, got %d", res.RowsAffected)
	}

	// Verify table is empty
	res, err = exec.Execute(`SELECT * FROM items;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(res.Rows))
	}

	// Verify we can insert same names again (index cleared)
	_, err = exec.Execute(`INSERT INTO items VALUES (4, 'A');`)
	if err != nil {
		t.Fatalf("insert after delete all should succeed: %v", err)
	}
}

func TestExecutor_DeleteForeignKeyRestrict(t *testing.T) {
	// Setup
	tmpFile := "test_delete_fk.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create parent table
	mustExec(t, exec, `CREATE TABLE departments (id INT PRIMARY KEY, name TEXT);`)

	// Create child table with FK reference
	mustExec(t, exec, `CREATE TABLE employees (
		id INT PRIMARY KEY, 
		name TEXT, 
		dept_id INT REFERENCES departments(id)
	);`)

	// Insert data
	mustExec(t, exec, `INSERT INTO departments VALUES (1, 'Engineering');`)
	mustExec(t, exec, `INSERT INTO departments VALUES (2, 'Sales');`)
	mustExec(t, exec, `INSERT INTO employees VALUES (100, 'Alice', 1);`)
	mustExec(t, exec, `INSERT INTO employees VALUES (101, 'Bob', 1);`)

	// Try to delete referenced department - should fail
	_, err = exec.Execute(`DELETE FROM departments WHERE id = 1;`)
	if err == nil {
		t.Fatal("expected FK constraint error, got nil")
	}
	if !containsSubstring(err.Error(), "FOREIGN KEY") {
		t.Errorf("expected FK constraint error, got: %v", err)
	}

	// Delete unreferenced department - should succeed
	res, err := exec.Execute(`DELETE FROM departments WHERE id = 2;`)
	if err != nil {
		t.Fatalf("delete unreferenced should succeed: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row deleted, got %d", res.RowsAffected)
	}

	// Delete child rows first, then parent should work
	mustExec(t, exec, `DELETE FROM employees WHERE dept_id = 1;`)
	res, err = exec.Execute(`DELETE FROM departments WHERE id = 1;`)
	if err != nil {
		t.Fatalf("delete after removing children should succeed: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row deleted, got %d", res.RowsAffected)
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && (containsSubstring(s[1:], substr) || s[:len(substr)] == substr))
}

func TestExecutor_HashJoin(t *testing.T) {
	// Setup
	tmpFile := "test_hash_join.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create tables
	mustExec(t, exec, `CREATE TABLE customers (id INT PRIMARY KEY, name TEXT);`)
	mustExec(t, exec, `CREATE TABLE orders2 (id INT PRIMARY KEY, customer_id INT, total INT);`)

	// Insert data
	mustExec(t, exec, `INSERT INTO customers VALUES (1, 'Alice');`)
	mustExec(t, exec, `INSERT INTO customers VALUES (2, 'Bob');`)
	mustExec(t, exec, `INSERT INTO customers VALUES (3, 'Charlie');`)

	mustExec(t, exec, `INSERT INTO orders2 VALUES (101, 1, 100);`)
	mustExec(t, exec, `INSERT INTO orders2 VALUES (102, 1, 200);`)
	mustExec(t, exec, `INSERT INTO orders2 VALUES (103, 2, 150);`)
	mustExec(t, exec, `INSERT INTO orders2 VALUES (104, 4, 50);`) // Customer 4 doesn't exist

	// Test: INNER JOIN with explicit syntax
	sql := `SELECT name, total FROM customers JOIN orders2 ON customers.id = orders2.customer_id`
	res, err := exec.Execute(sql)
	if err != nil {
		t.Fatalf("join query failed: %v", err)
	}

	// Expected: Alice(100), Alice(200), Bob(150) = 3 rows
	// Note: Customer 4 order should not appear (no matching customer)
	// Note: Charlie should not appear (no orders)
	if len(res.Rows) != 3 {
		t.Errorf("expected 3 rows from join, got %d", len(res.Rows))
		for i, row := range res.Rows {
			t.Logf("Row %d: %v", i, row)
		}
	}

	foundAlice100 := false
	foundAlice200 := false
	foundBob150 := false

	for _, row := range res.Rows {
		if len(row) < 2 {
			continue
		}
		name := row[0].Text()
		total := row[1].Int()

		if name == "Alice" && total == 100 {
			foundAlice100 = true
		}
		if name == "Alice" && total == 200 {
			foundAlice200 = true
		}
		if name == "Bob" && total == 150 {
			foundBob150 = true
		}
	}

	if !foundAlice100 {
		t.Error("missing Alice 100")
	}
	if !foundAlice200 {
		t.Error("missing Alice 200")
	}
	if !foundBob150 {
		t.Error("missing Bob 150")
	}
}
