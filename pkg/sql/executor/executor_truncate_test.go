package executor

import (
	"os"
	"testing"
	"tur/pkg/pager"
)

func TestExecutor_TruncateTable_Basic(t *testing.T) {
	tmpFile := "test_truncate_basic.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create table and insert data
	mustExec(t, exec, `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`)
	mustExec(t, exec, `INSERT INTO users VALUES (1, 'Alice');`)
	mustExec(t, exec, `INSERT INTO users VALUES (2, 'Bob');`)
	mustExec(t, exec, `INSERT INTO users VALUES (3, 'Charlie');`)

	// Verify data exists
	res, err := exec.Execute(`SELECT * FROM users;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Errorf("expected 3 rows before truncate, got %d", len(res.Rows))
	}

	// Truncate table
	res, err = exec.Execute(`TRUNCATE TABLE users;`)
	if err != nil {
		t.Fatalf("truncate failed: %v", err)
	}

	// Verify table is empty
	res, err = exec.Execute(`SELECT * FROM users;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(res.Rows) != 0 {
		t.Errorf("expected 0 rows after truncate, got %d", len(res.Rows))
	}
}

func TestExecutor_TruncateTable_ResetsAutoIncrement(t *testing.T) {
	tmpFile := "test_truncate_autoincrement.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create table and insert data (omitting id to use auto-increment)
	mustExec(t, exec, `CREATE TABLE items (id INT PRIMARY KEY, name TEXT);`)
	mustExec(t, exec, `INSERT INTO items (name) VALUES ('Item1');`)
	mustExec(t, exec, `INSERT INTO items (name) VALUES ('Item2');`)
	mustExec(t, exec, `INSERT INTO items (name) VALUES ('Item3');`)

	// Verify we have 3 rows with ids 1, 2, 3
	res, err := exec.Execute(`SELECT id FROM items ORDER BY id;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}

	// Truncate table
	mustExec(t, exec, `TRUNCATE TABLE items;`)

	// Insert new row - ID should start from 1
	mustExec(t, exec, `INSERT INTO items (name) VALUES ('NewItem');`)

	res, err = exec.Execute(`SELECT id, name FROM items;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0].Int() != 1 {
		t.Errorf("expected id 1 after truncate, got %d", res.Rows[0][0].Int())
	}
}

func TestExecutor_TruncateTable_ClearsIndexes(t *testing.T) {
	tmpFile := "test_truncate_indexes.db"
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

	// Truncate table
	mustExec(t, exec, `TRUNCATE TABLE products;`)

	// Should be able to insert same names again (index should be empty)
	_, err = exec.Execute(`INSERT INTO products VALUES (1, 'Widget', 150);`)
	if err != nil {
		t.Fatalf("insert after truncate should succeed: %v", err)
	}

	// Verify row exists
	res, err := exec.Execute(`SELECT name, price FROM products WHERE id = 1;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
}

func TestExecutor_TruncateTable_NonExistentTable(t *testing.T) {
	tmpFile := "test_truncate_nonexistent.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Truncate non-existent table should fail
	_, err = exec.Execute(`TRUNCATE TABLE nonexistent;`)
	if err == nil {
		t.Fatal("expected error for truncating non-existent table")
	}
}

func TestExecutor_TruncateTable_WithForeignKeyReference(t *testing.T) {
	tmpFile := "test_truncate_fk.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create parent and child tables with FK
	mustExec(t, exec, `CREATE TABLE departments (id INT PRIMARY KEY, name TEXT);`)
	mustExec(t, exec, `CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id));`)

	// Insert data
	mustExec(t, exec, `INSERT INTO departments VALUES (1, 'Engineering');`)
	mustExec(t, exec, `INSERT INTO employees VALUES (1, 'Alice', 1);`)

	// Truncating departments should fail because employees references it
	_, err = exec.Execute(`TRUNCATE TABLE departments;`)
	if err == nil {
		t.Fatal("expected error when truncating table with FK references")
	}

	// Truncating employees should succeed (no references to it)
	_, err = exec.Execute(`TRUNCATE TABLE employees;`)
	if err != nil {
		t.Fatalf("truncate employees should succeed: %v", err)
	}

	// Now truncating departments should succeed (no more references)
	_, err = exec.Execute(`TRUNCATE TABLE departments;`)
	if err != nil {
		t.Fatalf("truncate departments should succeed after employees truncated: %v", err)
	}
}
