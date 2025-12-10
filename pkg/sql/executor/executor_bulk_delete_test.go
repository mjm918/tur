package executor

import (
	"testing"
)

// Tests for bulk DELETE operations with IN clause

func TestExecutor_BulkDelete_WithIN(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup
	_, err := exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Bulk delete with IN clause
	result, err := exec.Execute("DELETE FROM items WHERE id IN (1, 3, 5)")
	if err != nil {
		t.Fatalf("Delete with IN: %v", err)
	}

	if result.RowsAffected != 3 {
		t.Errorf("Expected 3 rows affected, got %d", result.RowsAffected)
	}

	// Verify remaining rows
	selectResult, err := exec.Execute("SELECT id FROM items ORDER BY id")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(selectResult.Rows) != 2 {
		t.Fatalf("Expected 2 remaining rows, got %d", len(selectResult.Rows))
	}

	if selectResult.Rows[0][0].Int() != 2 || selectResult.Rows[1][0].Int() != 4 {
		t.Errorf("Expected ids [2, 4], got [%d, %d]", selectResult.Rows[0][0].Int(), selectResult.Rows[1][0].Int())
	}
}

func TestExecutor_BulkDelete_WithIN_SomeNotExist(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup
	_, err := exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Delete with IN clause where some IDs don't exist
	result, err := exec.Execute("DELETE FROM items WHERE id IN (1, 99, 3, 100)")
	if err != nil {
		t.Fatalf("Delete with IN: %v", err)
	}

	// Only 2 rows should be deleted (1 and 3)
	if result.RowsAffected != 2 {
		t.Errorf("Expected 2 rows affected, got %d", result.RowsAffected)
	}

	// Verify remaining rows
	selectResult, err := exec.Execute("SELECT id FROM items")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(selectResult.Rows) != 1 {
		t.Fatalf("Expected 1 remaining row, got %d", len(selectResult.Rows))
	}

	if selectResult.Rows[0][0].Int() != 2 {
		t.Errorf("Expected id 2, got %d", selectResult.Rows[0][0].Int())
	}
}

func TestExecutor_BulkDelete_WithIN_LargeSet(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup with 100 rows
	_, err := exec.Execute("CREATE TABLE numbers (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	// Insert 100 rows
	for i := 1; i <= 100; i++ {
		_, err := exec.Execute("INSERT INTO numbers VALUES (" + itoa(i) + ")")
		if err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
	}

	// Delete odd numbers (1, 3, 5, ..., 99) - 50 rows
	result, err := exec.Execute("DELETE FROM numbers WHERE id IN (1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55, 57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77, 79, 81, 83, 85, 87, 89, 91, 93, 95, 97, 99)")
	if err != nil {
		t.Fatalf("Delete with large IN: %v", err)
	}

	if result.RowsAffected != 50 {
		t.Errorf("Expected 50 rows affected, got %d", result.RowsAffected)
	}

	// Verify remaining 50 even numbers
	selectResult, err := exec.Execute("SELECT id FROM numbers ORDER BY id")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(selectResult.Rows) != 50 {
		t.Errorf("Expected 50 remaining rows, got %d", len(selectResult.Rows))
	}

	// Verify first few are even
	if len(selectResult.Rows) > 0 && selectResult.Rows[0][0].Int() != 2 {
		t.Errorf("Expected first remaining id to be 2, got %d", selectResult.Rows[0][0].Int())
	}
}

func TestExecutor_BulkDelete_WithSubquery(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup
	_, err := exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)")
	if err != nil {
		t.Fatalf("Create orders table: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE inactive_customers (customer_id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Create inactive_customers table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO orders VALUES (1, 10, 100), (2, 20, 200), (3, 10, 150), (4, 30, 300), (5, 20, 250)")
	if err != nil {
		t.Fatalf("Insert orders: %v", err)
	}

	_, err = exec.Execute("INSERT INTO inactive_customers VALUES (10), (30)")
	if err != nil {
		t.Fatalf("Insert inactive_customers: %v", err)
	}

	// Delete orders for inactive customers using subquery
	result, err := exec.Execute("DELETE FROM orders WHERE customer_id IN (SELECT customer_id FROM inactive_customers)")
	if err != nil {
		t.Fatalf("Delete with subquery: %v", err)
	}

	// Should delete orders with customer_id 10 and 30 (ids 1, 3, 4)
	if result.RowsAffected != 3 {
		t.Errorf("Expected 3 rows affected, got %d", result.RowsAffected)
	}

	// Verify remaining orders (customer_id 20)
	selectResult, err := exec.Execute("SELECT id FROM orders ORDER BY id")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(selectResult.Rows) != 2 {
		t.Fatalf("Expected 2 remaining rows, got %d", len(selectResult.Rows))
	}

	// Remaining should be ids 2 and 5 (customer_id 20)
	if selectResult.Rows[0][0].Int() != 2 || selectResult.Rows[1][0].Int() != 5 {
		t.Errorf("Expected ids [2, 5], got [%d, %d]", selectResult.Rows[0][0].Int(), selectResult.Rows[1][0].Int())
	}
}

func TestExecutor_BulkDelete_WithForeignKey_Cascade(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup parent and child tables with CASCADE
	_, err := exec.Execute("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Create departments: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE employees (id INT PRIMARY KEY, dept_id INT REFERENCES departments(id) ON DELETE CASCADE, name TEXT)")
	if err != nil {
		t.Fatalf("Create employees: %v", err)
	}

	_, err = exec.Execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')")
	if err != nil {
		t.Fatalf("Insert departments: %v", err)
	}

	_, err = exec.Execute("INSERT INTO employees VALUES (1, 1, 'Alice'), (2, 1, 'Bob'), (3, 2, 'Charlie'), (4, 3, 'Dave')")
	if err != nil {
		t.Fatalf("Insert employees: %v", err)
	}

	// Bulk delete departments - should cascade to employees
	result, err := exec.Execute("DELETE FROM departments WHERE id IN (1, 3)")
	if err != nil {
		t.Fatalf("Delete departments: %v", err)
	}

	if result.RowsAffected != 2 {
		t.Errorf("Expected 2 departments deleted, got %d", result.RowsAffected)
	}

	// Verify employees cascaded (should only have Charlie from Sales left)
	selectResult, err := exec.Execute("SELECT id, name FROM employees")
	if err != nil {
		t.Fatalf("Select employees: %v", err)
	}

	if len(selectResult.Rows) != 1 {
		t.Fatalf("Expected 1 employee remaining, got %d", len(selectResult.Rows))
	}

	if selectResult.Rows[0][1].Text() != "Charlie" {
		t.Errorf("Expected Charlie to remain, got %s", selectResult.Rows[0][1].Text())
	}
}

func TestExecutor_BulkDelete_AllRows(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup
	_, err := exec.Execute("CREATE TABLE temp (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO temp VALUES (1), (2), (3), (4), (5)")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Delete all rows with IN containing all IDs
	result, err := exec.Execute("DELETE FROM temp WHERE id IN (1, 2, 3, 4, 5)")
	if err != nil {
		t.Fatalf("Delete all: %v", err)
	}

	if result.RowsAffected != 5 {
		t.Errorf("Expected 5 rows affected, got %d", result.RowsAffected)
	}

	// Verify table is empty
	selectResult, err := exec.Execute("SELECT id FROM temp")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	if len(selectResult.Rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(selectResult.Rows))
	}
}

// Helper function for int to string conversion
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var s string
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	for i > 0 {
		s = string('0'+byte(i%10)) + s
		i /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}
