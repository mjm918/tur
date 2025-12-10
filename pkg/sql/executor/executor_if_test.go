package executor

import (
	"testing"
)

// Tests for IF() function execution in SQL queries

func TestExecutor_If_BasicTrue(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// IF(1, 'yes', 'no') should return 'yes'
	result, err := exec.Execute("SELECT IF(1, 'yes', 'no')")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0][0].Text() != "yes" {
		t.Errorf("Expected 'yes', got %q", result.Rows[0][0].Text())
	}
}

func TestExecutor_If_BasicFalse(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// IF(0, 'yes', 'no') should return 'no'
	result, err := exec.Execute("SELECT IF(0, 'yes', 'no')")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0][0].Text() != "no" {
		t.Errorf("Expected 'no', got %q", result.Rows[0][0].Text())
	}
}

func TestExecutor_If_WithComparison(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup table and data
	_, err := exec.Execute("CREATE TABLE scores (id INT PRIMARY KEY, score INT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO scores VALUES (1, 85), (2, 45), (3, 70)")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Use IF with comparison: IF(score >= 60, 'pass', 'fail')
	result, err := exec.Execute("SELECT id, IF(score >= 60, 'pass', 'fail') AS status FROM scores ORDER BY id")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	expected := []string{"pass", "fail", "pass"}
	for i, exp := range expected {
		status := result.Rows[i][1].Text()
		if status != exp {
			t.Errorf("Row %d: expected %q, got %q", i, exp, status)
		}
	}
}

func TestExecutor_If_WithNull(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// IF(NULL, 'yes', 'no') should return 'no'
	result, err := exec.Execute("SELECT IF(NULL, 'yes', 'no')")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.Rows[0][0].Text() != "no" {
		t.Errorf("Expected 'no', got %q", result.Rows[0][0].Text())
	}
}

func TestExecutor_If_NestedIf(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup table and data
	_, err := exec.Execute("CREATE TABLE grades (id INT PRIMARY KEY, score INT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO grades VALUES (1, 95), (2, 75), (3, 55), (4, 35)")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Nested IF: grade assignment
	result, err := exec.Execute(`
		SELECT id, IF(score >= 90, 'A', IF(score >= 70, 'B', IF(score >= 50, 'C', 'F'))) AS grade
		FROM grades ORDER BY id
	`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	expected := []string{"A", "B", "C", "F"}
	for i, exp := range expected {
		grade := result.Rows[i][1].Text()
		if grade != exp {
			t.Errorf("Row %d: expected %q, got %q", i, exp, grade)
		}
	}
}

func TestExecutor_If_InUpdate(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup table and data
	_, err := exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, price INT, discount INT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO products VALUES (1, 100, 0), (2, 200, 0), (3, 50, 0)")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Update using IF: apply 10% discount for expensive items
	_, err = exec.Execute("UPDATE products SET discount = IF(price >= 100, price / 10, 0)")
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Verify results
	result, err := exec.Execute("SELECT id, discount FROM products ORDER BY id")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	expected := []int64{10, 20, 0}
	for i, exp := range expected {
		discount := result.Rows[i][1].Int()
		if discount != exp {
			t.Errorf("Row %d: expected discount %d, got %d", i, exp, discount)
		}
	}
}

func TestExecutor_If_WithArithmetic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// IF with arithmetic in result values
	result, err := exec.Execute("SELECT IF(5 > 3, 10 + 20, 100 - 50)")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// 5 > 3 is true, so result should be 10 + 20 = 30
	if result.Rows[0][0].Int() != 30 {
		t.Errorf("Expected 30, got %d", result.Rows[0][0].Int())
	}
}

func TestExecutor_If_MixedTypes(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// IF returning different types based on condition
	// When true returns integer, when false returns string
	result, err := exec.Execute("SELECT IF(1, 42, 'forty-two')")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if result.Rows[0][0].Int() != 42 {
		t.Errorf("Expected 42, got %v", result.Rows[0][0])
	}
}
