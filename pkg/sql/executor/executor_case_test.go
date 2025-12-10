package executor

import (
	"testing"
)

// Tests for CASE expression execution
// These test bulk UPDATE operations using CASE expressions

func TestExecutor_CaseExpr_SearchedCase(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup table and data
	_, err := exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, price INT, category TEXT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO products VALUES (1, 100, 'electronics'), (2, 50, 'books'), (3, 200, 'electronics'), (4, 30, 'books')")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Bulk update using searched CASE: different price adjustments by category
	_, err = exec.Execute(`
		UPDATE products SET price = CASE
			WHEN category = 'electronics' THEN price * 2
			WHEN category = 'books' THEN price + 10
			ELSE price
		END
	`)
	if err != nil {
		t.Fatalf("Update with CASE: %v", err)
	}

	// Verify results
	result, err := exec.Execute("SELECT id, price FROM products ORDER BY id")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	expected := []struct {
		id    int64
		price int64
	}{
		{1, 200},  // electronics: 100 * 2
		{2, 60},   // books: 50 + 10
		{3, 400},  // electronics: 200 * 2
		{4, 40},   // books: 30 + 10
	}

	if len(result.Rows) != len(expected) {
		t.Fatalf("Expected %d rows, got %d", len(expected), len(result.Rows))
	}

	for i, exp := range expected {
		id := result.Rows[i][0].Int()
		price := result.Rows[i][1].Int()
		if id != exp.id || price != exp.price {
			t.Errorf("Row %d: got (id=%d, price=%d), want (id=%d, price=%d)", i, id, price, exp.id, exp.price)
		}
	}
}

func TestExecutor_CaseExpr_SimpleCase(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup table and data
	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, status INT, status_text TEXT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (1, 1, ''), (2, 2, ''), (3, 3, ''), (4, 1, '')")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Update using simple CASE: map status codes to text
	_, err = exec.Execute(`
		UPDATE users SET status_text = CASE status
			WHEN 1 THEN 'active'
			WHEN 2 THEN 'inactive'
			WHEN 3 THEN 'pending'
			ELSE 'unknown'
		END
	`)
	if err != nil {
		t.Fatalf("Update with simple CASE: %v", err)
	}

	// Verify results
	result, err := exec.Execute("SELECT id, status_text FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	expected := []struct {
		id         int64
		statusText string
	}{
		{1, "active"},
		{2, "inactive"},
		{3, "pending"},
		{4, "active"},
	}

	if len(result.Rows) != len(expected) {
		t.Fatalf("Expected %d rows, got %d", len(expected), len(result.Rows))
	}

	for i, exp := range expected {
		id := result.Rows[i][0].Int()
		statusText := result.Rows[i][1].Text()
		if id != exp.id || statusText != exp.statusText {
			t.Errorf("Row %d: got (id=%d, status_text=%q), want (id=%d, status_text=%q)", i, id, statusText, exp.id, exp.statusText)
		}
	}
}

func TestExecutor_CaseExpr_WithoutElse(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup
	_, err := exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, value INT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1, 10), (2, 20), (3, 30)")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// CASE without ELSE should return NULL for unmatched
	_, err = exec.Execute(`
		UPDATE items SET value = CASE
			WHEN id = 1 THEN 100
			WHEN id = 2 THEN 200
		END
	`)
	if err != nil {
		t.Fatalf("Update with CASE without ELSE: %v", err)
	}

	// Verify results
	result, err := exec.Execute("SELECT id, value FROM items ORDER BY id")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	// id=1 -> 100, id=2 -> 200, id=3 -> NULL (no ELSE)
	if result.Rows[0][1].Int() != 100 {
		t.Errorf("id=1: expected value 100, got %v", result.Rows[0][1])
	}
	if result.Rows[1][1].Int() != 200 {
		t.Errorf("id=2: expected value 200, got %v", result.Rows[1][1])
	}
	if !result.Rows[2][1].IsNull() {
		t.Errorf("id=3: expected NULL, got %v", result.Rows[2][1])
	}
}

func TestExecutor_CaseExpr_InSelect(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup
	_, err := exec.Execute("CREATE TABLE scores (id INT PRIMARY KEY, score INT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO scores VALUES (1, 95), (2, 75), (3, 55), (4, 35)")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Use CASE in SELECT to compute grades
	result, err := exec.Execute(`
		SELECT id, score, CASE
			WHEN score >= 90 THEN 'A'
			WHEN score >= 70 THEN 'B'
			WHEN score >= 50 THEN 'C'
			ELSE 'F'
		END AS grade FROM scores ORDER BY id
	`)
	if err != nil {
		t.Fatalf("Select with CASE: %v", err)
	}

	expected := []string{"A", "B", "C", "F"}
	for i, exp := range expected {
		grade := result.Rows[i][2].Text()
		if grade != exp {
			t.Errorf("Row %d: expected grade %q, got %q", i, exp, grade)
		}
	}
}

func TestExecutor_CaseExpr_NestedCase(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup
	_, err := exec.Execute("CREATE TABLE data (id INT PRIMARY KEY, x INT, y INT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO data VALUES (1, 5, 10), (2, -5, 20), (3, 15, 5)")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Nested CASE: outer checks sign of x, inner checks comparison with y
	result, err := exec.Execute(`
		SELECT id, CASE
			WHEN x > 0 THEN CASE WHEN x > y THEN 'x>y' ELSE 'x<=y' END
			ELSE 'negative'
		END AS result FROM data ORDER BY id
	`)
	if err != nil {
		t.Fatalf("Select with nested CASE: %v", err)
	}

	expected := []string{"x<=y", "negative", "x>y"}
	for i, exp := range expected {
		res := result.Rows[i][1].Text()
		if res != exp {
			t.Errorf("Row %d: expected %q, got %q", i, exp, res)
		}
	}
}

func TestExecutor_CaseExpr_BulkUpdateWithWhere(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Setup
	_, err := exec.Execute("CREATE TABLE employees (id INT PRIMARY KEY, dept TEXT, salary INT)")
	if err != nil {
		t.Fatalf("Create table: %v", err)
	}

	_, err = exec.Execute("INSERT INTO employees VALUES (1, 'sales', 50000), (2, 'engineering', 70000), (3, 'sales', 45000), (4, 'engineering', 80000), (5, 'hr', 55000)")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Bulk update with CASE and WHERE clause
	result, err := exec.Execute(`
		UPDATE employees SET salary = CASE
			WHEN salary < 50000 THEN salary + 5000
			WHEN salary >= 50000 AND salary < 70000 THEN salary + 3000
			ELSE salary + 1000
		END WHERE dept IN ('sales', 'engineering')
	`)
	if err != nil {
		t.Fatalf("Update with CASE and WHERE: %v", err)
	}

	if result.RowsAffected != 4 {
		t.Errorf("Expected 4 rows affected, got %d", result.RowsAffected)
	}

	// Verify results
	selectResult, err := exec.Execute("SELECT id, salary FROM employees ORDER BY id")
	if err != nil {
		t.Fatalf("Select: %v", err)
	}

	expected := []int64{
		53000, // sales: 50000 -> +3000 (50000 >= 50000)
		71000, // engineering: 70000 -> +1000 (>= 70000)
		50000, // sales: 45000 -> +5000 (< 50000)
		81000, // engineering: 80000 -> +1000 (>= 70000)
		55000, // hr: not updated (not in WHERE)
	}

	for i, exp := range expected {
		salary := selectResult.Rows[i][1].Int()
		if salary != exp {
			t.Errorf("Row %d: expected salary %d, got %d", i, exp, salary)
		}
	}
}
