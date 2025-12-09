package executor

import (
	"path/filepath"
	"testing"
	"tur/pkg/pager"
)

func TestExecutor_ROW_NUMBER_Simple(t *testing.T) {
	// Setup executor
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	e := New(p)
	defer e.Close()

	// Create test table
	if _, err := e.Execute("CREATE TABLE employees (id INT, name TEXT, salary INT)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	if _, err := e.Execute("INSERT INTO employees VALUES (1, 'Alice', 50000)"); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err := e.Execute("INSERT INTO employees VALUES (2, 'Bob', 60000)"); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err := e.Execute("INSERT INTO employees VALUES (3, 'Charlie', 55000)"); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test ROW_NUMBER without ORDER BY (undefined order, just verify row numbers 1,2,3)
	sql := "SELECT ROW_NUMBER() OVER () FROM employees"
	res, err := e.Execute(sql)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if len(res.Rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(res.Rows))
	}

	// Verify row numbers are 1, 2, 3 (in some order)
	rowNums := make(map[int64]bool)
	for _, row := range res.Rows {
		rowNums[row[0].Int()] = true
	}
	if !rowNums[1] || !rowNums[2] || !rowNums[3] {
		t.Errorf("Expected row numbers 1, 2, 3, got %v", rowNums)
	}
}

func TestExecutor_ROW_NUMBER_WithOrderBy(t *testing.T) {
	// Setup executor
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	e := New(p)
	defer e.Close()

	// Create test table
	if _, err := e.Execute("CREATE TABLE employees (id INT, name TEXT, salary INT)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	if _, err := e.Execute("INSERT INTO employees VALUES (1, 'Alice', 50000)"); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err := e.Execute("INSERT INTO employees VALUES (2, 'Bob', 60000)"); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err := e.Execute("INSERT INTO employees VALUES (3, 'Charlie', 55000)"); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test ROW_NUMBER with ORDER BY salary DESC
	// Expected: Bob(60000) -> 1, Charlie(55000) -> 2, Alice(50000) -> 3
	sql := "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees"
	res, err := e.Execute(sql)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if len(res.Rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(res.Rows))
	}

	// Verify expected order
	expected := []struct {
		name   string
		rowNum int64
	}{
		{"Bob", 1},
		{"Charlie", 2},
		{"Alice", 3},
	}

	for i, row := range res.Rows {
		name := row[0].Text()
		rowNum := row[1].Int()
		if name != expected[i].name || rowNum != expected[i].rowNum {
			t.Errorf("Row %d: got (%s, %d), want (%s, %d)", i, name, rowNum, expected[i].name, expected[i].rowNum)
		}
	}
}

func TestExecutor_ROW_NUMBER_WithPartitionBy(t *testing.T) {
	// Setup executor
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	e := New(p)
	defer e.Close()

	// Create test table with department
	if _, err := e.Execute("CREATE TABLE employees (id INT, name TEXT, dept TEXT, salary INT)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data - two departments
	if _, err := e.Execute("INSERT INTO employees VALUES (1, 'Alice', 'Sales', 50000)"); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err := e.Execute("INSERT INTO employees VALUES (2, 'Bob', 'Sales', 60000)"); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err := e.Execute("INSERT INTO employees VALUES (3, 'Charlie', 'Eng', 55000)"); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err := e.Execute("INSERT INTO employees VALUES (4, 'Diana', 'Eng', 70000)"); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test ROW_NUMBER with PARTITION BY dept ORDER BY salary DESC
	// Expected:
	//   Sales: Bob(60000) -> 1, Alice(50000) -> 2
	//   Eng: Diana(70000) -> 1, Charlie(55000) -> 2
	sql := "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees"
	res, err := e.Execute(sql)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if len(res.Rows) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(res.Rows))
	}

	// Verify row numbers within each partition
	rowNumsByDept := make(map[string][]int64)
	for _, row := range res.Rows {
		dept := row[1].Text()
		rowNum := row[2].Int()
		rowNumsByDept[dept] = append(rowNumsByDept[dept], rowNum)
	}

	// Each department should have row numbers 1 and 2
	for dept, nums := range rowNumsByDept {
		if len(nums) != 2 {
			t.Errorf("Department %s: expected 2 row numbers, got %d", dept, len(nums))
			continue
		}
		hasOne, hasTwo := false, false
		for _, n := range nums {
			if n == 1 {
				hasOne = true
			}
			if n == 2 {
				hasTwo = true
			}
		}
		if !hasOne || !hasTwo {
			t.Errorf("Department %s: expected row numbers 1 and 2, got %v", dept, nums)
		}
	}
}
