package executor

import (
	"fmt"
	"testing"
)

// TestWhereIn tests the IN predicate (column IN (value1, value2, ...))
func TestWhereIn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create test table
	_, err := exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, category TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert test data
	testData := []struct {
		id       int
		name     string
		category string
	}{
		{1, "Apple", "fruit"},
		{2, "Banana", "fruit"},
		{3, "Carrot", "vegetable"},
		{4, "Broccoli", "vegetable"},
		{5, "Milk", "dairy"},
	}

	for _, d := range testData {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO products VALUES (%d, '%s', '%s')", d.id, d.name, d.category))
		if err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	// Test IN with integers
	result, err := exec.Execute("SELECT name FROM products WHERE id IN (1, 3, 5) ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT with IN (integers): %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}
	expected := []string{"Apple", "Carrot", "Milk"}
	for i, row := range result.Rows {
		if row[0].Text() != expected[i] {
			t.Errorf("Row %d: expected %s, got %s", i, expected[i], row[0].Text())
		}
	}

	// Test IN with strings
	result, err = exec.Execute("SELECT name FROM products WHERE category IN ('fruit', 'dairy') ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT with IN (strings): %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows for fruit/dairy, got %d", len(result.Rows))
	}

	// Test NOT IN
	result, err = exec.Execute("SELECT name FROM products WHERE id NOT IN (1, 2) ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT with NOT IN: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows for NOT IN, got %d", len(result.Rows))
	}
}

// TestWhereLike tests the LIKE predicate (column LIKE pattern)
func TestWhereLike(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create test table
	_, err := exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, description TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert test data
	testData := []struct {
		id          int
		name        string
		description string
	}{
		{1, "Apple iPhone", "A smartphone by Apple"},
		{2, "Samsung Galaxy", "A smartphone by Samsung"},
		{3, "Apple MacBook", "A laptop by Apple"},
		{4, "Dell XPS", "A laptop by Dell"},
		{5, "Apple Watch", "A smartwatch by Apple"},
	}

	for _, d := range testData {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO products VALUES (%d, '%s', '%s')", d.id, d.name, d.description))
		if err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	// Test LIKE with % at start (ends with pattern)
	result, err := exec.Execute("SELECT name FROM products WHERE name LIKE '%Watch'")
	if err != nil {
		t.Fatalf("SELECT with LIKE (ends with): %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row for LIKE '%%Watch', got %d", len(result.Rows))
	}
	if len(result.Rows) > 0 && result.Rows[0][0].Text() != "Apple Watch" {
		t.Errorf("Expected 'Apple Watch', got '%s'", result.Rows[0][0].Text())
	}

	// Test LIKE with % at end (starts with pattern)
	result, err = exec.Execute("SELECT name FROM products WHERE name LIKE 'Apple%' ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT with LIKE (starts with): %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows for LIKE 'Apple%%', got %d", len(result.Rows))
	}

	// Test LIKE with % on both sides (contains pattern)
	result, err = exec.Execute("SELECT name FROM products WHERE name LIKE '%Galaxy%'")
	if err != nil {
		t.Fatalf("SELECT with LIKE (contains): %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row for LIKE '%%Galaxy%%', got %d", len(result.Rows))
	}

	// Test LIKE with _ single character wildcard
	result, err = exec.Execute("SELECT name FROM products WHERE name LIKE 'Dell ___'")
	if err != nil {
		t.Fatalf("SELECT with LIKE (underscore): %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row for LIKE 'Dell ___', got %d", len(result.Rows))
	}

	// Test NOT LIKE
	result, err = exec.Execute("SELECT name FROM products WHERE name NOT LIKE 'Apple%' ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT with NOT LIKE: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows for NOT LIKE 'Apple%%', got %d", len(result.Rows))
	}

	// Test case-insensitive LIKE (ILIKE style or case insensitive by default)
	result, err = exec.Execute("SELECT name FROM products WHERE name LIKE '%iphone%'")
	if err != nil {
		t.Fatalf("SELECT with LIKE (case insensitive): %v", err)
	}
	// Note: Standard SQL LIKE is case-sensitive, but we might want case-insensitive
	// This test documents current behavior
	t.Logf("Case insensitive LIKE returned %d rows", len(result.Rows))
}

// TestFindInSet tests the FIND_IN_SET function
func TestFindInSet(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create test table
	_, err := exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT, tags TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert test data with comma-separated values
	testData := []struct {
		id   int
		name string
		tags string
	}{
		{1, "Post 1", "news,tech,featured"},
		{2, "Post 2", "sports,featured"},
		{3, "Post 3", "tech,science"},
		{4, "Post 4", "news,sports"},
		{5, "Post 5", "featured"},
	}

	for _, d := range testData {
		_, err = exec.Execute(fmt.Sprintf("INSERT INTO items VALUES (%d, '%s', '%s')", d.id, d.name, d.tags))
		if err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	// Test FIND_IN_SET returns position (1-indexed)
	result, err := exec.Execute("SELECT FIND_IN_SET('tech', 'news,tech,featured')")
	if err != nil {
		t.Fatalf("SELECT FIND_IN_SET (literal): %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	position := result.Rows[0][0].Int()
	if position != 2 {
		t.Errorf("Expected position 2 for 'tech' in 'news,tech,featured', got %d", position)
	}

	// Test FIND_IN_SET returns 0 when not found
	result, err = exec.Execute("SELECT FIND_IN_SET('music', 'news,tech,featured')")
	if err != nil {
		t.Fatalf("SELECT FIND_IN_SET (not found): %v", err)
	}
	if result.Rows[0][0].Int() != 0 {
		t.Errorf("Expected 0 when not found, got %d", result.Rows[0][0].Int())
	}

	// Test FIND_IN_SET in WHERE clause
	result, err = exec.Execute("SELECT name FROM items WHERE FIND_IN_SET('featured', tags) > 0 ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT with FIND_IN_SET in WHERE: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows with 'featured' tag, got %d", len(result.Rows))
	}
	expected := []string{"Post 1", "Post 2", "Post 5"}
	for i, row := range result.Rows {
		if row[0].Text() != expected[i] {
			t.Errorf("Row %d: expected %s, got %s", i, expected[i], row[0].Text())
		}
	}

	// Test FIND_IN_SET with column reference
	result, err = exec.Execute("SELECT name FROM items WHERE FIND_IN_SET('tech', tags) > 0 ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT with FIND_IN_SET column: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows with 'tech' tag, got %d", len(result.Rows))
	}
}
