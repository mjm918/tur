package executor

import (
	"testing"
)

func TestWindowFunction_Rank_Basic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with some data
	_, err := exec.Execute("CREATE TABLE scores (name TEXT, score INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with some ties
	testData := []string{
		"INSERT INTO scores VALUES ('Alice', 100)",
		"INSERT INTO scores VALUES ('Bob', 90)",
		"INSERT INTO scores VALUES ('Charlie', 90)",
		"INSERT INTO scores VALUES ('Dave', 80)",
	}
	for _, sql := range testData {
		_, err = exec.Execute(sql)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test RANK() - should have gaps after ties
	result, err := exec.Execute("SELECT name, score, RANK() OVER (ORDER BY score DESC) FROM scores")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}

	if len(result.Rows) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(result.Rows))
	}

	// Expected: Alice=1, Bob=2, Charlie=2, Dave=4 (gap after ties)
	expectedRanks := map[string]int64{
		"Alice":   1,
		"Bob":     2,
		"Charlie": 2,
		"Dave":    4, // Gap: rank 3 is skipped
	}

	for _, row := range result.Rows {
		name := row[0].Text()
		rank := row[2].Int()
		expected, ok := expectedRanks[name]
		if !ok {
			t.Errorf("Unexpected name: %s", name)
			continue
		}
		if rank != expected {
			t.Errorf("RANK for %s = %d, want %d", name, rank, expected)
		}
	}
}

func TestWindowFunction_DenseRank_Basic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with some data
	_, err := exec.Execute("CREATE TABLE scores (name TEXT, score INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data with some ties
	testData := []string{
		"INSERT INTO scores VALUES ('Alice', 100)",
		"INSERT INTO scores VALUES ('Bob', 90)",
		"INSERT INTO scores VALUES ('Charlie', 90)",
		"INSERT INTO scores VALUES ('Dave', 80)",
	}
	for _, sql := range testData {
		_, err = exec.Execute(sql)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test DENSE_RANK() - no gaps after ties
	result, err := exec.Execute("SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) FROM scores")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}

	if len(result.Rows) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(result.Rows))
	}

	// Expected: Alice=1, Bob=2, Charlie=2, Dave=3 (no gap)
	expectedRanks := map[string]int64{
		"Alice":   1,
		"Bob":     2,
		"Charlie": 2,
		"Dave":    3, // No gap: rank follows consecutively
	}

	for _, row := range result.Rows {
		name := row[0].Text()
		rank := row[2].Int()
		expected, ok := expectedRanks[name]
		if !ok {
			t.Errorf("Unexpected name: %s", name)
			continue
		}
		if rank != expected {
			t.Errorf("DENSE_RANK for %s = %d, want %d", name, rank, expected)
		}
	}
}

func TestWindowFunction_Rank_WithPartitionBy(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with departments
	_, err := exec.Execute("CREATE TABLE employees (name TEXT, dept TEXT, score INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []string{
		"INSERT INTO employees VALUES ('Alice', 'Sales', 100)",
		"INSERT INTO employees VALUES ('Bob', 'Sales', 90)",
		"INSERT INTO employees VALUES ('Charlie', 'Engineering', 95)",
		"INSERT INTO employees VALUES ('Dave', 'Engineering', 85)",
	}
	for _, sql := range testData {
		_, err = exec.Execute(sql)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test RANK() with PARTITION BY
	result, err := exec.Execute("SELECT name, dept, RANK() OVER (PARTITION BY dept ORDER BY score DESC) FROM employees")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}

	if len(result.Rows) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(result.Rows))
	}

	// Expected: Ranks reset per department
	// Sales: Alice=1, Bob=2
	// Engineering: Charlie=1, Dave=2
	expectedRanks := map[string]int64{
		"Alice":   1, // Sales top
		"Bob":     2, // Sales second
		"Charlie": 1, // Engineering top
		"Dave":    2, // Engineering second
	}

	for _, row := range result.Rows {
		name := row[0].Text()
		rank := row[2].Int()
		expected, ok := expectedRanks[name]
		if !ok {
			t.Errorf("Unexpected name: %s", name)
			continue
		}
		if rank != expected {
			t.Errorf("RANK for %s = %d, want %d", name, rank, expected)
		}
	}
}

func TestWindowFunction_PeerRows(t *testing.T) {
	// Test that peer rows (same ORDER BY value) get the same rank
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE scores (name TEXT, score INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// All have the same score
	testData := []string{
		"INSERT INTO scores VALUES ('Alice', 100)",
		"INSERT INTO scores VALUES ('Bob', 100)",
		"INSERT INTO scores VALUES ('Charlie', 100)",
	}
	for _, sql := range testData {
		_, err = exec.Execute(sql)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test RANK() - all should be rank 1
	result, err := exec.Execute("SELECT name, RANK() OVER (ORDER BY score DESC) FROM scores")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}

	for _, row := range result.Rows {
		name := row[0].Text()
		rank := row[1].Int()
		if rank != 1 {
			t.Errorf("RANK for %s = %d, want 1 (all same score)", name, rank)
		}
	}
}
