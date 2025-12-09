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

func TestWindowFunction_SUM_RunningTotal(t *testing.T) {
	// Test SUM() with ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (running total)
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE sales (id INT, amount INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []string{
		"INSERT INTO sales VALUES (1, 100)",
		"INSERT INTO sales VALUES (2, 200)",
		"INSERT INTO sales VALUES (3, 300)",
		"INSERT INTO sales VALUES (4, 400)",
	}
	for _, sql := range testData {
		_, err = exec.Execute(sql)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Running total: cumulative sum up to current row
	result, err := exec.Execute("SELECT id, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}

	if len(result.Rows) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(result.Rows))
	}

	// Expected running totals: 100, 300, 600, 1000
	expectedSums := []int64{100, 300, 600, 1000}
	for i, row := range result.Rows {
		sum := row[2].Int()
		if sum != expectedSums[i] {
			t.Errorf("Row %d: SUM = %d, want %d", i, sum, expectedSums[i])
		}
	}
}

func TestWindowFunction_AVG_MovingAverage(t *testing.T) {
	// Test AVG() with ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING (3-point moving average)
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE data (id INT, value INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []string{
		"INSERT INTO data VALUES (1, 10)",
		"INSERT INTO data VALUES (2, 20)",
		"INSERT INTO data VALUES (3, 30)",
		"INSERT INTO data VALUES (4, 40)",
		"INSERT INTO data VALUES (5, 50)",
	}
	for _, sql := range testData {
		_, err = exec.Execute(sql)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// 3-point moving average
	result, err := exec.Execute("SELECT id, value, AVG(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM data")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}

	if len(result.Rows) != 5 {
		t.Fatalf("Expected 5 rows, got %d", len(result.Rows))
	}

	// Expected averages:
	// id=1: avg(10,20) = 15 (no preceding)
	// id=2: avg(10,20,30) = 20
	// id=3: avg(20,30,40) = 30
	// id=4: avg(30,40,50) = 40
	// id=5: avg(40,50) = 45 (no following)
	expectedAvgs := []float64{15, 20, 30, 40, 45}
	for i, row := range result.Rows {
		avg := row[2].Float()
		if avg != expectedAvgs[i] {
			t.Errorf("Row %d: AVG = %f, want %f", i, avg, expectedAvgs[i])
		}
	}
}

func TestWindowFunction_COUNT_WindowFrame(t *testing.T) {
	// Test COUNT() with window frame
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE data (id INT, value INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []string{
		"INSERT INTO data VALUES (1, 10)",
		"INSERT INTO data VALUES (2, 20)",
		"INSERT INTO data VALUES (3, 30)",
		"INSERT INTO data VALUES (4, 40)",
	}
	for _, sql := range testData {
		_, err = exec.Execute(sql)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Count rows in frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
	result, err := exec.Execute("SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM data")
	if err != nil {
		t.Fatalf("Failed to execute window function query: %v", err)
	}

	if len(result.Rows) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(result.Rows))
	}

	// Expected counts: 1, 2, 2, 2
	expectedCounts := []int64{1, 2, 2, 2}
	for i, row := range result.Rows {
		count := row[1].Int()
		if count != expectedCounts[i] {
			t.Errorf("Row %d: COUNT = %d, want %d", i, count, expectedCounts[i])
		}
	}
}

func TestWindowFunction_MIN_MAX_WindowFrame(t *testing.T) {
	// Test MIN() and MAX() with window frame
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE data (id INT, value INT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []string{
		"INSERT INTO data VALUES (1, 50)",
		"INSERT INTO data VALUES (2, 30)",
		"INSERT INTO data VALUES (3, 70)",
		"INSERT INTO data VALUES (4, 20)",
	}
	for _, sql := range testData {
		_, err = exec.Execute(sql)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Running MIN
	result, err := exec.Execute("SELECT id, value, MIN(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM data")
	if err != nil {
		t.Fatalf("Failed to execute MIN window function: %v", err)
	}

	// Expected running MIN: 50, 30, 30, 20
	expectedMins := []int64{50, 30, 30, 20}
	for i, row := range result.Rows {
		minVal := row[2].Int()
		if minVal != expectedMins[i] {
			t.Errorf("Row %d: MIN = %d, want %d", i, minVal, expectedMins[i])
		}
	}
}
