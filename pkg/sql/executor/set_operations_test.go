package executor

import (
	"testing"
)

func TestExecutor_UnionAll_Simple(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create two tables
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("Create users: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE admins (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("Create admins: %v", err)
	}

	// Insert data
	exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	exec.Execute("INSERT INTO admins VALUES (10, 'Admin1')")
	exec.Execute("INSERT INTO admins VALUES (20, 'Admin2')")

	// Execute UNION ALL
	result, err := exec.Execute("SELECT id, name FROM users UNION ALL SELECT id, name FROM admins")
	if err != nil {
		t.Fatalf("Execute UNION ALL: %v", err)
	}

	// Should have 4 rows (all rows from both tables)
	if len(result.Rows) != 4 {
		t.Errorf("Rows = %d, want 4", len(result.Rows))
	}

	// Check that we have all expected IDs
	ids := make(map[int64]bool)
	for _, row := range result.Rows {
		ids[row[0].Int()] = true
	}

	expectedIDs := []int64{1, 2, 10, 20}
	for _, id := range expectedIDs {
		if !ids[id] {
			t.Errorf("Missing ID %d in result", id)
		}
	}
}

func TestExecutor_UnionAll_WithDuplicates(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create two tables with overlapping data
	_, err := exec.Execute("CREATE TABLE t1 (x INT)")
	if err != nil {
		t.Fatalf("Create t1: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE t2 (y INT)")
	if err != nil {
		t.Fatalf("Create t2: %v", err)
	}

	// Insert overlapping data
	exec.Execute("INSERT INTO t1 VALUES (1)")
	exec.Execute("INSERT INTO t1 VALUES (2)")
	exec.Execute("INSERT INTO t2 VALUES (2)")
	exec.Execute("INSERT INTO t2 VALUES (3)")

	// Execute UNION ALL - should preserve duplicates
	result, err := exec.Execute("SELECT x FROM t1 UNION ALL SELECT y FROM t2")
	if err != nil {
		t.Fatalf("Execute UNION ALL: %v", err)
	}

	// Should have 4 rows (duplicates preserved)
	if len(result.Rows) != 4 {
		t.Errorf("Rows = %d, want 4 (UNION ALL preserves duplicates)", len(result.Rows))
	}
}

func TestExecutor_Union_Dedup(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create two tables with overlapping data
	_, err := exec.Execute("CREATE TABLE t1 (x INT)")
	if err != nil {
		t.Fatalf("Create t1: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE t2 (y INT)")
	if err != nil {
		t.Fatalf("Create t2: %v", err)
	}

	// Insert overlapping data: t1 = {1, 2}, t2 = {2, 3}
	exec.Execute("INSERT INTO t1 VALUES (1)")
	exec.Execute("INSERT INTO t1 VALUES (2)")
	exec.Execute("INSERT INTO t2 VALUES (2)")
	exec.Execute("INSERT INTO t2 VALUES (3)")

	// Execute UNION - should deduplicate
	result, err := exec.Execute("SELECT x FROM t1 UNION SELECT y FROM t2")
	if err != nil {
		t.Fatalf("Execute UNION: %v", err)
	}

	// Should have 3 rows (duplicates removed: {1, 2, 3})
	if len(result.Rows) != 3 {
		t.Errorf("Rows = %d, want 3 (UNION removes duplicates)", len(result.Rows))
	}

	// Verify we have exactly {1, 2, 3}
	vals := make(map[int64]bool)
	for _, row := range result.Rows {
		vals[row[0].Int()] = true
	}
	if !vals[1] || !vals[2] || !vals[3] {
		t.Errorf("Missing expected values, got %v", vals)
	}
}

func TestExecutor_Intersect(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create two tables
	_, err := exec.Execute("CREATE TABLE t1 (x INT)")
	if err != nil {
		t.Fatalf("Create t1: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE t2 (y INT)")
	if err != nil {
		t.Fatalf("Create t2: %v", err)
	}

	// Insert overlapping data: t1 = {1, 2, 3}, t2 = {2, 3, 4}
	exec.Execute("INSERT INTO t1 VALUES (1)")
	exec.Execute("INSERT INTO t1 VALUES (2)")
	exec.Execute("INSERT INTO t1 VALUES (3)")
	exec.Execute("INSERT INTO t2 VALUES (2)")
	exec.Execute("INSERT INTO t2 VALUES (3)")
	exec.Execute("INSERT INTO t2 VALUES (4)")

	// Execute INTERSECT - should return common rows {2, 3}
	result, err := exec.Execute("SELECT x FROM t1 INTERSECT SELECT y FROM t2")
	if err != nil {
		t.Fatalf("Execute INTERSECT: %v", err)
	}

	// Should have 2 rows
	if len(result.Rows) != 2 {
		t.Errorf("Rows = %d, want 2", len(result.Rows))
	}

	// Verify we have {2, 3}
	vals := make(map[int64]bool)
	for _, row := range result.Rows {
		vals[row[0].Int()] = true
	}
	if !vals[2] || !vals[3] {
		t.Errorf("Missing expected values {2, 3}, got %v", vals)
	}
	if vals[1] || vals[4] {
		t.Errorf("Unexpected values {1, 4} in result, got %v", vals)
	}
}

func TestExecutor_IntersectAll(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create two tables
	_, err := exec.Execute("CREATE TABLE t1 (x INT)")
	if err != nil {
		t.Fatalf("Create t1: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE t2 (y INT)")
	if err != nil {
		t.Fatalf("Create t2: %v", err)
	}

	// Insert with duplicates: t1 = {1, 2, 2}, t2 = {2, 2, 2}
	exec.Execute("INSERT INTO t1 VALUES (1)")
	exec.Execute("INSERT INTO t1 VALUES (2)")
	exec.Execute("INSERT INTO t1 VALUES (2)")
	exec.Execute("INSERT INTO t2 VALUES (2)")
	exec.Execute("INSERT INTO t2 VALUES (2)")
	exec.Execute("INSERT INTO t2 VALUES (2)")

	// Execute INTERSECT ALL - should return min count of duplicates
	// Common value 2 appears 2 times in t1 and 3 times in t2, so result has 2 copies
	result, err := exec.Execute("SELECT x FROM t1 INTERSECT ALL SELECT y FROM t2")
	if err != nil {
		t.Fatalf("Execute INTERSECT ALL: %v", err)
	}

	// Should have 2 rows (both 2s from t1 match)
	if len(result.Rows) != 2 {
		t.Errorf("Rows = %d, want 2 (min of duplicate counts)", len(result.Rows))
	}
}

func TestExecutor_Except(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create two tables
	_, err := exec.Execute("CREATE TABLE t1 (x INT)")
	if err != nil {
		t.Fatalf("Create t1: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE t2 (y INT)")
	if err != nil {
		t.Fatalf("Create t2: %v", err)
	}

	// Insert: t1 = {1, 2, 3}, t2 = {2, 3}
	exec.Execute("INSERT INTO t1 VALUES (1)")
	exec.Execute("INSERT INTO t1 VALUES (2)")
	exec.Execute("INSERT INTO t1 VALUES (3)")
	exec.Execute("INSERT INTO t2 VALUES (2)")
	exec.Execute("INSERT INTO t2 VALUES (3)")

	// Execute EXCEPT - should return t1 - t2 = {1}
	result, err := exec.Execute("SELECT x FROM t1 EXCEPT SELECT y FROM t2")
	if err != nil {
		t.Fatalf("Execute EXCEPT: %v", err)
	}

	// Should have 1 row
	if len(result.Rows) != 1 {
		t.Errorf("Rows = %d, want 1", len(result.Rows))
	}

	// Verify we have {1}
	if result.Rows[0][0].Int() != 1 {
		t.Errorf("Expected 1, got %d", result.Rows[0][0].Int())
	}
}

func TestExecutor_ExceptAll(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create two tables
	_, err := exec.Execute("CREATE TABLE t1 (x INT)")
	if err != nil {
		t.Fatalf("Create t1: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE t2 (y INT)")
	if err != nil {
		t.Fatalf("Create t2: %v", err)
	}

	// Insert with duplicates: t1 = {1, 2, 2, 2}, t2 = {2}
	exec.Execute("INSERT INTO t1 VALUES (1)")
	exec.Execute("INSERT INTO t1 VALUES (2)")
	exec.Execute("INSERT INTO t1 VALUES (2)")
	exec.Execute("INSERT INTO t1 VALUES (2)")
	exec.Execute("INSERT INTO t2 VALUES (2)")

	// Execute EXCEPT ALL - should remove one 2 for each 2 in t2
	// Result: {1, 2, 2} (one 2 removed)
	result, err := exec.Execute("SELECT x FROM t1 EXCEPT ALL SELECT y FROM t2")
	if err != nil {
		t.Fatalf("Execute EXCEPT ALL: %v", err)
	}

	// Should have 3 rows
	if len(result.Rows) != 3 {
		t.Errorf("Rows = %d, want 3", len(result.Rows))
	}

	// Count: should have 1 one and 2 twos
	count1, count2 := 0, 0
	for _, row := range result.Rows {
		v := row[0].Int()
		if v == 1 {
			count1++
		} else if v == 2 {
			count2++
		}
	}

	if count1 != 1 {
		t.Errorf("count of 1s = %d, want 1", count1)
	}
	if count2 != 2 {
		t.Errorf("count of 2s = %d, want 2", count2)
	}
}
