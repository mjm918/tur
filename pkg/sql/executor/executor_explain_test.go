package executor

import (
	"strings"
	"testing"
)

func TestExecutor_Explain_Select(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table first
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Now EXPLAIN a SELECT
	result, err := exec.Execute("EXPLAIN SELECT * FROM users")
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}

	// EXPLAIN should return bytecode information
	// SQLite format: addr, opcode, p1, p2, p3, p4, p5, comment
	expectedColumns := []string{"addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment"}
	if len(result.Columns) != len(expectedColumns) {
		t.Fatalf("Columns count = %d, want %d", len(result.Columns), len(expectedColumns))
	}

	for i, want := range expectedColumns {
		if result.Columns[i] != want {
			t.Errorf("Column[%d] = %q, want %q", i, result.Columns[i], want)
		}
	}

	// Should have at least some bytecode rows
	if len(result.Rows) < 2 {
		t.Errorf("Expected at least 2 bytecode rows, got %d", len(result.Rows))
	}

	// Check that we have Init and Halt opcodes at minimum
	hasInit := false
	hasHalt := false
	for _, row := range result.Rows {
		opcode := row[1].Text() // opcode is at index 1
		if opcode == "Init" {
			hasInit = true
		}
		if opcode == "Halt" {
			hasHalt = true
		}
	}

	if !hasInit {
		t.Error("Missing Init opcode in EXPLAIN output")
	}
	if !hasHalt {
		t.Error("Missing Halt opcode in EXPLAIN output")
	}
}

func TestExecutor_Explain_Insert(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table first
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Now EXPLAIN an INSERT - this falls back to query plan mode
	// because the VDBE compiler has limitations with INSERT
	result, err := exec.Execute("EXPLAIN INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}

	// Should have result rows
	if len(result.Rows) < 1 {
		t.Errorf("Expected at least 1 row, got %d", len(result.Rows))
	}

	// For INSERT that falls back to query plan, check detail mentions INSERT
	foundInsert := false
	for _, row := range result.Rows {
		// Check either opcode column (index 1) or detail column (index 3)
		for i := range row {
			text := row[i].Text()
			if strings.Contains(text, "Insert") || strings.Contains(text, "INSERT") {
				foundInsert = true
				break
			}
		}
		if foundInsert {
			break
		}
	}

	if !foundInsert {
		t.Error("Expected INSERT information in EXPLAIN output")
	}
}

func TestExecutor_ExplainQueryPlan_Select(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table first
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Now EXPLAIN QUERY PLAN
	result, err := exec.Execute("EXPLAIN QUERY PLAN SELECT * FROM users WHERE id > 10")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}

	// EXPLAIN QUERY PLAN should return plan information
	// SQLite format: id, parent, notused, detail
	expectedColumns := []string{"id", "parent", "notused", "detail"}
	if len(result.Columns) != len(expectedColumns) {
		t.Fatalf("Columns count = %d, want %d", len(result.Columns), len(expectedColumns))
	}

	for i, want := range expectedColumns {
		if result.Columns[i] != want {
			t.Errorf("Column[%d] = %q, want %q", i, result.Columns[i], want)
		}
	}

	// Should have at least one row describing the plan
	if len(result.Rows) < 1 {
		t.Fatal("Expected at least 1 plan row")
	}

	// Check that some row's detail column contains scan information
	foundScanOrTable := false
	for _, row := range result.Rows {
		detail := row[3].Text()
		if strings.Contains(detail, "SCAN") || strings.Contains(detail, "TABLE") {
			foundScanOrTable = true
			break
		}
	}
	if !foundScanOrTable {
		t.Errorf("Expected at least one row to contain SCAN or TABLE")
	}
}

func TestExecutor_ExplainQueryPlan_Join(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create tables
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE users: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE orders (id INT, user_id INT, amount REAL)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders: %v", err)
	}

	// EXPLAIN QUERY PLAN for a join
	result, err := exec.Execute("EXPLAIN QUERY PLAN SELECT * FROM users JOIN orders ON users.id = orders.user_id")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}

	// Should have plan rows
	if len(result.Rows) < 1 {
		t.Fatal("Expected at least 1 plan row for join")
	}

	// For a join, we expect multiple scan operations or a join mention
	foundJoin := false
	for _, row := range result.Rows {
		detail := row[3].Text()
		if strings.Contains(detail, "JOIN") || strings.Contains(detail, "NESTED") {
			foundJoin = true
			break
		}
	}

	// It's ok if join is implicit (just table scans), but we should see table info
	foundTables := 0
	for _, row := range result.Rows {
		detail := row[3].Text()
		if strings.Contains(detail, "users") || strings.Contains(detail, "orders") {
			foundTables++
		}
	}

	if foundTables == 0 && !foundJoin {
		t.Error("Expected plan to mention tables or join")
	}
}

func TestExecutor_ExplainAnalyze_Select(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table and insert some data
	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users VALUES (2, 'Bob')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Now EXPLAIN ANALYZE a SELECT
	result, err := exec.Execute("EXPLAIN ANALYZE SELECT * FROM users")
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE: %v", err)
	}

	// EXPLAIN ANALYZE should return analysis with runtime statistics
	// We expect columns like: operation, actual_rows, time, etc.
	if len(result.Columns) < 1 {
		t.Fatal("Expected at least 1 column in EXPLAIN ANALYZE output")
	}

	// Should have at least one row with analysis information
	if len(result.Rows) < 1 {
		t.Fatal("Expected at least 1 row in EXPLAIN ANALYZE output")
	}

	// The output should contain information about actual execution
	// Check for timing or row count information in the output
	foundExecutionInfo := false
	for _, row := range result.Rows {
		for i := range row {
			text := row[i].Text()
			// Look for indicators of actual execution like timing, row counts, or "actual"
			if strings.Contains(strings.ToLower(text), "actual") ||
				strings.Contains(strings.ToLower(text), "time") ||
				strings.Contains(strings.ToLower(text), "rows") {
				foundExecutionInfo = true
				break
			}
		}
		if foundExecutionInfo {
			break
		}
	}

	if !foundExecutionInfo {
		t.Error("Expected EXPLAIN ANALYZE output to contain execution information (timing, row counts, etc.)")
	}
}
