package executor

import (
	"path/filepath"
	"testing"
	"tur/pkg/pager"
)

func TestExecutor_RecursiveCTE_Counter(t *testing.T) {
	// Setup executor
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	e := New(p)
	defer e.Close()

	// Create dummy table for anchor
	if _, err := e.Execute("CREATE TABLE dummy (i INT)"); err != nil {
		t.Fatalf("Failed to create dummy table: %v", err)
	}
	if _, err := e.Execute("INSERT INTO dummy VALUES (1)"); err != nil {
		t.Fatalf("Failed to insert into dummy: %v", err)
	}

	sql := `
	WITH RECURSIVE cnt(x) AS (
		SELECT 1 FROM dummy
		UNION ALL
		SELECT x+1 FROM cnt WHERE x < 5
	)
	SELECT x FROM cnt
	`

	// Parse manually to ensure no parser errors first?
	// But Execute handles parsing.
	res, err := e.Execute(sql)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	if len(res.Rows) != 5 {
		t.Fatalf("Expected 5 rows, got %d", len(res.Rows))
	}

	for i, row := range res.Rows {
		val := row[0].Int()
		expected := int64(i + 1)
		if val != expected {
			t.Errorf("Row %d: got %d, want %d", i, val, expected)
		}
	}
}
