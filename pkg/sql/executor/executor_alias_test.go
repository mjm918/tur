package executor

import (
	"testing"
)

// TestExecutor_ColumnAlias tests that column aliases are properly returned in result columns
func TestExecutor_ColumnAlias(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	tests := []struct {
		name            string
		sql             string
		expectedColumns []string
	}{
		{
			name:            "function with alias",
			sql:             "SELECT NOW() AS t",
			expectedColumns: []string{"t"},
		},
		{
			name:            "function with alias using AS keyword",
			sql:             "SELECT NOW() AS current_time",
			expectedColumns: []string{"current_time"},
		},
		{
			name:            "multiple functions with aliases",
			sql:             "SELECT NOW() AS t1, NOW() AS t2",
			expectedColumns: []string{"t1", "t2"},
		},
		{
			name:            "literal with alias",
			sql:             "SELECT 1 AS one",
			expectedColumns: []string{"one"},
		},
		{
			name:            "expression with alias",
			sql:             "SELECT 1 + 2 AS result",
			expectedColumns: []string{"result"},
		},
		{
			name:            "mixed columns and aliases",
			sql:             "SELECT 1 AS a, 2 AS b, 3 AS c",
			expectedColumns: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.sql)
			if err != nil {
				t.Fatalf("Execute(%q): %v", tt.sql, err)
			}

			if len(result.Columns) != len(tt.expectedColumns) {
				t.Fatalf("got %d columns %v, want %d columns %v",
					len(result.Columns), result.Columns,
					len(tt.expectedColumns), tt.expectedColumns)
			}

			for i, want := range tt.expectedColumns {
				if result.Columns[i] != want {
					t.Errorf("column[%d] = %q, want %q (all columns: %v)",
						i, result.Columns[i], want, result.Columns)
				}
			}
		})
	}
}

// TestExecutor_ColumnAliasWithTable tests aliases work with table columns too
func TestExecutor_ColumnAliasWithTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table
	_, err := exec.Execute("CREATE TABLE items (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	_, err = exec.Execute("INSERT INTO items VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	tests := []struct {
		name            string
		sql             string
		expectedColumns []string
	}{
		{
			name:            "column with alias",
			sql:             "SELECT id AS item_id FROM items",
			expectedColumns: []string{"item_id"},
		},
		{
			name:            "multiple columns with aliases",
			sql:             "SELECT id AS item_id, name AS item_name FROM items",
			expectedColumns: []string{"item_id", "item_name"},
		},
		{
			name:            "mixed alias and no alias",
			sql:             "SELECT id AS item_id, name FROM items",
			expectedColumns: []string{"item_id", "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.sql)
			if err != nil {
				t.Fatalf("Execute(%q): %v", tt.sql, err)
			}

			if len(result.Columns) != len(tt.expectedColumns) {
				t.Fatalf("got %d columns %v, want %d columns %v",
					len(result.Columns), result.Columns,
					len(tt.expectedColumns), tt.expectedColumns)
			}

			for i, want := range tt.expectedColumns {
				if result.Columns[i] != want {
					t.Errorf("column[%d] = %q, want %q (all columns: %v)",
						i, result.Columns[i], want, result.Columns)
				}
			}
		})
	}
}
