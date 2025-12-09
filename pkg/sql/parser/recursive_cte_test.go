package parser

import (
	"testing"
)

func TestParser_RecursiveCTE(t *testing.T) {
	input := `
	WITH RECURSIVE cnt(x) AS (
		SELECT 1 FROM dummy
		UNION ALL
		SELECT x+1 FROM cnt WHERE x < 10
	)
	SELECT x FROM cnt`
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected *SelectStmt, got %T", stmt)
	}

	if sel.With == nil {
		t.Fatal("Expected WITH clause")
	}

	if !sel.With.Recursive {
		t.Error("Expected RECURSIVE to be true")
	}

	if len(sel.With.CTEs) != 1 {
		t.Fatalf("Expected 1 CTE, got %d", len(sel.With.CTEs))
	}

	cte := sel.With.CTEs[0]
	if cte.Name != "cnt" {
		t.Errorf("CTE name = %q, want 'cnt'", cte.Name)
	}

	// Check that Query is a SetOperation
	setOp, ok := cte.Query.(*SetOperation)
	if !ok {
		t.Fatalf("CTE Query type = %T, want *SetOperation", cte.Query)
	}

	if setOp.Operator != SetOpUnion {
		t.Errorf("Operator = %v, want Union", setOp.Operator)
	}

	if !setOp.All {
		t.Error("All = false, want true (UNION ALL)")
	}

	// Check left side (ANCHOR)
	left := setOp.Left
	if len(left.Columns) != 1 {
		t.Errorf("Left columns count = %d, want 1", len(left.Columns))
	}

	// Check right side (RECURSIVE)
	right := setOp.Right
	if len(right.Columns) != 1 {
		t.Errorf("Right columns count = %d, want 1", len(right.Columns))
	}
}
