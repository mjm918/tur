package parser

import (
	"testing"
)

// Tests for CASE expression parsing
// SQL syntax: CASE WHEN condition THEN result [WHEN ...] [ELSE result] END

func TestParser_CaseExpr_SimpleSearched(t *testing.T) {
	// Searched CASE: CASE WHEN condition THEN result END
	input := "SELECT CASE WHEN x > 10 THEN 'big' ELSE 'small' END FROM t"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected *SelectStmt, got %T", stmt)
	}

	if len(sel.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(sel.Columns))
	}

	caseExpr, ok := sel.Columns[0].Expr.(*CaseExpr)
	if !ok {
		t.Fatalf("Expected *CaseExpr, got %T", sel.Columns[0].Expr)
	}

	// Should have no operand (searched CASE)
	if caseExpr.Operand != nil {
		t.Error("Expected nil operand for searched CASE")
	}

	// Should have one WHEN clause
	if len(caseExpr.Whens) != 1 {
		t.Fatalf("Expected 1 WHEN clause, got %d", len(caseExpr.Whens))
	}

	// Should have ELSE clause
	if caseExpr.Else == nil {
		t.Error("Expected ELSE clause")
	}
}

func TestParser_CaseExpr_SimpleForm(t *testing.T) {
	// Simple CASE: CASE operand WHEN value THEN result END
	input := "SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' END FROM t"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)
	caseExpr, ok := sel.Columns[0].Expr.(*CaseExpr)
	if !ok {
		t.Fatalf("Expected *CaseExpr, got %T", sel.Columns[0].Expr)
	}

	// Should have operand (simple CASE)
	if caseExpr.Operand == nil {
		t.Error("Expected operand for simple CASE")
	}

	colRef, ok := caseExpr.Operand.(*ColumnRef)
	if !ok {
		t.Fatalf("Expected *ColumnRef operand, got %T", caseExpr.Operand)
	}
	if colRef.Name != "status" {
		t.Errorf("Expected operand column 'status', got %q", colRef.Name)
	}

	// Should have two WHEN clauses
	if len(caseExpr.Whens) != 2 {
		t.Fatalf("Expected 2 WHEN clauses, got %d", len(caseExpr.Whens))
	}

	// No ELSE clause
	if caseExpr.Else != nil {
		t.Error("Expected no ELSE clause")
	}
}

func TestParser_CaseExpr_MultipleWhens(t *testing.T) {
	input := "SELECT CASE WHEN x = 1 THEN 'one' WHEN x = 2 THEN 'two' WHEN x = 3 THEN 'three' ELSE 'other' END FROM t"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)
	caseExpr := sel.Columns[0].Expr.(*CaseExpr)

	if len(caseExpr.Whens) != 3 {
		t.Fatalf("Expected 3 WHEN clauses, got %d", len(caseExpr.Whens))
	}

	if caseExpr.Else == nil {
		t.Error("Expected ELSE clause")
	}
}

func TestParser_CaseExpr_InUpdate(t *testing.T) {
	// CASE in UPDATE statement - the main use case for bulk updates
	input := "UPDATE products SET price = CASE WHEN category = 'electronics' THEN price * 1.1 WHEN category = 'books' THEN price * 1.05 ELSE price END WHERE id IN (1, 2, 3)"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	update, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("Expected *UpdateStmt, got %T", stmt)
	}

	if len(update.Assignments) != 1 {
		t.Fatalf("Expected 1 assignment, got %d", len(update.Assignments))
	}

	caseExpr, ok := update.Assignments[0].Value.(*CaseExpr)
	if !ok {
		t.Fatalf("Expected *CaseExpr in assignment, got %T", update.Assignments[0].Value)
	}

	if len(caseExpr.Whens) != 2 {
		t.Errorf("Expected 2 WHEN clauses, got %d", len(caseExpr.Whens))
	}
}

func TestParser_CaseExpr_NestedCase(t *testing.T) {
	// Nested CASE expressions
	input := "SELECT CASE WHEN x > 0 THEN CASE WHEN x > 10 THEN 'big' ELSE 'medium' END ELSE 'negative' END FROM t"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)
	outerCase := sel.Columns[0].Expr.(*CaseExpr)

	if len(outerCase.Whens) != 1 {
		t.Fatalf("Expected 1 outer WHEN clause, got %d", len(outerCase.Whens))
	}

	// The THEN of the first WHEN should be another CASE
	innerCase, ok := outerCase.Whens[0].Then.(*CaseExpr)
	if !ok {
		t.Fatalf("Expected nested *CaseExpr, got %T", outerCase.Whens[0].Then)
	}

	if len(innerCase.Whens) != 1 {
		t.Errorf("Expected 1 inner WHEN clause, got %d", len(innerCase.Whens))
	}
}

func TestParser_CaseExpr_WithAlias(t *testing.T) {
	input := "SELECT CASE WHEN active THEN 'yes' ELSE 'no' END AS status FROM users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)
	if sel.Columns[0].Alias != "status" {
		t.Errorf("Expected alias 'status', got %q", sel.Columns[0].Alias)
	}

	_, ok := sel.Columns[0].Expr.(*CaseExpr)
	if !ok {
		t.Fatalf("Expected *CaseExpr, got %T", sel.Columns[0].Expr)
	}
}

func TestParser_CaseExpr_InWhereClause(t *testing.T) {
	input := "SELECT * FROM t WHERE CASE WHEN type = 'a' THEN value > 10 ELSE value > 20 END"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)
	_, ok := sel.Where.(*CaseExpr)
	if !ok {
		t.Fatalf("Expected *CaseExpr in WHERE, got %T", sel.Where)
	}
}
