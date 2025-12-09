package parser

import (
	"testing"
)

func TestParser_WindowFunction_ROW_NUMBER(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER () FROM employees"

	p := New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	if len(selectStmt.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(selectStmt.Columns))
	}

	winFunc, ok := selectStmt.Columns[0].Expr.(*WindowFunction)
	if !ok {
		t.Fatalf("Expected WindowFunction, got %T", selectStmt.Columns[0].Expr)
	}

	funcCall, ok := winFunc.Function.(*FunctionCall)
	if !ok {
		t.Fatalf("Expected FunctionCall inside WindowFunction, got %T", winFunc.Function)
	}

	if funcCall.Name != "ROW_NUMBER" {
		t.Errorf("Expected function name ROW_NUMBER, got %s", funcCall.Name)
	}

	if winFunc.Over == nil {
		t.Fatal("Expected Over clause, got nil")
	}
}

func TestParser_WindowFunction_WithPartitionBy(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER (PARTITION BY department) FROM employees"

	p := New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	if len(selectStmt.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(selectStmt.Columns))
	}

	winFunc, ok := selectStmt.Columns[0].Expr.(*WindowFunction)
	if !ok {
		t.Fatalf("Expected WindowFunction, got %T", selectStmt.Columns[0].Expr)
	}

	if winFunc.Over == nil {
		t.Fatal("Expected Over clause, got nil")
	}

	if len(winFunc.Over.PartitionBy) != 1 {
		t.Fatalf("Expected 1 partition by column, got %d", len(winFunc.Over.PartitionBy))
	}

	col, ok := winFunc.Over.PartitionBy[0].(*ColumnRef)
	if !ok {
		t.Fatalf("Expected ColumnRef in PartitionBy, got %T", winFunc.Over.PartitionBy[0])
	}

	if col.Name != "department" {
		t.Errorf("Expected partition by column 'department', got '%s'", col.Name)
	}
}

func TestParser_WindowFunction_WithPartitionByAndOrderBy(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees"

	p := New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	winFunc, ok := selectStmt.Columns[0].Expr.(*WindowFunction)
	if !ok {
		t.Fatalf("Expected WindowFunction, got %T", selectStmt.Columns[0].Expr)
	}

	if len(winFunc.Over.PartitionBy) != 1 {
		t.Fatalf("Expected 1 partition by column, got %d", len(winFunc.Over.PartitionBy))
	}

	if len(winFunc.Over.OrderBy) != 1 {
		t.Fatalf("Expected 1 order by expression, got %d", len(winFunc.Over.OrderBy))
	}

	orderExpr := winFunc.Over.OrderBy[0]
	col, ok := orderExpr.Expr.(*ColumnRef)
	if !ok {
		t.Fatalf("Expected ColumnRef in OrderBy, got %T", orderExpr.Expr)
	}

	if col.Name != "salary" {
		t.Errorf("Expected order by column 'salary', got '%s'", col.Name)
	}

	if orderExpr.Direction != OrderDesc {
		t.Error("Expected DESC ordering")
	}
}

func TestParser_WindowFunction_OrderByOnly(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM employees"

	p := New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}

	winFunc, ok := selectStmt.Columns[0].Expr.(*WindowFunction)
	if !ok {
		t.Fatalf("Expected WindowFunction, got %T", selectStmt.Columns[0].Expr)
	}

	if len(winFunc.Over.PartitionBy) != 0 {
		t.Fatalf("Expected 0 partition by columns, got %d", len(winFunc.Over.PartitionBy))
	}

	if len(winFunc.Over.OrderBy) != 1 {
		t.Fatalf("Expected 1 order by expression, got %d", len(winFunc.Over.OrderBy))
	}
}
