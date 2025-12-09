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

func TestParser_WindowFrame_RowsBetween(t *testing.T) {
	sql := "SELECT SUM(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM data"

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

	if winFunc.Over.Frame == nil {
		t.Fatal("Expected Frame clause, got nil")
	}

	frame := winFunc.Over.Frame
	if frame.Mode != FrameModeRows {
		t.Errorf("Expected FrameModeRows, got %v", frame.Mode)
	}

	if frame.StartBound == nil {
		t.Fatal("Expected StartBound, got nil")
	}
	if frame.StartBound.Type != FrameBoundUnboundedPreceding {
		t.Errorf("Expected FrameBoundUnboundedPreceding, got %v", frame.StartBound.Type)
	}

	if frame.EndBound == nil {
		t.Fatal("Expected EndBound, got nil")
	}
	if frame.EndBound.Type != FrameBoundCurrentRow {
		t.Errorf("Expected FrameBoundCurrentRow, got %v", frame.EndBound.Type)
	}
}

func TestParser_WindowFrame_RowsWithOffset(t *testing.T) {
	sql := "SELECT AVG(value) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM data"

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

	if winFunc.Over.Frame == nil {
		t.Fatal("Expected Frame clause, got nil")
	}

	frame := winFunc.Over.Frame
	if frame.Mode != FrameModeRows {
		t.Errorf("Expected FrameModeRows, got %v", frame.Mode)
	}

	// Check start bound: 2 PRECEDING
	if frame.StartBound.Type != FrameBoundPreceding {
		t.Errorf("Expected FrameBoundPreceding, got %v", frame.StartBound.Type)
	}
	if frame.StartBound.Offset == nil {
		t.Fatal("Expected offset for StartBound")
	}
	startOffset, ok := frame.StartBound.Offset.(*Literal)
	if !ok {
		t.Fatalf("Expected Literal offset, got %T", frame.StartBound.Offset)
	}
	if startOffset.Value.Int() != 2 {
		t.Errorf("Expected offset 2, got %d", startOffset.Value.Int())
	}

	// Check end bound: 2 FOLLOWING
	if frame.EndBound.Type != FrameBoundFollowing {
		t.Errorf("Expected FrameBoundFollowing, got %v", frame.EndBound.Type)
	}
	if frame.EndBound.Offset == nil {
		t.Fatal("Expected offset for EndBound")
	}
	endOffset, ok := frame.EndBound.Offset.(*Literal)
	if !ok {
		t.Fatalf("Expected Literal offset, got %T", frame.EndBound.Offset)
	}
	if endOffset.Value.Int() != 2 {
		t.Errorf("Expected offset 2, got %d", endOffset.Value.Int())
	}
}

func TestParser_WindowFrame_RangeBetween(t *testing.T) {
	sql := "SELECT SUM(value) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM data"

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

	if winFunc.Over.Frame == nil {
		t.Fatal("Expected Frame clause, got nil")
	}

	frame := winFunc.Over.Frame
	if frame.Mode != FrameModeRange {
		t.Errorf("Expected FrameModeRange, got %v", frame.Mode)
	}

	if frame.StartBound.Type != FrameBoundUnboundedPreceding {
		t.Errorf("Expected FrameBoundUnboundedPreceding, got %v", frame.StartBound.Type)
	}

	if frame.EndBound.Type != FrameBoundUnboundedFollowing {
		t.Errorf("Expected FrameBoundUnboundedFollowing, got %v", frame.EndBound.Type)
	}
}
