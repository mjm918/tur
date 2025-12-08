// pkg/sql/parser/ast_test.go
package parser

import (
	"testing"
)

// TestWindowFunctionNode tests the window function AST node
func TestWindowFunctionNode(t *testing.T) {
	// Test window function with OVER clause
	wf := &WindowFunction{
		Function: &FunctionCall{
			Name: "ROW_NUMBER",
			Args: []Expression{},
		},
		Over: &WindowSpec{
			PartitionBy: []Expression{
				&ColumnRef{Name: "category"},
			},
			OrderBy: []OrderByExpr{
				{
					Expr:      &ColumnRef{Name: "price"},
					Direction: OrderAsc,
				},
			},
		},
	}

	// Verify it implements Expression interface
	var _ Expression = wf

	// Verify fields are set correctly
	if wf.Function == nil {
		t.Error("Function should not be nil")
	}
	if wf.Over == nil {
		t.Error("Over should not be nil")
	}
	if len(wf.Over.PartitionBy) != 1 {
		t.Errorf("Expected 1 PARTITION BY expression, got %d", len(wf.Over.PartitionBy))
	}
	if len(wf.Over.OrderBy) != 1 {
		t.Errorf("Expected 1 ORDER BY expression, got %d", len(wf.Over.OrderBy))
	}
}

// TestWindowSpec tests window specification
func TestWindowSpec(t *testing.T) {
	ws := &WindowSpec{
		PartitionBy: []Expression{
			&ColumnRef{Name: "category"},
		},
		OrderBy: []OrderByExpr{
			{
				Expr:      &ColumnRef{Name: "price"},
				Direction: OrderDesc,
			},
		},
		Frame: &WindowFrame{
			Mode:        FrameModeRows,
			StartBound:  &FrameBound{Type: FrameBoundCurrentRow},
			EndBound:    &FrameBound{Type: FrameBoundUnboundedFollowing},
		},
	}

	if len(ws.PartitionBy) != 1 {
		t.Errorf("Expected 1 PARTITION BY column, got %d", len(ws.PartitionBy))
	}
	if len(ws.OrderBy) != 1 {
		t.Errorf("Expected 1 ORDER BY column, got %d", len(ws.OrderBy))
	}
	if ws.Frame == nil {
		t.Error("Frame should not be nil")
	}
	if ws.Frame.Mode != FrameModeRows {
		t.Errorf("Expected ROWS frame mode, got %v", ws.Frame.Mode)
	}
}

// TestOrderByExpr tests ORDER BY expression
func TestOrderByExpr(t *testing.T) {
	tests := []struct {
		name      string
		direction OrderDirection
	}{
		{"Ascending", OrderAsc},
		{"Descending", OrderDesc},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orderBy := OrderByExpr{
				Expr:      &ColumnRef{Name: "price"},
				Direction: tt.direction,
			}

			if orderBy.Direction != tt.direction {
				t.Errorf("Expected direction %v, got %v", tt.direction, orderBy.Direction)
			}
		})
	}
}

// TestWindowFrame tests window frame specification
func TestWindowFrame(t *testing.T) {
	tests := []struct {
		name       string
		mode       FrameMode
		startType  FrameBoundType
		endType    FrameBoundType
	}{
		{
			name:      "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
			mode:      FrameModeRows,
			startType: FrameBoundCurrentRow,
			endType:   FrameBoundUnboundedFollowing,
		},
		{
			name:      "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
			mode:      FrameModeRange,
			startType: FrameBoundUnboundedPreceding,
			endType:   FrameBoundCurrentRow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := &WindowFrame{
				Mode:       tt.mode,
				StartBound: &FrameBound{Type: tt.startType},
				EndBound:   &FrameBound{Type: tt.endType},
			}

			if frame.Mode != tt.mode {
				t.Errorf("Expected mode %v, got %v", tt.mode, frame.Mode)
			}
			if frame.StartBound.Type != tt.startType {
				t.Errorf("Expected start bound %v, got %v", tt.startType, frame.StartBound.Type)
			}
			if frame.EndBound.Type != tt.endType {
				t.Errorf("Expected end bound %v, got %v", tt.endType, frame.EndBound.Type)
			}
		})
	}
}

// TestFunctionCall tests function call AST node
func TestFunctionCall(t *testing.T) {
	fc := &FunctionCall{
		Name: "COUNT",
		Args: []Expression{
			&ColumnRef{Name: "id"},
		},
	}

	// Verify it implements Expression interface
	var _ Expression = fc

	if fc.Name != "COUNT" {
		t.Errorf("Expected function name COUNT, got %s", fc.Name)
	}
	if len(fc.Args) != 1 {
		t.Errorf("Expected 1 argument, got %d", len(fc.Args))
	}
}
