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

// TestCTE tests Common Table Expression AST node
func TestCTE(t *testing.T) {
	cte := &CTE{
		Name: "temp_results",
		Columns: []string{"id", "name"},
		Query: &SelectStmt{
			Columns: []SelectColumn{
				{Star: true},
			},
			From: "users",
		},
	}

	if cte.Name != "temp_results" {
		t.Errorf("Expected CTE name temp_results, got %s", cte.Name)
	}
	if len(cte.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cte.Columns))
	}
	if cte.Query == nil {
		t.Error("Query should not be nil")
	}
}

// TestWithClause tests WITH clause for CTEs
func TestWithClause(t *testing.T) {
	withClause := &WithClause{
		Recursive: false,
		CTEs: []CTE{
			{
				Name: "cte1",
				Query: &SelectStmt{
					Columns: []SelectColumn{{Star: true}},
					From:    "table1",
				},
			},
			{
				Name: "cte2",
				Query: &SelectStmt{
					Columns: []SelectColumn{{Star: true}},
					From:    "table2",
				},
			},
		},
	}

	if withClause.Recursive {
		t.Error("Expected non-recursive WITH clause")
	}
	if len(withClause.CTEs) != 2 {
		t.Errorf("Expected 2 CTEs, got %d", len(withClause.CTEs))
	}
}

// TestWithClause_Recursive tests recursive WITH clause
func TestWithClause_Recursive(t *testing.T) {
	withClause := &WithClause{
		Recursive: true,
		CTEs: []CTE{
			{
				Name: "recursive_cte",
				Query: &SelectStmt{
					Columns: []SelectColumn{{Star: true}},
					From:    "base_table",
				},
			},
		},
	}

	if !withClause.Recursive {
		t.Error("Expected recursive WITH clause")
	}
	if len(withClause.CTEs) != 1 {
		t.Errorf("Expected 1 CTE, got %d", len(withClause.CTEs))
	}
}

// TestSetOperation tests set operation AST node
func TestSetOperation(t *testing.T) {
	tests := []struct {
		name     string
		op       SetOperator
		all      bool
		expected string
	}{
		{"UNION", SetOpUnion, false, "UNION"},
		{"UNION ALL", SetOpUnion, true, "UNION ALL"},
		{"INTERSECT", SetOpIntersect, false, "INTERSECT"},
		{"EXCEPT", SetOpExcept, false, "EXCEPT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setOp := &SetOperation{
				Left: &SelectStmt{
					Columns: []SelectColumn{{Star: true}},
					From:    "table1",
				},
				Operator: tt.op,
				All:      tt.all,
				Right: &SelectStmt{
					Columns: []SelectColumn{{Star: true}},
					From:    "table2",
				},
			}

			// Verify it implements Statement interface
			var _ Statement = setOp

			if setOp.Operator != tt.op {
				t.Errorf("Expected operator %v, got %v", tt.op, setOp.Operator)
			}
			if setOp.All != tt.all {
				t.Errorf("Expected All=%v, got %v", tt.all, setOp.All)
			}
			if setOp.Left == nil {
				t.Error("Left should not be nil")
			}
			if setOp.Right == nil {
				t.Error("Right should not be nil")
			}
		})
	}
}

// TestSetOperator tests set operator enum
func TestSetOperator(t *testing.T) {
	tests := []struct {
		name     string
		operator SetOperator
	}{
		{"UNION", SetOpUnion},
		{"INTERSECT", SetOpIntersect},
		{"EXCEPT", SetOpExcept},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify the enum values exist and are distinct
			switch tt.operator {
			case SetOpUnion, SetOpIntersect, SetOpExcept:
				// Expected
			default:
				t.Errorf("Unexpected operator value: %v", tt.operator)
			}
		})
	}
}

// TestCreateTriggerStmt tests trigger creation AST node
func TestCreateTriggerStmt(t *testing.T) {
	trigger := &CreateTriggerStmt{
		TriggerName: "update_timestamp",
		Timing:      TriggerBefore,
		Event:       TriggerEventUpdate,
		TableName:   "users",
		Actions: []Statement{
			&InsertStmt{
				TableName: "audit_log",
				Values: [][]Expression{
					{
						&Literal{},
					},
				},
			},
		},
	}

	// Verify it implements Statement interface
	var _ Statement = trigger

	if trigger.TriggerName != "update_timestamp" {
		t.Errorf("Expected trigger name update_timestamp, got %s", trigger.TriggerName)
	}
	if trigger.Timing != TriggerBefore {
		t.Errorf("Expected BEFORE timing, got %v", trigger.Timing)
	}
	if trigger.Event != TriggerEventUpdate {
		t.Errorf("Expected UPDATE event, got %v", trigger.Event)
	}
	if trigger.TableName != "users" {
		t.Errorf("Expected table name users, got %s", trigger.TableName)
	}
	if len(trigger.Actions) != 1 {
		t.Errorf("Expected 1 action, got %d", len(trigger.Actions))
	}
}

// TestTriggerTiming tests trigger timing enum
func TestTriggerTiming(t *testing.T) {
	tests := []struct {
		name   string
		timing TriggerTiming
	}{
		{"BEFORE", TriggerBefore},
		{"AFTER", TriggerAfter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.timing {
			case TriggerBefore, TriggerAfter:
				// Expected
			default:
				t.Errorf("Unexpected timing value: %v", tt.timing)
			}
		})
	}
}

// TestTriggerEvent tests trigger event enum
func TestTriggerEvent(t *testing.T) {
	tests := []struct {
		name  string
		event TriggerEvent
	}{
		{"INSERT", TriggerEventInsert},
		{"UPDATE", TriggerEventUpdate},
		{"DELETE", TriggerEventDelete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch tt.event {
			case TriggerEventInsert, TriggerEventUpdate, TriggerEventDelete:
				// Expected
			default:
				t.Errorf("Unexpected event value: %v", tt.event)
			}
		})
	}
}

// TestDropTriggerStmt tests drop trigger AST node
func TestDropTriggerStmt(t *testing.T) {
	drop := &DropTriggerStmt{
		TriggerName: "my_trigger",
	}

	// Verify it implements Statement interface
	var _ Statement = drop

	if drop.TriggerName != "my_trigger" {
		t.Errorf("Expected trigger name my_trigger, got %s", drop.TriggerName)
	}
}
