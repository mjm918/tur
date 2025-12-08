package parser

import (
	"testing"
	"tur/pkg/sql/lexer"
)

func TestParser_Joins(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		verify func(t *testing.T, stmt *SelectStmt)
	}{
		{
			name:  "Explicit INNER JOIN",
			input: "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id",
			verify: func(t *testing.T, stmt *SelectStmt) {
				join, ok := stmt.From.(*Join)
				if !ok {
					t.Fatalf("Expected *Join, got %T", stmt.From)
				}
				if join.Type != JoinInner {
					t.Errorf("Type = %v, want JoinInner", join.Type)
				}

				left, ok := join.Left.(*Table)
				if !ok || left.Name != "t1" {
					t.Errorf("Left = %v, want t1", join.Left)
				}

				right, ok := join.Right.(*Table)
				if !ok || right.Name != "t2" {
					t.Errorf("Right = %v, want t2", join.Right)
				}

				// Check condition t1.id = t2.id
				cond, ok := join.Condition.(*BinaryExpr)
				if !ok || cond.Op != lexer.EQ {
					t.Error("Condition invalid")
				}
			},
		},
		{
			name:  "Basic JOIN (implicit Inner)",
			input: "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
			verify: func(t *testing.T, stmt *SelectStmt) {
				join, ok := stmt.From.(*Join)
				if !ok {
					t.Fatalf("Expected *Join, got %T", stmt.From)
				}
				if join.Type != JoinInner {
					t.Errorf("Type = %v, want JoinInner", join.Type)
				}
			},
		},
		{
			name:  "LEFT JOIN",
			input: "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = 2",
			verify: func(t *testing.T, stmt *SelectStmt) {
				join, ok := stmt.From.(*Join)
				if !ok {
					t.Fatalf("Expected *Join, got %T", stmt.From)
				}
				if join.Type != JoinLeft {
					t.Errorf("Type = %v, want JoinLeft", join.Type)
				}
			},
		},
		{
			name:  "LEFT OUTER JOIN",
			input: "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = 2",
			verify: func(t *testing.T, stmt *SelectStmt) {
				join, ok := stmt.From.(*Join)
				if !ok {
					t.Fatalf("Expected *Join, got %T", stmt.From)
				}
				if join.Type != JoinLeft {
					t.Errorf("Type = %v, want JoinLeft", join.Type)
				}
			},
		},
		{
			name:  "Multi-Way Join",
			input: "SELECT * FROM A JOIN B ON A.id = B.id JOIN C ON B.id = C.id",
			verify: func(t *testing.T, stmt *SelectStmt) {
				// (A JOIN B) JOIN C
				joinC, ok := stmt.From.(*Join) // The top join
				if !ok {
					t.Fatal("Top level not a join")
				}

				// Right should be C
				c, ok := joinC.Right.(*Table)
				if !ok || c.Name != "C" {
					t.Error("Top right should be C")
				}

				// Left should be (A JOIN B)
				joinB, ok := joinC.Left.(*Join)
				if !ok {
					t.Fatal("Top left not a join")
				}

				// Right of inner join should be B
				b, ok := joinB.Right.(*Table)
				if !ok || b.Name != "B" {
					t.Error("Inner right should be B")
				}

				// Left of inner join should be A
				a, ok := joinB.Left.(*Table)
				if !ok || a.Name != "A" {
					t.Error("Inner left should be A")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.input)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			sel, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			tt.verify(t, sel)
		})
	}
}
