// pkg/sql/parser/subquery_test.go
package parser

import (
	"testing"

	"tur/pkg/sql/lexer"
)

func TestParseScalarSubquery(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "scalar subquery in WHERE",
			input: "SELECT * FROM users WHERE id = (SELECT MAX(id) FROM users)",
		},
		{
			name:  "scalar subquery in SELECT",
			input: "SELECT id, (SELECT COUNT(*) FROM orders) FROM users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.input)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			if selectStmt.From == nil {
				t.Fatal("Expected FROM clause")
			}
		})
	}
}

func TestParseInSubquery(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectNot bool
	}{
		{
			name:      "IN subquery",
			input:     "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			expectNot: false,
		},
		{
			name:      "NOT IN subquery",
			input:     "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM blocked)",
			expectNot: true,
		},
		{
			name:      "IN value list",
			input:     "SELECT * FROM users WHERE id IN (1, 2, 3)",
			expectNot: false,
		},
		{
			name:      "NOT IN value list",
			input:     "SELECT * FROM users WHERE id NOT IN (4, 5, 6)",
			expectNot: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.input)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			if selectStmt.Where == nil {
				t.Fatal("Expected WHERE clause")
			}

			inExpr, ok := selectStmt.Where.(*InExpr)
			if !ok {
				t.Fatalf("Expected InExpr in WHERE, got %T", selectStmt.Where)
			}

			if inExpr.Not != tt.expectNot {
				t.Errorf("Expected Not=%v, got %v", tt.expectNot, inExpr.Not)
			}
		})
	}
}

func TestParseExistsSubquery(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectNot bool
	}{
		{
			name:      "EXISTS subquery",
			input:     "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
			expectNot: false,
		},
		{
			name:      "NOT EXISTS subquery",
			input:     "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM blocked WHERE blocked.user_id = users.id)",
			expectNot: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.input)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			if selectStmt.Where == nil {
				t.Fatal("Expected WHERE clause")
			}

			existsExpr, ok := selectStmt.Where.(*ExistsExpr)
			if !ok {
				t.Fatalf("Expected ExistsExpr in WHERE, got %T", selectStmt.Where)
			}

			if existsExpr.Not != tt.expectNot {
				t.Errorf("Expected Not=%v, got %v", tt.expectNot, existsExpr.Not)
			}

			if existsExpr.Subquery == nil {
				t.Error("Expected Subquery to be non-nil")
			}
		})
	}
}

func TestParseDerivedTable(t *testing.T) {
	tests := []struct {
		name  string
		input string
		alias string
	}{
		{
			name:  "derived table with alias",
			input: "SELECT * FROM (SELECT id, name FROM users) AS u",
			alias: "u",
		},
		{
			name:  "derived table in join",
			input: "SELECT * FROM users JOIN (SELECT user_id FROM orders) AS o ON users.id = o.user_id",
			alias: "o",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.input)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected SelectStmt, got %T", stmt)
			}

			if selectStmt.From == nil {
				t.Fatal("Expected FROM clause")
			}
		})
	}
}

func TestLexerInKeyword(t *testing.T) {
	input := "SELECT * FROM users WHERE id IN (1, 2, 3)"
	l := lexer.New(input)

	expectedTokens := []lexer.TokenType{
		lexer.SELECT,
		lexer.STAR,
		lexer.FROM,
		lexer.IDENT,
		lexer.WHERE,
		lexer.IDENT,
		lexer.IN_KW,
		lexer.LPAREN,
		lexer.INT,
		lexer.COMMA,
		lexer.INT,
		lexer.COMMA,
		lexer.INT,
		lexer.RPAREN,
		lexer.EOF,
	}

	for i, expected := range expectedTokens {
		tok := l.NextToken()
		if tok.Type != expected {
			t.Errorf("token[%d]: expected %s, got %s (%q)", i, expected, tok.Type, tok.Literal)
		}
	}
}
