package lexer

import "testing"

func TestLexer_SimpleTokens(t *testing.T) {
	input := "+-*/= < > (),;"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{PLUS, "+"},
		{MINUS, "-"},
		{STAR, "*"},
		{SLASH, "/"},
		{EQ, "="},
		{LT, "<"},
		{GT, ">"},
		{LPAREN, "("},
		{RPAREN, ")"},
		{COMMA, ","},
		{SEMICOLON, ";"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_ComparisonOperators(t *testing.T) {
	input := "= != <> < > <= >="
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{EQ, "="},
		{NEQ, "!="},
		{NEQ, "<>"},
		{LT, "<"},
		{GT, ">"},
		{LTE, "<="},
		{GTE, ">="},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_Keywords(t *testing.T) {
	input := "SELECT FROM WHERE INSERT INTO VALUES CREATE TABLE PRIMARY KEY NOT NULL AND OR"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{SELECT, "SELECT"},
		{FROM, "FROM"},
		{WHERE, "WHERE"},
		{INSERT, "INSERT"},
		{INTO, "INTO"},
		{VALUES, "VALUES"},
		{CREATE, "CREATE"},
		{TABLE, "TABLE"},
		{PRIMARY, "PRIMARY"},
		{KEY, "KEY"},
		{NOT, "NOT"},
		{NULL_KW, "NULL"},
		{AND, "AND"},
		{OR, "OR"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_CaseInsensitiveKeywords(t *testing.T) {
	input := "select SELECT Select sElEcT"
	l := New(input)

	for i := 0; i < 4; i++ {
		tok := l.NextToken()
		if tok.Type != SELECT {
			t.Errorf("token[%d]: type = %v, want SELECT", i, tok.Type)
		}
	}
}

func TestLexer_Identifiers(t *testing.T) {
	input := "users id user_name _private column1"
	expected := []string{"users", "id", "user_name", "_private", "column1"}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != IDENT {
			t.Errorf("token[%d]: type = %v, want IDENT", i, tok.Type)
		}
		if tok.Literal != exp {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp)
		}
	}
}

func TestLexer_Integers(t *testing.T) {
	input := "0 1 42 12345"
	expected := []string{"0", "1", "42", "12345"}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != INT {
			t.Errorf("token[%d]: type = %v, want INT", i, tok.Type)
		}
		if tok.Literal != exp {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp)
		}
	}
}

func TestLexer_Floats(t *testing.T) {
	input := "0.0 1.5 3.14159 .5 123."
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{FLOAT, "0.0"},
		{FLOAT, "1.5"},
		{FLOAT, "3.14159"},
		{FLOAT, ".5"},
		{FLOAT, "123."},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_Strings(t *testing.T) {
	input := `'hello' 'world' '' 'it''s escaped'`
	expected := []string{"hello", "world", "", "it's escaped"}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != STRING {
			t.Errorf("token[%d]: type = %v, want STRING", i, tok.Type)
		}
		if tok.Literal != exp {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp)
		}
	}
}

func TestLexer_SelectStatement(t *testing.T) {
	input := "SELECT id, name FROM users WHERE id = 1;"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{SELECT, "SELECT"},
		{IDENT, "id"},
		{COMMA, ","},
		{IDENT, "name"},
		{FROM, "FROM"},
		{IDENT, "users"},
		{WHERE, "WHERE"},
		{IDENT, "id"},
		{EQ, "="},
		{INT, "1"},
		{SEMICOLON, ";"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_CreateTableStatement(t *testing.T) {
	input := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL);"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{CREATE, "CREATE"},
		{TABLE, "TABLE"},
		{IDENT, "users"},
		{LPAREN, "("},
		{IDENT, "id"},
		{INT_TYPE, "INT"},
		{PRIMARY, "PRIMARY"},
		{KEY, "KEY"},
		{COMMA, ","},
		{IDENT, "name"},
		{TEXT_TYPE, "TEXT"},
		{NOT, "NOT"},
		{NULL_KW, "NULL"},
		{RPAREN, ")"},
		{SEMICOLON, ";"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_InsertStatement(t *testing.T) {
	input := "INSERT INTO users (id, name) VALUES (1, 'Alice');"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{INSERT, "INSERT"},
		{INTO, "INTO"},
		{IDENT, "users"},
		{LPAREN, "("},
		{IDENT, "id"},
		{COMMA, ","},
		{IDENT, "name"},
		{RPAREN, ")"},
		{VALUES, "VALUES"},
		{LPAREN, "("},
		{INT, "1"},
		{COMMA, ","},
		{STRING, "Alice"},
		{RPAREN, ")"},
		{SEMICOLON, ";"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_Whitespace(t *testing.T) {
	input := "  SELECT   \t\n  *  \r\n  FROM  \t  users  "
	expected := []TokenType{SELECT, STAR, FROM, IDENT, EOF}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp)
		}
	}
}

func TestLexer_NegativeNumbers(t *testing.T) {
	// Negative numbers are handled by parser (MINUS + INT)
	input := "-42"
	l := New(input)

	tok := l.NextToken()
	if tok.Type != MINUS {
		t.Errorf("expected MINUS, got %v", tok.Type)
	}

	tok = l.NextToken()
	if tok.Type != INT || tok.Literal != "42" {
		t.Errorf("expected INT '42', got %v %q", tok.Type, tok.Literal)
	}
}

func TestLexer_Position(t *testing.T) {
	input := "SELECT * FROM"
	l := New(input)

	tok := l.NextToken() // SELECT
	if tok.Pos != 0 {
		t.Errorf("SELECT pos = %d, want 0", tok.Pos)
	}

	tok = l.NextToken() // *
	if tok.Pos != 7 {
		t.Errorf("* pos = %d, want 7", tok.Pos)
	}

	tok = l.NextToken() // FROM
	if tok.Pos != 9 {
		t.Errorf("FROM pos = %d, want 9", tok.Pos)
	}
}

func TestLexer_GroupByHavingKeywords(t *testing.T) {
	input := "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{SELECT, "SELECT"},
		{IDENT, "department"},
		{COMMA, ","},
		{IDENT, "COUNT"},
		{LPAREN, "("},
		{STAR, "*"},
		{RPAREN, ")"},
		{FROM, "FROM"},
		{IDENT, "employees"},
		{GROUP, "GROUP"},
		{BY, "BY"},
		{IDENT, "department"},
		{HAVING, "HAVING"},
		{IDENT, "COUNT"},
		{LPAREN, "("},
		{STAR, "*"},
		{RPAREN, ")"},
		{GT, ">"},
		{INT, "5"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_WindowFunctionKeywords(t *testing.T) {
	input := "LAG(value) OVER (PARTITION BY category ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{IDENT, "LAG"},
		{LPAREN, "("},
		{IDENT, "value"},
		{RPAREN, ")"},
		{OVER, "OVER"},
		{LPAREN, "("},
		{PARTITION, "PARTITION"},
		{BY, "BY"},
		{IDENT, "category"},
		{ORDER, "ORDER"},
		{BY, "BY"},
		{IDENT, "id"},
		{ROWS, "ROWS"},
		{BETWEEN, "BETWEEN"},
		{UNBOUNDED, "UNBOUNDED"},
		{PRECEDING, "PRECEDING"},
		{AND, "AND"},
		{CURRENT, "CURRENT"},
		{ROW, "ROW"},
		{RPAREN, ")"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v (literal=%q)", i, tok.Type, exp.typ, tok.Literal)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_WindowFunctionKeywords_RowNumber(t *testing.T) {
	input := "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) FROM employees"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{SELECT, "SELECT"},
		{IDENT, "ROW_NUMBER"},
		{LPAREN, "("},
		{RPAREN, ")"},
		{OVER, "OVER"},
		{LPAREN, "("},
		{PARTITION, "PARTITION"},
		{BY, "BY"},
		{IDENT, "dept"},
		{ORDER, "ORDER"},
		{BY, "BY"},
		{IDENT, "salary"},
		{RPAREN, ")"},
		{FROM, "FROM"},
		{IDENT, "employees"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_WindowFrameKeywords(t *testing.T) {
	input := "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{ROWS, "ROWS"},
		{BETWEEN, "BETWEEN"},
		{UNBOUNDED, "UNBOUNDED"},
		{PRECEDING, "PRECEDING"},
		{AND, "AND"},
		{CURRENT, "CURRENT"},
		{ROW, "ROW"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_TriggerKeywords(t *testing.T) {
	input := "CREATE TRIGGER audit_insert BEFORE INSERT ON users BEGIN END"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{CREATE, "CREATE"},
		{TRIGGER, "TRIGGER"},
		{IDENT, "audit_insert"},
		{BEFORE, "BEFORE"},
		{INSERT, "INSERT"},
		{ON, "ON"},
		{IDENT, "users"},
		{BEGIN, "BEGIN"},
		{END, "END"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v (literal=%q)", i, tok.Type, exp.typ, tok.Literal)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_TriggerKeywords_AfterUpdate(t *testing.T) {
	input := "AFTER UPDATE DELETE TRIGGER"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{AFTER, "AFTER"},
		{UPDATE, "UPDATE"},
		{DELETE, "DELETE"},
		{TRIGGER, "TRIGGER"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_RaiseKeywords(t *testing.T) {
	input := "RAISE(ABORT, 'error message') RAISE(IGNORE)"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{RAISE, "RAISE"},
		{LPAREN, "("},
		{ABORT, "ABORT"},
		{COMMA, ","},
		{STRING, "error message"},
		{RPAREN, ")"},
		{RAISE, "RAISE"},
		{LPAREN, "("},
		{IGNORE, "IGNORE"},
		{RPAREN, ")"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v (literal=%q)", i, tok.Type, exp.typ, tok.Literal)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexerDuplicateKeyword(t *testing.T) {
	input := "ON DUPLICATE KEY UPDATE"
	l := New(input)

	expected := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{ON, "ON"},
		{DUPLICATE, "DUPLICATE"},
		{KEY, "KEY"},
		{UPDATE, "UPDATE"},
		{EOF, ""},
	}

	for i, tt := range expected {
		tok := l.NextToken()
		if tok.Type != tt.expectedType {
			t.Fatalf("tests[%d] - tokentype wrong. expected=%q, got=%q",
				i, tt.expectedType, tok.Type)
		}
		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("tests[%d] - literal wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.Literal)
		}
	}
}

func TestLexer_TruncateKeyword(t *testing.T) {
	input := "TRUNCATE TABLE users"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{TRUNCATE, "TRUNCATE"},
		{TABLE, "TABLE"},
		{IDENT, "users"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v", i, tok.Type, exp.typ)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_BacktickIdentifier(t *testing.T) {
	// Backtick-quoted identifiers should be treated as IDENT, not keywords
	input := "`SELECT` `FROM` `index` `normal_name`"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{IDENT, "SELECT"},
		{IDENT, "FROM"},
		{IDENT, "index"},
		{IDENT, "normal_name"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v (literal=%q)", i, tok.Type, exp.typ, tok.Literal)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_BacktickInStatement(t *testing.T) {
	// Test backtick identifiers in a full SQL statement
	input := "SELECT `index`, `order` FROM `table`"
	expected := []struct {
		typ     TokenType
		literal string
	}{
		{SELECT, "SELECT"},
		{IDENT, "index"},
		{COMMA, ","},
		{IDENT, "order"},
		{FROM, "FROM"},
		{IDENT, "table"},
		{EOF, ""},
	}

	l := New(input)
	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("token[%d]: type = %v, want %v (literal=%q)", i, tok.Type, exp.typ, tok.Literal)
		}
		if tok.Literal != exp.literal {
			t.Errorf("token[%d]: literal = %q, want %q", i, tok.Literal, exp.literal)
		}
	}
}

func TestLexer_BacktickEscapedBacktick(t *testing.T) {
	// MySQL-style: double backtick inside backtick-quoted identifier escapes to single backtick
	input := "`column``name`"
	l := New(input)

	tok := l.NextToken()
	if tok.Type != IDENT {
		t.Errorf("expected IDENT, got %v", tok.Type)
	}
	if tok.Literal != "column`name" {
		t.Errorf("expected 'column`name', got %q", tok.Literal)
	}
}

func TestLexer_BacktickEmpty(t *testing.T) {
	// Empty backtick identifier (edge case)
	input := "``"
	l := New(input)

	tok := l.NextToken()
	if tok.Type != IDENT {
		t.Errorf("expected IDENT, got %v", tok.Type)
	}
	if tok.Literal != "" {
		t.Errorf("expected empty string, got %q", tok.Literal)
	}
}
