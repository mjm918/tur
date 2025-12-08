package parser

import (
	"testing"

	"tur/pkg/sql/lexer"
	"tur/pkg/types"
)

func TestParser_CreateTable_Simple(t *testing.T) {
	input := "CREATE TABLE users (id INT, name TEXT)"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected *CreateTableStmt, got %T", stmt)
	}

	if create.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", create.TableName)
	}

	if len(create.Columns) != 2 {
		t.Fatalf("Columns count = %d, want 2", len(create.Columns))
	}

	// Check first column
	if create.Columns[0].Name != "id" || create.Columns[0].Type != types.TypeInt {
		t.Errorf("Column[0] = %+v, want {Name: 'id', Type: TypeInt}", create.Columns[0])
	}

	// Check second column
	if create.Columns[1].Name != "name" || create.Columns[1].Type != types.TypeText {
		t.Errorf("Column[1] = %+v, want {Name: 'name', Type: TypeText}", create.Columns[1])
	}
}

func TestParser_CreateTable_WithConstraints(t *testing.T) {
	input := "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)

	if !create.Columns[0].PrimaryKey {
		t.Error("Column[0].PrimaryKey = false, want true")
	}

	if !create.Columns[1].NotNull {
		t.Error("Column[1].NotNull = false, want true")
	}
}

func TestParser_CreateTable_AllTypes(t *testing.T) {
	input := "CREATE TABLE data (a INT, b INTEGER, c TEXT, d FLOAT, e REAL, f BLOB, g VECTOR(3))"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	expectedTypes := []types.ValueType{
		types.TypeInt, types.TypeInt, types.TypeText, types.TypeFloat,
		types.TypeFloat, types.TypeBlob, types.TypeVector,
	}

	if len(create.Columns) != len(expectedTypes) {
		t.Fatalf("Columns count = %d, want %d", len(create.Columns), len(expectedTypes))
	}

	for i, expected := range expectedTypes {
		if create.Columns[i].Type != expected {
			t.Errorf("Column[%d].Type = %v, want %v", i, create.Columns[i].Type, expected)
		}
	}
}

func TestParser_Insert_Simple(t *testing.T) {
	input := "INSERT INTO users VALUES (1, 'Alice')"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	insert, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("Expected *InsertStmt, got %T", stmt)
	}

	if insert.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", insert.TableName)
	}

	if insert.Columns != nil {
		t.Errorf("Columns = %v, want nil", insert.Columns)
	}

	if len(insert.Values) != 1 {
		t.Fatalf("Values rows = %d, want 1", len(insert.Values))
	}

	if len(insert.Values[0]) != 2 {
		t.Fatalf("Values[0] count = %d, want 2", len(insert.Values[0]))
	}

	// Check values
	lit1 := insert.Values[0][0].(*Literal)
	if lit1.Value.Int() != 1 {
		t.Errorf("Values[0][0] = %v, want 1", lit1.Value.Int())
	}

	lit2 := insert.Values[0][1].(*Literal)
	if lit2.Value.Text() != "Alice" {
		t.Errorf("Values[0][1] = %q, want 'Alice'", lit2.Value.Text())
	}
}

func TestParser_Insert_WithColumns(t *testing.T) {
	input := "INSERT INTO users (id, name) VALUES (1, 'Alice')"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	insert := stmt.(*InsertStmt)

	if len(insert.Columns) != 2 {
		t.Fatalf("Columns count = %d, want 2", len(insert.Columns))
	}

	if insert.Columns[0] != "id" || insert.Columns[1] != "name" {
		t.Errorf("Columns = %v, want ['id', 'name']", insert.Columns)
	}
}

func TestParser_Insert_MultipleRows(t *testing.T) {
	input := "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	insert := stmt.(*InsertStmt)

	if len(insert.Values) != 2 {
		t.Fatalf("Values rows = %d, want 2", len(insert.Values))
	}
}

func TestParser_Select_Star(t *testing.T) {
	input := "SELECT * FROM users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected *SelectStmt, got %T", stmt)
	}

	if sel.From != "users" {
		t.Errorf("From = %q, want 'users'", sel.From)
	}

	if len(sel.Columns) != 1 || !sel.Columns[0].Star {
		t.Errorf("Columns = %+v, want [{Star: true}]", sel.Columns)
	}

	if sel.Where != nil {
		t.Errorf("Where = %v, want nil", sel.Where)
	}
}

func TestParser_Select_Columns(t *testing.T) {
	input := "SELECT id, name FROM users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	if len(sel.Columns) != 2 {
		t.Fatalf("Columns count = %d, want 2", len(sel.Columns))
	}

	if sel.Columns[0].Name != "id" || sel.Columns[1].Name != "name" {
		t.Errorf("Columns = %+v", sel.Columns)
	}
}

func TestParser_Select_WithWhere(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	if sel.Where == nil {
		t.Fatal("Where = nil, expected expression")
	}

	binary, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("Where type = %T, want *BinaryExpr", sel.Where)
	}

	if binary.Op != lexer.EQ {
		t.Errorf("Where.Op = %v, want EQ", binary.Op)
	}

	col := binary.Left.(*ColumnRef)
	if col.Name != "id" {
		t.Errorf("Where.Left = %q, want 'id'", col.Name)
	}

	lit := binary.Right.(*Literal)
	if lit.Value.Int() != 1 {
		t.Errorf("Where.Right = %v, want 1", lit.Value.Int())
	}
}

func TestParser_Select_WhereAnd(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1 AND name = 'Alice'"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)
	binary := sel.Where.(*BinaryExpr)

	if binary.Op != lexer.AND {
		t.Errorf("Where.Op = %v, want AND", binary.Op)
	}

	// Left side: id = 1
	left := binary.Left.(*BinaryExpr)
	if left.Op != lexer.EQ {
		t.Errorf("Where.Left.Op = %v, want EQ", left.Op)
	}

	// Right side: name = 'Alice'
	right := binary.Right.(*BinaryExpr)
	if right.Op != lexer.EQ {
		t.Errorf("Where.Right.Op = %v, want EQ", right.Op)
	}
}

func TestParser_Select_WhereComparisons(t *testing.T) {
	tests := []struct {
		input string
		op    lexer.TokenType
	}{
		{"SELECT * FROM t WHERE x = 1", lexer.EQ},
		{"SELECT * FROM t WHERE x != 1", lexer.NEQ},
		{"SELECT * FROM t WHERE x <> 1", lexer.NEQ},
		{"SELECT * FROM t WHERE x < 1", lexer.LT},
		{"SELECT * FROM t WHERE x > 1", lexer.GT},
		{"SELECT * FROM t WHERE x <= 1", lexer.LTE},
		{"SELECT * FROM t WHERE x >= 1", lexer.GTE},
	}

	for _, tt := range tests {
		p := New(tt.input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q): %v", tt.input, err)
			continue
		}

		sel := stmt.(*SelectStmt)
		binary := sel.Where.(*BinaryExpr)

		if binary.Op != tt.op {
			t.Errorf("Parse(%q): op = %v, want %v", tt.input, binary.Op, tt.op)
		}
	}
}

func TestParser_Literals(t *testing.T) {
	tests := []struct {
		input    string
		checkVal func(*Literal) bool
	}{
		{"INSERT INTO t VALUES (NULL)", func(l *Literal) bool { return l.Value.IsNull() }},
		{"INSERT INTO t VALUES (42)", func(l *Literal) bool { return l.Value.Int() == 42 }},
		{"INSERT INTO t VALUES (-42)", func(l *Literal) bool { return l.Value.Int() == -42 }},
		{"INSERT INTO t VALUES (3.14)", func(l *Literal) bool { return l.Value.Float() == 3.14 }},
		{"INSERT INTO t VALUES ('hello')", func(l *Literal) bool { return l.Value.Text() == "hello" }},
	}

	for _, tt := range tests {
		p := New(tt.input)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q): %v", tt.input, err)
			continue
		}

		insert := stmt.(*InsertStmt)
		lit := insert.Values[0][0].(*Literal)

		if !tt.checkVal(lit) {
			t.Errorf("Parse(%q): unexpected value %+v", tt.input, lit.Value)
		}
	}
}

func TestParser_DropTable(t *testing.T) {
	input := "DROP TABLE users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	drop, ok := stmt.(*DropTableStmt)
	if !ok {
		t.Fatalf("Expected *DropTableStmt, got %T", stmt)
	}

	if drop.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", drop.TableName)
	}
}

func TestParser_Errors(t *testing.T) {
	tests := []string{
		"SELECT",                // missing columns
		"SELECT * FROM",         // missing table
		"CREATE TABLE",          // missing table name
		"CREATE TABLE t",        // missing columns
		"INSERT INTO",           // missing table
		"INSERT INTO t VALUES",  // missing values
		"SELECT * FROM t WHERE", // missing expression
		"CREATE TABLE t ()",     // empty columns
		"CREATE TABLE t (id)",   // missing type
	}

	for _, input := range tests {
		p := New(input)
		_, err := p.Parse()
		if err == nil {
			t.Errorf("Parse(%q): expected error, got nil", input)
		}
	}
}

func TestParser_CreateTable_VectorType(t *testing.T) {
	tests := []struct {
		input       string
		wantErr     bool
		expectedDim int
	}{
		{"CREATE TABLE t (v VECTOR(128))", false, 128},
		{"CREATE TABLE t (v VECTOR(3))", false, 3},
		{"CREATE TABLE t (v VECTOR(0))", true, 0},   // Invalid dim
		{"CREATE TABLE t (v VECTOR(-1))", true, 0},  // Invalid dim
		{"CREATE TABLE t (v VECTOR)", true, 0},      // Missing dim
		{"CREATE TABLE t (v VECTOR())", true, 0},    // Empty dim
		{"CREATE TABLE t (v VECTOR(abc))", true, 0}, // Non-int dim
	}

	for _, tt := range tests {
		p := New(tt.input)
		stmt, err := p.Parse()

		if tt.wantErr {
			if err == nil {
				t.Errorf("Parse(%q): expected error, got nil", tt.input)
			}
			continue
		}

		if err != nil {
			t.Errorf("Parse(%q): unexpected error: %v", tt.input, err)
			continue
		}

		create := stmt.(*CreateTableStmt)
		if len(create.Columns) != 1 {
			t.Fatalf("Parse(%q): expected 1 column, got %d", tt.input, len(create.Columns))
		}

		col := create.Columns[0]
		if col.Type != types.TypeVector {
			t.Errorf("Parse(%q): expected TypeVector, got %v", tt.input, col.Type)
		}

		if col.VectorDim != tt.expectedDim {
			t.Errorf("Parse(%q): expected dim %d, got %d", tt.input, tt.expectedDim, col.VectorDim)
		}
	}
}
