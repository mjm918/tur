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

func TestParser_Insert_Select(t *testing.T) {
	input := "INSERT INTO users SELECT * FROM old_users"
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

	if insert.SelectStmt == nil {
		t.Fatal("SelectStmt = nil, want non-nil")
	}

	fromTable, ok := insert.SelectStmt.From.(*Table)
	if !ok {
		t.Fatalf("SelectStmt.From type = %T, want *Table", insert.SelectStmt.From)
	}
	if fromTable.Name != "old_users" {
		t.Errorf("SelectStmt.From = %q, want 'old_users'", fromTable.Name)
	}

	if insert.Values != nil {
		t.Errorf("Values = %v, want nil (should use SelectStmt instead)", insert.Values)
	}
}

func TestParser_Insert_SelectWithColumns(t *testing.T) {
	input := "INSERT INTO users (id, name) SELECT user_id, username FROM old_users"
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

	if insert.SelectStmt == nil {
		t.Fatal("SelectStmt = nil, want non-nil")
	}

	if len(insert.SelectStmt.Columns) != 2 {
		t.Fatalf("SelectStmt.Columns count = %d, want 2", len(insert.SelectStmt.Columns))
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

	fromTable, ok := sel.From.(*Table)
	if !ok {
		t.Fatalf("From type = %T, want *Table", sel.From)
	}
	if fromTable.Name != "users" {
		t.Errorf("From = %q, want 'users'", fromTable.Name)
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

	col0 := sel.Columns[0].Expr.(*ColumnRef)
	if col0.Name != "id" {
		t.Errorf("Columns[0] = %q, want 'id'", col0.Name)
	}

	col1 := sel.Columns[1].Expr.(*ColumnRef)
	if col1.Name != "name" {
		t.Errorf("Columns[1] = %q, want 'name'", col1.Name)
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

	if drop.IfExists {
		t.Errorf("IfExists = true, want false")
	}

	if drop.Cascade {
		t.Errorf("Cascade = true, want false")
	}
}

func TestParser_DropTable_IfExists(t *testing.T) {
	input := "DROP TABLE IF EXISTS users"
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

	if !drop.IfExists {
		t.Errorf("IfExists = false, want true")
	}

	if drop.Cascade {
		t.Errorf("Cascade = true, want false")
	}
}

func TestParser_DropTable_Cascade(t *testing.T) {
	input := "DROP TABLE users CASCADE"
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

	if drop.IfExists {
		t.Errorf("IfExists = true, want false")
	}

	if !drop.Cascade {
		t.Errorf("Cascade = false, want true")
	}
}

func TestParser_DropTable_IfExists_Cascade(t *testing.T) {
	input := "DROP TABLE IF EXISTS users CASCADE"
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

	if !drop.IfExists {
		t.Errorf("IfExists = false, want true")
	}

	if !drop.Cascade {
		t.Errorf("Cascade = false, want true")
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

// ========== Constraint Parsing Tests ==========

func TestParser_CreateTable_UniqueConstraint(t *testing.T) {
	input := "CREATE TABLE users (email TEXT UNIQUE)"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	if len(create.Columns) != 1 {
		t.Fatalf("Columns count = %d, want 1", len(create.Columns))
	}

	col := create.Columns[0]
	if !col.Unique {
		t.Error("Column.Unique = false, want true")
	}
}

func TestParser_CreateTable_DefaultConstraint(t *testing.T) {
	tests := []struct {
		input          string
		wantDefaultInt int64
	}{
		{"CREATE TABLE t (status INT DEFAULT 0)", 0},
		{"CREATE TABLE t (count INT DEFAULT 100)", 100},
	}

	for _, tt := range tests {
		p := New(tt.input)
		stmt, err := p.Parse()
		if err != nil {
			t.Fatalf("Parse(%q) error: %v", tt.input, err)
		}

		create := stmt.(*CreateTableStmt)
		col := create.Columns[0]

		if col.DefaultExpr == nil {
			t.Fatalf("Parse(%q): DefaultExpr = nil, want non-nil", tt.input)
		}

		lit, ok := col.DefaultExpr.(*Literal)
		if !ok {
			t.Fatalf("Parse(%q): DefaultExpr type = %T, want *Literal", tt.input, col.DefaultExpr)
		}

		if lit.Value.Int() != tt.wantDefaultInt {
			t.Errorf("Parse(%q): DefaultExpr value = %d, want %d", tt.input, lit.Value.Int(), tt.wantDefaultInt)
		}
	}
}

func TestParser_CreateTable_DefaultTextConstraint(t *testing.T) {
	input := "CREATE TABLE t (name TEXT DEFAULT 'unknown')"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	col := create.Columns[0]

	if col.DefaultExpr == nil {
		t.Fatal("DefaultExpr = nil, want non-nil")
	}

	lit := col.DefaultExpr.(*Literal)
	if lit.Value.Text() != "unknown" {
		t.Errorf("DefaultExpr value = %q, want 'unknown'", lit.Value.Text())
	}
}

func TestParser_CreateTable_CheckConstraint(t *testing.T) {
	input := "CREATE TABLE t (age INT CHECK (age >= 0))"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	col := create.Columns[0]

	if col.CheckExpr == nil {
		t.Fatal("CheckExpr = nil, want non-nil")
	}

	// Verify it's a binary expression with >= operator
	binary, ok := col.CheckExpr.(*BinaryExpr)
	if !ok {
		t.Fatalf("CheckExpr type = %T, want *BinaryExpr", col.CheckExpr)
	}

	if binary.Op != lexer.GTE {
		t.Errorf("CheckExpr.Op = %v, want GTE", binary.Op)
	}
}

func TestParser_CreateTable_ForeignKeyConstraint(t *testing.T) {
	input := "CREATE TABLE orders (user_id INT REFERENCES users(id))"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	col := create.Columns[0]

	if col.ForeignKey == nil {
		t.Fatal("ForeignKey = nil, want non-nil")
	}

	if col.ForeignKey.RefTable != "users" {
		t.Errorf("ForeignKey.RefTable = %q, want 'users'", col.ForeignKey.RefTable)
	}

	if col.ForeignKey.RefColumn != "id" {
		t.Errorf("ForeignKey.RefColumn = %q, want 'id'", col.ForeignKey.RefColumn)
	}
}

func TestParser_CreateTable_ForeignKeyWithActions(t *testing.T) {
	input := "CREATE TABLE orders (user_id INT REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL)"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	col := create.Columns[0]

	if col.ForeignKey == nil {
		t.Fatal("ForeignKey = nil, want non-nil")
	}

	if col.ForeignKey.OnDelete != FKActionCascade {
		t.Errorf("ForeignKey.OnDelete = %v, want FKActionCascade", col.ForeignKey.OnDelete)
	}

	if col.ForeignKey.OnUpdate != FKActionSetNull {
		t.Errorf("ForeignKey.OnUpdate = %v, want FKActionSetNull", col.ForeignKey.OnUpdate)
	}
}

func TestParser_CreateTable_MultipleColumnConstraints(t *testing.T) {
	input := "CREATE TABLE users (id INT PRIMARY KEY NOT NULL, email TEXT UNIQUE NOT NULL, age INT DEFAULT 0 CHECK (age >= 0))"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	if len(create.Columns) != 3 {
		t.Fatalf("Columns count = %d, want 3", len(create.Columns))
	}

	// Check id column
	id := create.Columns[0]
	if !id.PrimaryKey {
		t.Error("id.PrimaryKey = false, want true")
	}
	if !id.NotNull {
		t.Error("id.NotNull = false, want true")
	}

	// Check email column
	email := create.Columns[1]
	if !email.Unique {
		t.Error("email.Unique = false, want true")
	}
	if !email.NotNull {
		t.Error("email.NotNull = false, want true")
	}

	// Check age column
	age := create.Columns[2]
	if age.DefaultExpr == nil {
		t.Error("age.DefaultExpr = nil, want non-nil")
	}
	if age.CheckExpr == nil {
		t.Error("age.CheckExpr = nil, want non-nil")
	}
}

func TestParser_CreateTable_TableLevelPrimaryKey(t *testing.T) {
	input := "CREATE TABLE order_items (order_id INT, product_id INT, PRIMARY KEY (order_id, product_id))"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	if len(create.TableConstraints) != 1 {
		t.Fatalf("TableConstraints count = %d, want 1", len(create.TableConstraints))
	}

	pk := create.TableConstraints[0]
	if pk.Type != TableConstraintPrimaryKey {
		t.Errorf("TableConstraints[0].Type = %v, want TableConstraintPrimaryKey", pk.Type)
	}

	if len(pk.Columns) != 2 {
		t.Fatalf("TableConstraints[0].Columns count = %d, want 2", len(pk.Columns))
	}

	if pk.Columns[0] != "order_id" || pk.Columns[1] != "product_id" {
		t.Errorf("TableConstraints[0].Columns = %v, want ['order_id', 'product_id']", pk.Columns)
	}
}

func TestParser_CreateTable_TableLevelUnique(t *testing.T) {
	input := "CREATE TABLE t (a INT, b INT, UNIQUE (a, b))"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	if len(create.TableConstraints) != 1 {
		t.Fatalf("TableConstraints count = %d, want 1", len(create.TableConstraints))
	}

	unique := create.TableConstraints[0]
	if unique.Type != TableConstraintUnique {
		t.Errorf("Type = %v, want TableConstraintUnique", unique.Type)
	}

	if len(unique.Columns) != 2 {
		t.Errorf("Columns = %v, want 2 columns", unique.Columns)
	}
}

func TestParser_CreateTable_TableLevelForeignKey(t *testing.T) {
	input := "CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id))"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	if len(create.TableConstraints) != 1 {
		t.Fatalf("TableConstraints count = %d, want 1", len(create.TableConstraints))
	}

	fk := create.TableConstraints[0]
	if fk.Type != TableConstraintForeignKey {
		t.Errorf("Type = %v, want TableConstraintForeignKey", fk.Type)
	}

	if len(fk.Columns) != 1 || fk.Columns[0] != "user_id" {
		t.Errorf("Columns = %v, want ['user_id']", fk.Columns)
	}

	if fk.RefTable != "users" {
		t.Errorf("RefTable = %q, want 'users'", fk.RefTable)
	}

	if len(fk.RefColumns) != 1 || fk.RefColumns[0] != "id" {
		t.Errorf("RefColumns = %v, want ['id']", fk.RefColumns)
	}
}

func TestParser_CreateTable_TableLevelCheck(t *testing.T) {
	input := "CREATE TABLE t (start_date INT, end_date INT, CHECK (start_date < end_date))"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	if len(create.TableConstraints) != 1 {
		t.Fatalf("TableConstraints count = %d, want 1", len(create.TableConstraints))
	}

	check := create.TableConstraints[0]
	if check.Type != TableConstraintCheck {
		t.Errorf("Type = %v, want TableConstraintCheck", check.Type)
	}

	if check.CheckExpr == nil {
		t.Fatal("CheckExpr = nil, want non-nil")
	}
}

func TestParser_CreateTable_NamedConstraint(t *testing.T) {
	input := "CREATE TABLE t (id INT, CONSTRAINT pk_t PRIMARY KEY (id))"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateTableStmt)
	if len(create.TableConstraints) != 1 {
		t.Fatalf("TableConstraints count = %d, want 1", len(create.TableConstraints))
	}

	pk := create.TableConstraints[0]
	if pk.Name != "pk_t" {
		t.Errorf("Name = %q, want 'pk_t'", pk.Name)
	}
}

// ========== CREATE INDEX Tests ==========

func TestParser_CreateIndex_Simple(t *testing.T) {
	input := "CREATE INDEX idx_users_email ON users (email)"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("Expected *CreateIndexStmt, got %T", stmt)
	}

	if create.IndexName != "idx_users_email" {
		t.Errorf("IndexName = %q, want 'idx_users_email'", create.IndexName)
	}
	if create.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", create.TableName)
	}
	if len(create.Columns) != 1 || create.Columns[0] != "email" {
		t.Errorf("Columns = %v, want ['email']", create.Columns)
	}
	if create.Unique {
		t.Error("Unique = true, want false")
	}
}

func TestParser_CreateIndex_MultiColumn(t *testing.T) {
	input := "CREATE INDEX idx_orders ON orders (customer_id, order_date)"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateIndexStmt)
	if len(create.Columns) != 2 {
		t.Fatalf("Columns count = %d, want 2", len(create.Columns))
	}
	if create.Columns[0] != "customer_id" || create.Columns[1] != "order_date" {
		t.Errorf("Columns = %v, want ['customer_id', 'order_date']", create.Columns)
	}
}

func TestParser_CreateIndex_Unique(t *testing.T) {
	input := "CREATE UNIQUE INDEX idx_users_email ON users (email)"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create := stmt.(*CreateIndexStmt)
	if !create.Unique {
		t.Error("Unique = false, want true")
	}
}

func TestParser_DropIndex(t *testing.T) {
	input := "DROP INDEX idx_users_email"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	drop, ok := stmt.(*DropIndexStmt)
	if !ok {
		t.Fatalf("Expected *DropIndexStmt, got %T", stmt)
	}

	if drop.IndexName != "idx_users_email" {
		t.Errorf("IndexName = %q, want 'idx_users_email'", drop.IndexName)
	}

	if drop.IfExists {
		t.Error("IfExists = true, want false")
	}
}

func TestParser_DropIndex_IfExists(t *testing.T) {
	input := "DROP INDEX IF EXISTS idx_users_email"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	drop, ok := stmt.(*DropIndexStmt)
	if !ok {
		t.Fatalf("Expected *DropIndexStmt, got %T", stmt)
	}

	if drop.IndexName != "idx_users_email" {
		t.Errorf("IndexName = %q, want 'idx_users_email'", drop.IndexName)
	}

	if !drop.IfExists {
		t.Error("IfExists = false, want true")
	}
}

// UPDATE statement tests

func TestParser_Update_Simple(t *testing.T) {
	input := "UPDATE users SET name = 'Alice'"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	update, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("Expected *UpdateStmt, got %T", stmt)
	}

	if update.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", update.TableName)
	}

	if len(update.Assignments) != 1 {
		t.Fatalf("Assignments count = %d, want 1", len(update.Assignments))
	}

	if update.Assignments[0].Column != "name" {
		t.Errorf("Assignment column = %q, want 'name'", update.Assignments[0].Column)
	}

	lit, ok := update.Assignments[0].Value.(*Literal)
	if !ok {
		t.Fatalf("Expected *Literal value, got %T", update.Assignments[0].Value)
	}
	if lit.Value.Text() != "Alice" {
		t.Errorf("Assignment value = %q, want 'Alice'", lit.Value.Text())
	}

	if update.Where != nil {
		t.Error("Where should be nil for UPDATE without WHERE")
	}
}

func TestParser_Update_MultipleAssignments(t *testing.T) {
	input := "UPDATE users SET name = 'Bob', age = 30"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	update := stmt.(*UpdateStmt)

	if len(update.Assignments) != 2 {
		t.Fatalf("Assignments count = %d, want 2", len(update.Assignments))
	}

	if update.Assignments[0].Column != "name" {
		t.Errorf("Assignment[0] column = %q, want 'name'", update.Assignments[0].Column)
	}
	if update.Assignments[1].Column != "age" {
		t.Errorf("Assignment[1] column = %q, want 'age'", update.Assignments[1].Column)
	}

	// Check second assignment value
	lit, ok := update.Assignments[1].Value.(*Literal)
	if !ok {
		t.Fatalf("Expected *Literal value, got %T", update.Assignments[1].Value)
	}
	if lit.Value.Int() != 30 {
		t.Errorf("Assignment[1] value = %d, want 30", lit.Value.Int())
	}
}

func TestParser_Update_WithWhere(t *testing.T) {
	input := "UPDATE users SET name = 'Charlie' WHERE id = 1"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	update := stmt.(*UpdateStmt)

	if update.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", update.TableName)
	}

	if len(update.Assignments) != 1 {
		t.Fatalf("Assignments count = %d, want 1", len(update.Assignments))
	}

	if update.Where == nil {
		t.Fatal("Where should not be nil")
	}

	// Check WHERE is id = 1
	bin, ok := update.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("Expected *BinaryExpr for WHERE, got %T", update.Where)
	}

	col, ok := bin.Left.(*ColumnRef)
	if !ok || col.Name != "id" {
		t.Errorf("WHERE left side should be column 'id', got %v", bin.Left)
	}

	if bin.Op != lexer.EQ {
		t.Errorf("WHERE op = %v, want EQ", bin.Op)
	}

	lit, ok := bin.Right.(*Literal)
	if !ok || lit.Value.Int() != 1 {
		t.Errorf("WHERE right side should be 1, got %v", bin.Right)
	}
}

func TestParser_Update_ExpressionValue(t *testing.T) {
	input := "UPDATE counters SET value = value + 1 WHERE id = 5"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	update := stmt.(*UpdateStmt)

	if len(update.Assignments) != 1 {
		t.Fatalf("Assignments count = %d, want 1", len(update.Assignments))
	}

	if update.Assignments[0].Column != "value" {
		t.Errorf("Assignment column = %q, want 'value'", update.Assignments[0].Column)
	}

	// Value should be expression: value + 1
	bin, ok := update.Assignments[0].Value.(*BinaryExpr)
	if !ok {
		t.Fatalf("Expected *BinaryExpr value, got %T", update.Assignments[0].Value)
	}

	if bin.Op != lexer.PLUS {
		t.Errorf("Expression op = %v, want PLUS", bin.Op)
	}

	col, ok := bin.Left.(*ColumnRef)
	if !ok || col.Name != "value" {
		t.Errorf("Expression left should be column 'value', got %v", bin.Left)
	}

	lit, ok := bin.Right.(*Literal)
	if !ok || lit.Value.Int() != 1 {
		t.Errorf("Expression right should be 1, got %v", bin.Right)
	}
}

// ========== DELETE statement tests ==========

func TestParser_Delete_Simple(t *testing.T) {
	input := "DELETE FROM users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("Expected *DeleteStmt, got %T", stmt)
	}

	if del.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", del.TableName)
	}

	if del.Where != nil {
		t.Error("Where should be nil for DELETE without WHERE")
	}
}

func TestParser_Delete_WithWhere(t *testing.T) {
	input := "DELETE FROM users WHERE id = 1"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("Expected *DeleteStmt, got %T", stmt)
	}

	if del.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", del.TableName)
	}

	if del.Where == nil {
		t.Fatal("Where should not be nil")
	}

	// Check WHERE is id = 1
	bin, ok := del.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("Expected *BinaryExpr for WHERE, got %T", del.Where)
	}

	col, ok := bin.Left.(*ColumnRef)
	if !ok || col.Name != "id" {
		t.Errorf("WHERE left side should be column 'id', got %v", bin.Left)
	}

	if bin.Op != lexer.EQ {
		t.Errorf("WHERE op = %v, want EQ", bin.Op)
	}

	rightLit, ok := bin.Right.(*Literal)
	if !ok || rightLit.Value.Int() != 1 {
		t.Errorf("WHERE right side should be 1, got %v", bin.Right)
	}
}

func TestParser_Delete_ComplexWhere(t *testing.T) {
	input := "DELETE FROM orders WHERE status = 'cancelled' AND created_at < 100"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("Expected *DeleteStmt, got %T", stmt)
	}

	if del.TableName != "orders" {
		t.Errorf("TableName = %q, want 'orders'", del.TableName)
	}

	if del.Where == nil {
		t.Fatal("Where should not be nil")
	}

	// Check WHERE is an AND expression
	bin, ok := del.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("Expected *BinaryExpr for WHERE, got %T", del.Where)
	}

	if bin.Op != lexer.AND {
		t.Errorf("WHERE op = %v, want AND", bin.Op)
	}
}

// ========== ANALYZE Tests ==========

func TestParser_Analyze_AllTables(t *testing.T) {
	input := "ANALYZE"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	analyze, ok := stmt.(*AnalyzeStmt)
	if !ok {
		t.Fatalf("Expected *AnalyzeStmt, got %T", stmt)
	}

	if analyze.TableName != "" {
		t.Errorf("TableName = %q, want empty string for all tables", analyze.TableName)
	}
}

func TestParser_Analyze_SpecificTable(t *testing.T) {
	input := "ANALYZE users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	analyze, ok := stmt.(*AnalyzeStmt)
	if !ok {
		t.Fatalf("Expected *AnalyzeStmt, got %T", stmt)
	}

	if analyze.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", analyze.TableName)
	}
}

func TestParser_Analyze_SpecificIndex(t *testing.T) {
	input := "ANALYZE idx_users_email"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	analyze, ok := stmt.(*AnalyzeStmt)
	if !ok {
		t.Fatalf("Expected *AnalyzeStmt, got %T", stmt)
	}

	// When only one identifier is given, it could be a table or index name
	// The executor will determine which based on what exists in the catalog
	if analyze.TableName != "idx_users_email" {
		t.Errorf("TableName = %q, want 'idx_users_email'", analyze.TableName)
	}
}

// ========== ALTER TABLE Tests ==========

func TestParser_AlterTable_AddColumn_Simple(t *testing.T) {
	input := "ALTER TABLE users ADD COLUMN email TEXT"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	alter, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if alter.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", alter.TableName)
	}

	if alter.Action != AlterActionAddColumn {
		t.Errorf("Action = %v, want AlterActionAddColumn", alter.Action)
	}

	if alter.NewColumn == nil {
		t.Fatal("NewColumn = nil, want non-nil")
	}

	if alter.NewColumn.Name != "email" {
		t.Errorf("NewColumn.Name = %q, want 'email'", alter.NewColumn.Name)
	}

	if alter.NewColumn.Type != types.TypeText {
		t.Errorf("NewColumn.Type = %v, want TypeText", alter.NewColumn.Type)
	}
}

func TestParser_AlterTable_AddColumn_WithConstraints(t *testing.T) {
	input := "ALTER TABLE users ADD COLUMN age INT NOT NULL DEFAULT 0"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	alter := stmt.(*AlterTableStmt)

	if alter.NewColumn.Name != "age" {
		t.Errorf("NewColumn.Name = %q, want 'age'", alter.NewColumn.Name)
	}

	if alter.NewColumn.Type != types.TypeInt {
		t.Errorf("NewColumn.Type = %v, want TypeInt", alter.NewColumn.Type)
	}

	if !alter.NewColumn.NotNull {
		t.Error("NewColumn.NotNull = false, want true")
	}

	if alter.NewColumn.DefaultExpr == nil {
		t.Fatal("NewColumn.DefaultExpr = nil, want non-nil")
	}

	lit, ok := alter.NewColumn.DefaultExpr.(*Literal)
	if !ok {
		t.Fatalf("DefaultExpr type = %T, want *Literal", alter.NewColumn.DefaultExpr)
	}
	if lit.Value.Int() != 0 {
		t.Errorf("DefaultExpr value = %d, want 0", lit.Value.Int())
	}
}

func TestParser_AlterTable_AddColumn_NoColumnKeyword(t *testing.T) {
	// SQLite allows omitting COLUMN keyword: ALTER TABLE t ADD col TYPE
	input := "ALTER TABLE users ADD email TEXT"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	alter := stmt.(*AlterTableStmt)
	if alter.NewColumn.Name != "email" {
		t.Errorf("NewColumn.Name = %q, want 'email'", alter.NewColumn.Name)
	}
}

func TestParser_AlterTable_DropColumn(t *testing.T) {
	input := "ALTER TABLE users DROP COLUMN email"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	alter, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if alter.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", alter.TableName)
	}

	if alter.Action != AlterActionDropColumn {
		t.Errorf("Action = %v, want AlterActionDropColumn", alter.Action)
	}

	if alter.ColumnName != "email" {
		t.Errorf("ColumnName = %q, want 'email'", alter.ColumnName)
	}
}

func TestParser_AlterTable_DropColumn_NoColumnKeyword(t *testing.T) {
	// SQLite allows omitting COLUMN keyword: ALTER TABLE t DROP col
	input := "ALTER TABLE users DROP email"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	alter := stmt.(*AlterTableStmt)
	if alter.ColumnName != "email" {
		t.Errorf("ColumnName = %q, want 'email'", alter.ColumnName)
	}
}

func TestParser_AlterTable_RenameTo(t *testing.T) {
	input := "ALTER TABLE users RENAME TO customers"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	alter, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("Expected *AlterTableStmt, got %T", stmt)
	}

	if alter.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", alter.TableName)
	}

	if alter.Action != AlterActionRenameTable {
		t.Errorf("Action = %v, want AlterActionRenameTable", alter.Action)
	}

	if alter.NewName != "customers" {
		t.Errorf("NewName = %q, want 'customers'", alter.NewName)
	}
}

func TestParser_Select_OrderBy(t *testing.T) {
	input := "SELECT * FROM users ORDER BY name"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected *SelectStmt, got %T", stmt)
	}

	if len(sel.OrderBy) != 1 {
		t.Fatalf("OrderBy count = %d, want 1", len(sel.OrderBy))
	}

	colRef, ok := sel.OrderBy[0].Expr.(*ColumnRef)
	if !ok {
		t.Fatalf("OrderBy[0].Expr type = %T, want *ColumnRef", sel.OrderBy[0].Expr)
	}
	if colRef.Name != "name" {
		t.Errorf("OrderBy[0].Expr.Name = %q, want 'name'", colRef.Name)
	}

	if sel.OrderBy[0].Direction != OrderAsc {
		t.Errorf("OrderBy[0].Direction = %v, want OrderAsc", sel.OrderBy[0].Direction)
	}
}

func TestParser_Select_OrderByDesc(t *testing.T) {
	input := "SELECT * FROM users ORDER BY age DESC"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	if len(sel.OrderBy) != 1 {
		t.Fatalf("OrderBy count = %d, want 1", len(sel.OrderBy))
	}

	if sel.OrderBy[0].Direction != OrderDesc {
		t.Errorf("OrderBy[0].Direction = %v, want OrderDesc", sel.OrderBy[0].Direction)
	}
}

func TestParser_Select_OrderByMultiple(t *testing.T) {
	input := "SELECT * FROM users ORDER BY age DESC, name ASC"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	if len(sel.OrderBy) != 2 {
		t.Fatalf("OrderBy count = %d, want 2", len(sel.OrderBy))
	}

	// First: age DESC
	col0, _ := sel.OrderBy[0].Expr.(*ColumnRef)
	if col0.Name != "age" || sel.OrderBy[0].Direction != OrderDesc {
		t.Errorf("OrderBy[0] = {%q, %v}, want {age, DESC}", col0.Name, sel.OrderBy[0].Direction)
	}

	// Second: name ASC
	col1, _ := sel.OrderBy[1].Expr.(*ColumnRef)
	if col1.Name != "name" || sel.OrderBy[1].Direction != OrderAsc {
		t.Errorf("OrderBy[1] = {%q, %v}, want {name, ASC}", col1.Name, sel.OrderBy[1].Direction)
	}
}

func TestParser_Select_Limit(t *testing.T) {
	input := "SELECT * FROM users LIMIT 10"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	if sel.Limit == nil {
		t.Fatal("Limit = nil, expected expression")
	}

	lit, ok := sel.Limit.(*Literal)
	if !ok {
		t.Fatalf("Limit type = %T, want *Literal", sel.Limit)
	}
	if lit.Value.Int() != 10 {
		t.Errorf("Limit = %d, want 10", lit.Value.Int())
	}
}

func TestParser_Select_LimitOffset(t *testing.T) {
	input := "SELECT * FROM users LIMIT 10 OFFSET 5"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	if sel.Limit == nil {
		t.Fatal("Limit = nil, expected expression")
	}
	if sel.Offset == nil {
		t.Fatal("Offset = nil, expected expression")
	}

	limitLit := sel.Limit.(*Literal)
	if limitLit.Value.Int() != 10 {
		t.Errorf("Limit = %d, want 10", limitLit.Value.Int())
	}

	offsetLit := sel.Offset.(*Literal)
	if offsetLit.Value.Int() != 5 {
		t.Errorf("Offset = %d, want 5", offsetLit.Value.Int())
	}
}

func TestParser_Select_OrderByLimitOffset(t *testing.T) {
	input := "SELECT * FROM users ORDER BY name LIMIT 10 OFFSET 5"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	// ORDER BY
	if len(sel.OrderBy) != 1 {
		t.Fatalf("OrderBy count = %d, want 1", len(sel.OrderBy))
	}
	colRef := sel.OrderBy[0].Expr.(*ColumnRef)
	if colRef.Name != "name" {
		t.Errorf("OrderBy[0].Expr.Name = %q, want 'name'", colRef.Name)
	}

	// LIMIT
	if sel.Limit == nil {
		t.Fatal("Limit = nil, expected expression")
	}
	limitLit := sel.Limit.(*Literal)
	if limitLit.Value.Int() != 10 {
		t.Errorf("Limit = %d, want 10", limitLit.Value.Int())
	}

	// OFFSET
	if sel.Offset == nil {
		t.Fatal("Offset = nil, expected expression")
	}
	offsetLit := sel.Offset.(*Literal)
	if offsetLit.Value.Int() != 5 {
		t.Errorf("Offset = %d, want 5", offsetLit.Value.Int())
	}
}

func TestParser_Select_WhereOrderByLimitOffset(t *testing.T) {
	input := "SELECT id, name FROM users WHERE age > 18 ORDER BY name DESC LIMIT 20 OFFSET 10"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	// Columns
	if len(sel.Columns) != 2 {
		t.Fatalf("Columns count = %d, want 2", len(sel.Columns))
	}

	// WHERE
	if sel.Where == nil {
		t.Fatal("Where = nil, expected expression")
	}

	// ORDER BY
	if len(sel.OrderBy) != 1 {
		t.Fatalf("OrderBy count = %d, want 1", len(sel.OrderBy))
	}
	if sel.OrderBy[0].Direction != OrderDesc {
		t.Errorf("OrderBy[0].Direction = %v, want OrderDesc", sel.OrderBy[0].Direction)
	}

	// LIMIT
	limitLit := sel.Limit.(*Literal)
	if limitLit.Value.Int() != 20 {
		t.Errorf("Limit = %d, want 20", limitLit.Value.Int())
	}

	// OFFSET
	offsetLit := sel.Offset.(*Literal)
	if offsetLit.Value.Int() != 10 {
		t.Errorf("Offset = %d, want 10", offsetLit.Value.Int())
	}
}

// ========== GROUP BY Tests ==========

func TestParser_Select_GroupBy(t *testing.T) {
	input := "SELECT department, COUNT(*) FROM employees GROUP BY department"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	// Check GROUP BY
	if len(sel.GroupBy) != 1 {
		t.Fatalf("GroupBy count = %d, want 1", len(sel.GroupBy))
	}

	colRef, ok := sel.GroupBy[0].(*ColumnRef)
	if !ok {
		t.Fatalf("GroupBy[0] type = %T, want *ColumnRef", sel.GroupBy[0])
	}
	if colRef.Name != "department" {
		t.Errorf("GroupBy[0].Name = %q, want 'department'", colRef.Name)
	}
}

func TestParser_Select_GroupByMultipleColumns(t *testing.T) {
	input := "SELECT department, title, COUNT(*) FROM employees GROUP BY department, title"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	// Check GROUP BY
	if len(sel.GroupBy) != 2 {
		t.Fatalf("GroupBy count = %d, want 2", len(sel.GroupBy))
	}

	col1 := sel.GroupBy[0].(*ColumnRef)
	col2 := sel.GroupBy[1].(*ColumnRef)

	if col1.Name != "department" {
		t.Errorf("GroupBy[0].Name = %q, want 'department'", col1.Name)
	}
	if col2.Name != "title" {
		t.Errorf("GroupBy[1].Name = %q, want 'title'", col2.Name)
	}
}

func TestParser_Select_GroupByWithHaving(t *testing.T) {
	input := "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	// Check GROUP BY
	if len(sel.GroupBy) != 1 {
		t.Fatalf("GroupBy count = %d, want 1", len(sel.GroupBy))
	}

	// Check HAVING
	if sel.Having == nil {
		t.Fatal("Having = nil, want non-nil")
	}

	binary, ok := sel.Having.(*BinaryExpr)
	if !ok {
		t.Fatalf("Having type = %T, want *BinaryExpr", sel.Having)
	}

	if binary.Op != lexer.GT {
		t.Errorf("Having.Op = %v, want GT", binary.Op)
	}
}

func TestParser_Select_WhereGroupByHavingOrderBy(t *testing.T) {
	input := "SELECT department, AVG(salary) FROM employees WHERE status = 'active' GROUP BY department HAVING AVG(salary) > 50000 ORDER BY department"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	// WHERE
	if sel.Where == nil {
		t.Error("Where = nil, want non-nil")
	}

	// GROUP BY
	if len(sel.GroupBy) != 1 {
		t.Fatalf("GroupBy count = %d, want 1", len(sel.GroupBy))
	}

	// HAVING
	if sel.Having == nil {
		t.Error("Having = nil, want non-nil")
	}

	// ORDER BY
	if len(sel.OrderBy) != 1 {
		t.Fatalf("OrderBy count = %d, want 1", len(sel.OrderBy))
	}
}

// ========== Transaction Control Tests ==========

func TestParser_Begin(t *testing.T) {
	input := "BEGIN"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	begin, ok := stmt.(*BeginStmt)
	if !ok {
		t.Fatalf("Expected *BeginStmt, got %T", stmt)
	}

	// BEGIN without TRANSACTION should work
	_ = begin
}

func TestParser_BeginTransaction(t *testing.T) {
	input := "BEGIN TRANSACTION"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	_, ok := stmt.(*BeginStmt)
	if !ok {
		t.Fatalf("Expected *BeginStmt, got %T", stmt)
	}
}

func TestParser_Commit(t *testing.T) {
	input := "COMMIT"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	commit, ok := stmt.(*CommitStmt)
	if !ok {
		t.Fatalf("Expected *CommitStmt, got %T", stmt)
	}

	_ = commit
}

func TestParser_CommitTransaction(t *testing.T) {
	input := "COMMIT TRANSACTION"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	_, ok := stmt.(*CommitStmt)
	if !ok {
		t.Fatalf("Expected *CommitStmt, got %T", stmt)
	}
}

func TestParser_Rollback(t *testing.T) {
	input := "ROLLBACK"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	rollback, ok := stmt.(*RollbackStmt)
	if !ok {
		t.Fatalf("Expected *RollbackStmt, got %T", stmt)
	}

	_ = rollback
}

func TestParser_RollbackTransaction(t *testing.T) {
	input := "ROLLBACK TRANSACTION"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	_, ok := stmt.(*RollbackStmt)
	if !ok {
		t.Fatalf("Expected *RollbackStmt, got %T", stmt)
	}
}

// ========== UNION/INTERSECT/EXCEPT Tests ==========

func TestParser_Union_Simple(t *testing.T) {
	input := "SELECT id FROM users UNION SELECT id FROM admins"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	setOp, ok := stmt.(*SetOperation)
	if !ok {
		t.Fatalf("Expected *SetOperation, got %T", stmt)
	}

	if setOp.Operator != SetOpUnion {
		t.Errorf("Operator = %v, want SetOpUnion", setOp.Operator)
	}

	if setOp.All {
		t.Error("All = true, want false (UNION without ALL should deduplicate)")
	}

	// Check left SELECT
	if setOp.Left == nil {
		t.Fatal("Left = nil, want non-nil")
	}
	leftTable, ok := setOp.Left.From.(*Table)
	if !ok {
		t.Fatalf("Left.From type = %T, want *Table", setOp.Left.From)
	}
	if leftTable.Name != "users" {
		t.Errorf("Left.From.Name = %q, want 'users'", leftTable.Name)
	}

	// Check right SELECT
	if setOp.Right == nil {
		t.Fatal("Right = nil, want non-nil")
	}
	rightTable, ok := setOp.Right.From.(*Table)
	if !ok {
		t.Fatalf("Right.From type = %T, want *Table", setOp.Right.From)
	}
	if rightTable.Name != "admins" {
		t.Errorf("Right.From.Name = %q, want 'admins'", rightTable.Name)
	}
}

func TestParser_UnionAll(t *testing.T) {
	input := "SELECT id FROM users UNION ALL SELECT id FROM admins"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	setOp, ok := stmt.(*SetOperation)
	if !ok {
		t.Fatalf("Expected *SetOperation, got %T", stmt)
	}

	if setOp.Operator != SetOpUnion {
		t.Errorf("Operator = %v, want SetOpUnion", setOp.Operator)
	}

	if !setOp.All {
		t.Error("All = false, want true (UNION ALL should preserve duplicates)")
	}
}

func TestParser_Intersect(t *testing.T) {
	input := "SELECT id FROM users INTERSECT SELECT id FROM admins"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	setOp, ok := stmt.(*SetOperation)
	if !ok {
		t.Fatalf("Expected *SetOperation, got %T", stmt)
	}

	if setOp.Operator != SetOpIntersect {
		t.Errorf("Operator = %v, want SetOpIntersect", setOp.Operator)
	}

	if setOp.All {
		t.Error("All = true, want false")
	}

	// Check left SELECT
	if setOp.Left == nil {
		t.Fatal("Left = nil, want non-nil")
	}
	leftTable, ok := setOp.Left.From.(*Table)
	if !ok {
		t.Fatalf("Left.From type = %T, want *Table", setOp.Left.From)
	}
	if leftTable.Name != "users" {
		t.Errorf("Left.From.Name = %q, want 'users'", leftTable.Name)
	}

	// Check right SELECT
	if setOp.Right == nil {
		t.Fatal("Right = nil, want non-nil")
	}
	rightTable, ok := setOp.Right.From.(*Table)
	if !ok {
		t.Fatalf("Right.From type = %T, want *Table", setOp.Right.From)
	}
	if rightTable.Name != "admins" {
		t.Errorf("Right.From.Name = %q, want 'admins'", rightTable.Name)
	}
}

func TestParser_IntersectAll(t *testing.T) {
	input := "SELECT id FROM users INTERSECT ALL SELECT id FROM admins"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	setOp, ok := stmt.(*SetOperation)
	if !ok {
		t.Fatalf("Expected *SetOperation, got %T", stmt)
	}

	if setOp.Operator != SetOpIntersect {
		t.Errorf("Operator = %v, want SetOpIntersect", setOp.Operator)
	}

	if !setOp.All {
		t.Error("All = false, want true")
	}
}

func TestParser_Except(t *testing.T) {
	input := "SELECT id FROM users EXCEPT SELECT id FROM banned"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	setOp, ok := stmt.(*SetOperation)
	if !ok {
		t.Fatalf("Expected *SetOperation, got %T", stmt)
	}

	if setOp.Operator != SetOpExcept {
		t.Errorf("Operator = %v, want SetOpExcept", setOp.Operator)
	}

	if setOp.All {
		t.Error("All = true, want false")
	}

	// Check right SELECT
	rightTable, ok := setOp.Right.From.(*Table)
	if !ok {
		t.Fatalf("Right.From type = %T, want *Table", setOp.Right.From)
	}
	if rightTable.Name != "banned" {
		t.Errorf("Right.From.Name = %q, want 'banned'", rightTable.Name)
	}
}

func TestParser_ExceptAll(t *testing.T) {
	input := "SELECT id FROM users EXCEPT ALL SELECT id FROM banned"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	setOp, ok := stmt.(*SetOperation)
	if !ok {
		t.Fatalf("Expected *SetOperation, got %T", stmt)
	}

	if setOp.Operator != SetOpExcept {
		t.Errorf("Operator = %v, want SetOpExcept", setOp.Operator)
	}

	if !setOp.All {
		t.Error("All = false, want true")
	}
}

// ============================================================================
// CTE (Common Table Expression) Tests
// ============================================================================

func TestParser_CTE_Simple(t *testing.T) {
	// WITH temp AS (SELECT 1) SELECT * FROM temp
	input := "WITH temp AS (SELECT 1 FROM dual) SELECT * FROM temp"
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
		t.Fatal("With = nil, want non-nil WithClause")
	}

	if sel.With.Recursive {
		t.Error("With.Recursive = true, want false")
	}

	if len(sel.With.CTEs) != 1 {
		t.Fatalf("CTEs count = %d, want 1", len(sel.With.CTEs))
	}

	cte := sel.With.CTEs[0]
	if cte.Name != "temp" {
		t.Errorf("CTE name = %q, want 'temp'", cte.Name)
	}

	if cte.Query == nil {
		t.Fatal("CTE.Query = nil, want non-nil")
	}

	// Verify the main SELECT uses the CTE
	fromTable, ok := sel.From.(*Table)
	if !ok {
		t.Fatalf("From type = %T, want *Table", sel.From)
	}
	if fromTable.Name != "temp" {
		t.Errorf("From table = %q, want 'temp'", fromTable.Name)
	}
}

func TestParser_CTE_Recursive(t *testing.T) {
	// WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<10) SELECT x FROM cnt
	input := "WITH RECURSIVE cnt(x) AS (SELECT 1 FROM dual) SELECT x FROM cnt"
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
		t.Fatal("With = nil, want non-nil WithClause")
	}

	if !sel.With.Recursive {
		t.Error("With.Recursive = false, want true")
	}

	if len(sel.With.CTEs) != 1 {
		t.Fatalf("CTEs count = %d, want 1", len(sel.With.CTEs))
	}

	cte := sel.With.CTEs[0]
	if cte.Name != "cnt" {
		t.Errorf("CTE name = %q, want 'cnt'", cte.Name)
	}

	// Check column list
	if len(cte.Columns) != 1 {
		t.Fatalf("CTE columns count = %d, want 1", len(cte.Columns))
	}
	if cte.Columns[0] != "x" {
		t.Errorf("CTE column[0] = %q, want 'x'", cte.Columns[0])
	}
}

func TestParser_CTE_MultipleCTEs(t *testing.T) {
	// WITH cte1 AS (...), cte2 AS (...) SELECT ...
	input := "WITH cte1 AS (SELECT 1 FROM t1), cte2 AS (SELECT 2 FROM t2) SELECT * FROM cte1, cte2"
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
		t.Fatal("With = nil, want non-nil WithClause")
	}

	if len(sel.With.CTEs) != 2 {
		t.Fatalf("CTEs count = %d, want 2", len(sel.With.CTEs))
	}

	// Verify first CTE
	cte1 := sel.With.CTEs[0]
	if cte1.Name != "cte1" {
		t.Errorf("CTE[0] name = %q, want 'cte1'", cte1.Name)
	}

	// Verify second CTE
	cte2 := sel.With.CTEs[1]
	if cte2.Name != "cte2" {
		t.Errorf("CTE[1] name = %q, want 'cte2'", cte2.Name)
	}
}

// CREATE VIEW tests

func TestParser_CreateView_Simple(t *testing.T) {
	input := "CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	view, ok := stmt.(*CreateViewStmt)
	if !ok {
		t.Fatalf("Expected *CreateViewStmt, got %T", stmt)
	}

	if view.ViewName != "active_users" {
		t.Errorf("ViewName = %q, want 'active_users'", view.ViewName)
	}

	if view.Query == nil {
		t.Fatal("Query is nil, expected SelectStmt")
	}

	// Verify the SELECT has the expected structure
	if len(view.Query.Columns) != 2 {
		t.Errorf("Query columns = %d, want 2", len(view.Query.Columns))
	}
}

func TestParser_CreateView_WithColumnList(t *testing.T) {
	input := "CREATE VIEW user_summary (user_id, user_name) AS SELECT id, name FROM users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	view, ok := stmt.(*CreateViewStmt)
	if !ok {
		t.Fatalf("Expected *CreateViewStmt, got %T", stmt)
	}

	if view.ViewName != "user_summary" {
		t.Errorf("ViewName = %q, want 'user_summary'", view.ViewName)
	}

	if len(view.Columns) != 2 {
		t.Fatalf("Columns count = %d, want 2", len(view.Columns))
	}

	if view.Columns[0] != "user_id" {
		t.Errorf("Columns[0] = %q, want 'user_id'", view.Columns[0])
	}

	if view.Columns[1] != "user_name" {
		t.Errorf("Columns[1] = %q, want 'user_name'", view.Columns[1])
	}
}

func TestParser_CreateView_IfNotExists(t *testing.T) {
	input := "CREATE VIEW IF NOT EXISTS my_view AS SELECT * FROM data"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	view, ok := stmt.(*CreateViewStmt)
	if !ok {
		t.Fatalf("Expected *CreateViewStmt, got %T", stmt)
	}

	if view.ViewName != "my_view" {
		t.Errorf("ViewName = %q, want 'my_view'", view.ViewName)
	}

	if !view.IfNotExists {
		t.Error("IfNotExists = false, want true")
	}
}

// DROP VIEW tests

func TestParser_DropView_Simple(t *testing.T) {
	input := "DROP VIEW my_view"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	drop, ok := stmt.(*DropViewStmt)
	if !ok {
		t.Fatalf("Expected *DropViewStmt, got %T", stmt)
	}

	if drop.ViewName != "my_view" {
		t.Errorf("ViewName = %q, want 'my_view'", drop.ViewName)
	}

	if drop.IfExists {
		t.Error("IfExists = true, want false")
	}
}

func TestParser_DropView_IfExists(t *testing.T) {
	input := "DROP VIEW IF EXISTS my_view"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	drop, ok := stmt.(*DropViewStmt)
	if !ok {
		t.Fatalf("Expected *DropViewStmt, got %T", stmt)
	}

	if drop.ViewName != "my_view" {
		t.Errorf("ViewName = %q, want 'my_view'", drop.ViewName)
	}

	if !drop.IfExists {
		t.Error("IfExists = false, want true")
	}
}

// ============================================================================
// EXPLAIN Tests
// ============================================================================

func TestParser_Explain_Select(t *testing.T) {
	input := "EXPLAIN SELECT * FROM users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	explain, ok := stmt.(*ExplainStmt)
	if !ok {
		t.Fatalf("Expected *ExplainStmt, got %T", stmt)
	}

	if explain.QueryPlan {
		t.Error("QueryPlan = true, want false (EXPLAIN without QUERY PLAN)")
	}

	// Check the inner statement is a SELECT
	sel, ok := explain.Statement.(*SelectStmt)
	if !ok {
		t.Fatalf("Statement type = %T, want *SelectStmt", explain.Statement)
	}

	fromTable, ok := sel.From.(*Table)
	if !ok {
		t.Fatalf("From type = %T, want *Table", sel.From)
	}
	if fromTable.Name != "users" {
		t.Errorf("From.Name = %q, want 'users'", fromTable.Name)
	}
}

func TestParser_Explain_Insert(t *testing.T) {
	input := "EXPLAIN INSERT INTO users VALUES (1, 'test')"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	explain, ok := stmt.(*ExplainStmt)
	if !ok {
		t.Fatalf("Expected *ExplainStmt, got %T", stmt)
	}

	insert, ok := explain.Statement.(*InsertStmt)
	if !ok {
		t.Fatalf("Statement type = %T, want *InsertStmt", explain.Statement)
	}

	if insert.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", insert.TableName)
	}
}

func TestParser_Explain_Update(t *testing.T) {
	input := "EXPLAIN UPDATE users SET name = 'test' WHERE id = 1"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	explain, ok := stmt.(*ExplainStmt)
	if !ok {
		t.Fatalf("Expected *ExplainStmt, got %T", stmt)
	}

	update, ok := explain.Statement.(*UpdateStmt)
	if !ok {
		t.Fatalf("Statement type = %T, want *UpdateStmt", explain.Statement)
	}

	if update.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", update.TableName)
	}
}

func TestParser_Explain_Delete(t *testing.T) {
	input := "EXPLAIN DELETE FROM users WHERE id = 1"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	explain, ok := stmt.(*ExplainStmt)
	if !ok {
		t.Fatalf("Expected *ExplainStmt, got %T", stmt)
	}

	delete, ok := explain.Statement.(*DeleteStmt)
	if !ok {
		t.Fatalf("Statement type = %T, want *DeleteStmt", explain.Statement)
	}

	if delete.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", delete.TableName)
	}
}

func TestParser_ExplainQueryPlan_Select(t *testing.T) {
	input := "EXPLAIN QUERY PLAN SELECT * FROM users WHERE id > 10"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	explain, ok := stmt.(*ExplainStmt)
	if !ok {
		t.Fatalf("Expected *ExplainStmt, got %T", stmt)
	}

	if !explain.QueryPlan {
		t.Error("QueryPlan = false, want true (EXPLAIN QUERY PLAN)")
	}

	// Check the inner statement is a SELECT
	sel, ok := explain.Statement.(*SelectStmt)
	if !ok {
		t.Fatalf("Statement type = %T, want *SelectStmt", explain.Statement)
	}

	fromTable, ok := sel.From.(*Table)
	if !ok {
		t.Fatalf("From type = %T, want *Table", sel.From)
	}
	if fromTable.Name != "users" {
		t.Errorf("From.Name = %q, want 'users'", fromTable.Name)
	}

	// Check WHERE clause exists
	if sel.Where == nil {
		t.Error("Where = nil, want non-nil")
	}
}

func TestParser_ExplainQueryPlan_Join(t *testing.T) {
	input := "EXPLAIN QUERY PLAN SELECT * FROM users JOIN orders ON users.id = orders.user_id"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	explain, ok := stmt.(*ExplainStmt)
	if !ok {
		t.Fatalf("Expected *ExplainStmt, got %T", stmt)
	}

	if !explain.QueryPlan {
		t.Error("QueryPlan = false, want true")
	}

	sel, ok := explain.Statement.(*SelectStmt)
	if !ok {
		t.Fatalf("Statement type = %T, want *SelectStmt", explain.Statement)
	}

	// Check that FROM is a Join
	join, ok := sel.From.(*Join)
	if !ok {
		t.Fatalf("From type = %T, want *Join", sel.From)
	}
	if join.Type != JoinInner {
		t.Errorf("Join type = %v, want JoinInner", join.Type)
	}
}

// ====== Window Functions Tests ======

func TestParser_WindowFunction_RankWithOrderBy(t *testing.T) {
	input := "SELECT name, RANK() OVER (ORDER BY score DESC) FROM users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected *SelectStmt, got %T", stmt)
	}

	if len(sel.Columns) != 2 {
		t.Fatalf("Columns count = %d, want 2", len(sel.Columns))
	}

	// Second column should be a window function
	winFunc, ok := sel.Columns[1].Expr.(*WindowFunction)
	if !ok {
		t.Fatalf("Expected *WindowFunction for column 2, got %T", sel.Columns[1].Expr)
	}

	// Check the function name
	funcCall, ok := winFunc.Function.(*FunctionCall)
	if !ok {
		t.Fatalf("Expected *FunctionCall in WindowFunction, got %T", winFunc.Function)
	}
	if funcCall.Name != "RANK" {
		t.Errorf("Function name = %q, want 'RANK'", funcCall.Name)
	}

	// Check OVER clause exists
	if winFunc.Over == nil {
		t.Fatal("Over clause is nil, want non-nil")
	}

	// Check ORDER BY in window spec
	if len(winFunc.Over.OrderBy) != 1 {
		t.Fatalf("OrderBy count = %d, want 1", len(winFunc.Over.OrderBy))
	}

	orderExpr := winFunc.Over.OrderBy[0]
	colRef, ok := orderExpr.Expr.(*ColumnRef)
	if !ok {
		t.Fatalf("OrderBy expression type = %T, want *ColumnRef", orderExpr.Expr)
	}
	if colRef.Name != "score" {
		t.Errorf("OrderBy column = %q, want 'score'", colRef.Name)
	}
	if orderExpr.Direction != OrderDesc {
		t.Errorf("OrderBy direction = %v, want OrderDesc", orderExpr.Direction)
	}
}

func TestParser_WindowFunction_DenseRankWithOrderBy(t *testing.T) {
	input := "SELECT name, DENSE_RANK() OVER (ORDER BY score ASC) FROM users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	// Second column should be a window function
	winFunc, ok := sel.Columns[1].Expr.(*WindowFunction)
	if !ok {
		t.Fatalf("Expected *WindowFunction for column 2, got %T", sel.Columns[1].Expr)
	}

	funcCall := winFunc.Function.(*FunctionCall)
	if funcCall.Name != "DENSE_RANK" {
		t.Errorf("Function name = %q, want 'DENSE_RANK'", funcCall.Name)
	}

	if winFunc.Over.OrderBy[0].Direction != OrderAsc {
		t.Errorf("OrderBy direction = %v, want OrderAsc", winFunc.Over.OrderBy[0].Direction)
	}
}

func TestParser_WindowFunction_RANK_WithPartitionBy(t *testing.T) {
	input := "SELECT name, RANK() OVER (PARTITION BY dept ORDER BY score DESC) FROM users"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)

	winFunc, ok := sel.Columns[1].Expr.(*WindowFunction)
	if !ok {
		t.Fatalf("Expected *WindowFunction for column 2, got %T", sel.Columns[1].Expr)
	}

	// Check PARTITION BY
	if len(winFunc.Over.PartitionBy) != 1 {
		t.Fatalf("PartitionBy count = %d, want 1", len(winFunc.Over.PartitionBy))
	}

	partCol, ok := winFunc.Over.PartitionBy[0].(*ColumnRef)
	if !ok {
		t.Fatalf("PartitionBy expression type = %T, want *ColumnRef", winFunc.Over.PartitionBy[0])
	}
	if partCol.Name != "dept" {
		t.Errorf("PartitionBy column = %q, want 'dept'", partCol.Name)
	}

	// Check ORDER BY still works
	if len(winFunc.Over.OrderBy) != 1 {
		t.Fatalf("OrderBy count = %d, want 1", len(winFunc.Over.OrderBy))
	}
}

func TestParser_WindowFunction_LAG_Basic(t *testing.T) {
	input := "SELECT id, value, LAG(value) OVER (ORDER BY id) FROM data"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected *SelectStmt, got %T", stmt)
	}

	if len(sel.Columns) != 3 {
		t.Fatalf("Columns count = %d, want 3", len(sel.Columns))
	}

	// Third column should be a WindowFunction
	wf, ok := sel.Columns[2].Expr.(*WindowFunction)
	if !ok {
		t.Fatalf("Column[2].Expr type = %T, want *WindowFunction", sel.Columns[2].Expr)
	}

	// Check that the function is LAG
	funcCall, ok := wf.Function.(*FunctionCall)
	if !ok {
		t.Fatalf("WindowFunction.Function type = %T, want *FunctionCall", wf.Function)
	}
	if funcCall.Name != "LAG" {
		t.Errorf("Function name = %q, want 'LAG'", funcCall.Name)
	}

	// Check OVER clause has ORDER BY
	if wf.Over == nil {
		t.Fatal("WindowFunction.Over is nil")
	}
	if len(wf.Over.OrderBy) != 1 {
		t.Errorf("OrderBy count = %d, want 1", len(wf.Over.OrderBy))
	}
}

func TestParser_WindowFunction_LAG_WithPartition(t *testing.T) {
	input := "SELECT category, value, LAG(value, 1, 0) OVER (PARTITION BY category ORDER BY id) FROM data"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected *SelectStmt, got %T", stmt)
	}

	// Third column should be a WindowFunction
	wf, ok := sel.Columns[2].Expr.(*WindowFunction)
	if !ok {
		t.Fatalf("Column[2].Expr type = %T, want *WindowFunction", sel.Columns[2].Expr)
	}

	// Check LAG has 3 arguments
	funcCall := wf.Function.(*FunctionCall)
	if len(funcCall.Args) != 3 {
		t.Errorf("LAG args count = %d, want 3", len(funcCall.Args))
	}

	// Check OVER clause has PARTITION BY and ORDER BY
	if len(wf.Over.PartitionBy) != 1 {
		t.Errorf("PartitionBy count = %d, want 1", len(wf.Over.PartitionBy))
	}
	if len(wf.Over.OrderBy) != 1 {
		t.Errorf("OrderBy count = %d, want 1", len(wf.Over.OrderBy))
	}
}

func TestParser_WindowFunction_LEAD(t *testing.T) {
	input := "SELECT LEAD(value) OVER (ORDER BY id DESC) FROM data"
	p := New(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStmt)
	wf, ok := sel.Columns[0].Expr.(*WindowFunction)
	if !ok {
		t.Fatalf("Column[0].Expr type = %T, want *WindowFunction", sel.Columns[0].Expr)
	}

	funcCall := wf.Function.(*FunctionCall)
	if funcCall.Name != "LEAD" {
		t.Errorf("Function name = %q, want 'LEAD'", funcCall.Name)
	}

	// Check ORDER BY has DESC direction
	if wf.Over.OrderBy[0].Direction != OrderDesc {
		t.Errorf("OrderBy direction = %v, want OrderDesc", wf.Over.OrderBy[0].Direction)
	}
}
