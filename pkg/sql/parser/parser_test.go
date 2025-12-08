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
}
