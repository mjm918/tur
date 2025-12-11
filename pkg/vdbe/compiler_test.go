// pkg/vdbe/compiler_test.go
package vdbe

import (
	"path/filepath"
	"testing"

	"tur/pkg/btree"
	"tur/pkg/pager"
	"tur/pkg/record"
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

func TestCompilerSelectSimple(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Create a B-tree and insert data
	bt, err := btree.Create(p)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// Insert rows
	for i := 1; i <= 3; i++ {
		key := make([]byte, 8)
		key[7] = byte(i)
		values := []types.Value{types.NewInt(int64(i)), types.NewText("name")}
		data := record.Encode(values)
		bt.Insert(key, data)
	}

	// Create catalog with table definition
	catalog := schema.NewCatalog()
	table := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt32},
			{Name: "name", Type: types.TypeText},
		},
		RootPage: bt.RootPage(),
	}
	catalog.CreateTable(table)

	// Parse SQL
	stmt, err := parser.New("SELECT id, name FROM users").Parse()
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	// Compile
	compiler := NewCompiler(catalog, p)
	prog, err := compiler.Compile(stmt)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	// Execute
	vm := NewVM(prog, p)
	vm.SetNumRegisters(compiler.NumRegisters())

	err = vm.Run()
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	results := vm.Results()
	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(results))
	}

	// Verify results
	for i, row := range results {
		expected := int64(i + 1)
		if row[0].Int() != expected {
			t.Errorf("row %d: expected id=%d, got %d", i, expected, row[0].Int())
		}
		if row[1].Text() != "name" {
			t.Errorf("row %d: expected name='name', got '%s'", i, row[1].Text())
		}
	}
}

func TestCompilerSelectWhere(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Create B-tree with data
	bt, _ := btree.Create(p)
	for i := 1; i <= 5; i++ {
		key := make([]byte, 8)
		key[7] = byte(i)
		values := []types.Value{types.NewInt(int64(i)), types.NewText("user")}
		record.Encode(values)
		bt.Insert(key, record.Encode(values))
	}

	// Create catalog
	catalog := schema.NewCatalog()
	table := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt32},
			{Name: "name", Type: types.TypeText},
		},
		RootPage: bt.RootPage(),
	}
	catalog.CreateTable(table)

	// Parse SQL with WHERE clause
	stmt, _ := parser.New("SELECT id FROM users WHERE id > 3").Parse()

	// Compile
	compiler := NewCompiler(catalog, p)
	prog, err := compiler.Compile(stmt)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	// Execute
	vm := NewVM(prog, p)
	vm.SetNumRegisters(compiler.NumRegisters())

	err = vm.Run()
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	results := vm.Results()
	if len(results) != 2 {
		t.Fatalf("expected 2 rows (id=4,5), got %d", len(results))
	}

	if results[0][0].Int() != 4 {
		t.Errorf("expected first row id=4, got %d", results[0][0].Int())
	}
	if results[1][0].Int() != 5 {
		t.Errorf("expected second row id=5, got %d", results[1][0].Int())
	}
}

func TestCompilerSelectStar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	bt, _ := btree.Create(p)
	key := make([]byte, 8)
	key[7] = 1
	values := []types.Value{types.NewInt(42), types.NewText("hello"), types.NewFloat(3.14)}
	bt.Insert(key, record.Encode(values))

	catalog := schema.NewCatalog()
	table := &schema.TableDef{
		Name: "test",
		Columns: []schema.ColumnDef{
			{Name: "a", Type: types.TypeInt32},
			{Name: "b", Type: types.TypeText},
			{Name: "c", Type: types.TypeFloat},
		},
		RootPage: bt.RootPage(),
	}
	catalog.CreateTable(table)

	stmt, _ := parser.New("SELECT * FROM test").Parse()

	compiler := NewCompiler(catalog, p)
	prog, err := compiler.Compile(stmt)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	vm := NewVM(prog, p)
	vm.SetNumRegisters(compiler.NumRegisters())

	err = vm.Run()
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	results := vm.Results()
	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}

	row := results[0]
	if len(row) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(row))
	}
	if row[0].Int() != 42 {
		t.Errorf("expected a=42, got %d", row[0].Int())
	}
	if row[1].Text() != "hello" {
		t.Errorf("expected b='hello', got '%s'", row[1].Text())
	}
	if row[2].Float() != 3.14 {
		t.Errorf("expected c=3.14, got %f", row[2].Float())
	}
}

func TestCompilerInsert(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	bt, _ := btree.Create(p)

	catalog := schema.NewCatalog()
	table := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt32, PrimaryKey: true},
			{Name: "name", Type: types.TypeText},
		},
		RootPage: bt.RootPage(),
	}
	catalog.CreateTable(table)

	// Compile INSERT statement
	stmt, _ := parser.New("INSERT INTO users (id, name) VALUES (1, 'Alice')").Parse()

	compiler := NewCompiler(catalog, p)
	prog, err := compiler.Compile(stmt)
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	vm := NewVM(prog, p)
	vm.SetNumRegisters(compiler.NumRegisters())

	err = vm.Run()
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Verify insert by reading back
	cursor := bt.Cursor()
	defer cursor.Close()
	cursor.First()
	if !cursor.Valid() {
		t.Fatal("expected one row after insert")
	}

	values := record.Decode(cursor.Value())
	if len(values) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(values))
	}
	if values[0].Int() != 1 {
		t.Errorf("expected id=1, got %d", values[0].Int())
	}
	if values[1].Text() != "Alice" {
		t.Errorf("expected name='Alice', got '%s'", values[1].Text())
	}
}
