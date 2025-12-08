package schema

import (
	"testing"

	"tur/pkg/types"
)

func TestColumnDef_Basic(t *testing.T) {
	col := ColumnDef{
		Name:       "id",
		Type:       types.TypeInt,
		PrimaryKey: true,
		NotNull:    true,
	}

	if col.Name != "id" {
		t.Errorf("Name: got %q, want 'id'", col.Name)
	}
	if col.Type != types.TypeInt {
		t.Errorf("Type: got %v, want TypeInt", col.Type)
	}
	if !col.PrimaryKey {
		t.Error("PrimaryKey: expected true")
	}
	if !col.NotNull {
		t.Error("NotNull: expected true")
	}
}

func TestTableDef_Basic(t *testing.T) {
	table := TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: types.TypeText, NotNull: true},
			{Name: "email", Type: types.TypeText},
		},
		RootPage: 2,
	}

	if table.Name != "users" {
		t.Errorf("Name: got %q, want 'users'", table.Name)
	}
	if len(table.Columns) != 3 {
		t.Fatalf("Columns: got %d, want 3", len(table.Columns))
	}
	if table.RootPage != 2 {
		t.Errorf("RootPage: got %d, want 2", table.RootPage)
	}
}

func TestTableDef_GetColumn(t *testing.T) {
	table := TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "name", Type: types.TypeText},
		},
	}

	col, idx := table.GetColumn("id")
	if col == nil || idx != 0 {
		t.Errorf("GetColumn('id'): got (%v, %d)", col, idx)
	}

	col, idx = table.GetColumn("name")
	if col == nil || idx != 1 {
		t.Errorf("GetColumn('name'): got (%v, %d)", col, idx)
	}

	col, idx = table.GetColumn("unknown")
	if col != nil || idx != -1 {
		t.Errorf("GetColumn('unknown'): got (%v, %d), want (nil, -1)", col, idx)
	}
}

func TestTableDef_PrimaryKeyColumn(t *testing.T) {
	table := TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt, PrimaryKey: true},
			{Name: "name", Type: types.TypeText},
		},
	}

	col, idx := table.PrimaryKeyColumn()
	if col == nil || idx != 0 || col.Name != "id" {
		t.Errorf("PrimaryKeyColumn: got (%v, %d)", col, idx)
	}

	// Table with no primary key
	tableNoPK := TableDef{
		Name: "logs",
		Columns: []ColumnDef{
			{Name: "message", Type: types.TypeText},
		},
	}

	col, idx = tableNoPK.PrimaryKeyColumn()
	if col != nil || idx != -1 {
		t.Errorf("PrimaryKeyColumn (no PK): got (%v, %d)", col, idx)
	}
}

func TestCatalog_CreateTable(t *testing.T) {
	catalog := NewCatalog()

	table := &TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		},
		RootPage: 2,
	}

	err := catalog.CreateTable(table)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Table should exist
	got := catalog.GetTable("users")
	if got == nil {
		t.Fatal("GetTable: table not found")
	}
	if got.Name != "users" {
		t.Errorf("GetTable: got name %q", got.Name)
	}
}

func TestCatalog_CreateTable_Duplicate(t *testing.T) {
	catalog := NewCatalog()

	table := &TableDef{Name: "users", Columns: []ColumnDef{}}
	catalog.CreateTable(table)

	err := catalog.CreateTable(table)
	if err == nil {
		t.Error("CreateTable: expected error for duplicate table")
	}
}

func TestCatalog_DropTable(t *testing.T) {
	catalog := NewCatalog()

	table := &TableDef{Name: "users", Columns: []ColumnDef{}}
	catalog.CreateTable(table)

	err := catalog.DropTable("users")
	if err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	if catalog.GetTable("users") != nil {
		t.Error("DropTable: table still exists")
	}
}

func TestCatalog_DropTable_NotExists(t *testing.T) {
	catalog := NewCatalog()

	err := catalog.DropTable("nonexistent")
	if err == nil {
		t.Error("DropTable: expected error for nonexistent table")
	}
}

func TestCatalog_ListTables(t *testing.T) {
	catalog := NewCatalog()

	catalog.CreateTable(&TableDef{Name: "users"})
	catalog.CreateTable(&TableDef{Name: "orders"})
	catalog.CreateTable(&TableDef{Name: "products"})

	tables := catalog.ListTables()
	if len(tables) != 3 {
		t.Fatalf("ListTables: got %d tables, want 3", len(tables))
	}

	// Check that all tables are present (order may vary)
	found := make(map[string]bool)
	for _, name := range tables {
		found[name] = true
	}
	if !found["users"] || !found["orders"] || !found["products"] {
		t.Errorf("ListTables: missing tables, got %v", tables)
	}
}
