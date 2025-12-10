package executor

import (
	"path/filepath"
	"testing"

	"tur/pkg/dbfile"
	"tur/pkg/pager"
)

func TestNewExecutor_InitializesSchemaBTree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_schema_init.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	exec := New(p)
	if exec.schemaBTree == nil {
		t.Fatal("Expected schema B-tree to be initialized")
	}

	// Verify schema B-tree root is page 1
	if exec.schemaBTree.RootPage() != 1 {
		t.Errorf("Expected schema B-tree root page 1, got %d", exec.schemaBTree.RootPage())
	}
}

func TestPersistSchemaEntry_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_persist_entry.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	exec := New(p)

	// Create a schema entry
	entry := &dbfile.SchemaEntry{
		Type:      dbfile.SchemaEntryTable,
		Name:      "users",
		TableName: "users",
		RootPage:  10,
		SQL:       "CREATE TABLE users (id INT)",
	}

	// Persist it
	err = exec.persistSchemaEntry(entry)
	if err != nil {
		t.Fatalf("persistSchemaEntry failed: %v", err)
	}

	// Read it back
	retrieved, err := exec.getSchemaEntry("users")
	if err != nil {
		t.Fatalf("getSchemaEntry failed: %v", err)
	}

	if retrieved.Name != entry.Name {
		t.Errorf("Name mismatch: got %s, want %s", retrieved.Name, entry.Name)
	}

	if retrieved.RootPage != entry.RootPage {
		t.Errorf("RootPage mismatch: got %d, want %d", retrieved.RootPage, entry.RootPage)
	}

	if retrieved.SQL != entry.SQL {
		t.Errorf("SQL mismatch: got %s, want %s", retrieved.SQL, entry.SQL)
	}
}

func TestCreateTable_PersistsSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_create_table_persist.db")

	// Phase 1: Create table and close
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	p.Close()

	// Phase 2: Reopen and verify schema persisted
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	table := exec2.catalog.GetTable("users")
	if table == nil {
		t.Fatal("Table 'users' not found after reopen - schema not persisted")
	}

	if len(table.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(table.Columns))
	}

	if table.Columns[0].Name != "id" {
		t.Errorf("First column should be 'id', got %s", table.Columns[0].Name)
	}
}

func TestCreateIndex_PersistsSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_create_index_persist.db")

	// Create table and index
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_email ON users(email)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	p.Close()

	// Reopen and verify
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	idx := exec2.catalog.GetIndex("idx_email")
	if idx == nil {
		t.Fatal("Index not found after reopen")
	}

	if idx.TableName != "users" {
		t.Errorf("Wrong table name: %s", idx.TableName)
	}

	if len(idx.Columns) != 1 || idx.Columns[0] != "email" {
		t.Errorf("Wrong columns: %v", idx.Columns)
	}
}

func TestDropTable_RemovesSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_drop_table.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE temp (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("DROP TABLE temp")
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}

	p.Close()

	// Reopen and verify table is gone
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	if exec2.catalog.GetTable("temp") != nil {
		t.Error("Dropped table still exists after reopen")
	}
}

func TestDropIndex_RemovesSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_drop_index.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_email ON users(email)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	_, err = exec.Execute("DROP INDEX idx_email")
	if err != nil {
		t.Fatalf("DROP INDEX failed: %v", err)
	}

	p.Close()

	// Reopen and verify index is gone
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	if exec2.catalog.GetIndex("idx_email") != nil {
		t.Error("Dropped index still exists after reopen")
	}

	// Table should still exist
	if exec2.catalog.GetTable("users") == nil {
		t.Error("Table was incorrectly removed when dropping index")
	}
}

func TestCreateView_PersistsSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_create_view_persist.db")

	// Phase 1: Create table and view, then close
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE VIEW adult_users AS SELECT id, name FROM users WHERE age >= 18")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	p.Close()

	// Phase 2: Reopen and verify view persisted
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	view := exec2.catalog.GetView("adult_users")
	if view == nil {
		t.Fatal("View 'adult_users' not found after reopen - schema not persisted")
	}

	if view.Name != "adult_users" {
		t.Errorf("Expected view name 'adult_users', got %s", view.Name)
	}
}
