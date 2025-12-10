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
