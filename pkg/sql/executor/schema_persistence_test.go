package executor

import (
	"path/filepath"
	"testing"

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
