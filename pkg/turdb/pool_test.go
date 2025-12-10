// pkg/turdb/pool_test.go
package turdb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPool_Basic(t *testing.T) {
	// Setup: create temp directory for database
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Test: Pool can be created with path and max connections
	pool, err := OpenPool(dbPath, 5)
	if err != nil {
		t.Fatalf("OpenPool failed: %v", err)
	}
	defer pool.Close()

	// Verify pool has expected configuration
	if pool.MaxConns() != 5 {
		t.Errorf("expected MaxConns=5, got %d", pool.MaxConns())
	}

	if pool.Path() != dbPath {
		t.Errorf("expected Path=%q, got %q", dbPath, pool.Path())
	}
}

func TestPool_MaxConnsValidation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Test: maxConns must be at least 1
	_, err = OpenPool(dbPath, 0)
	if err == nil {
		t.Error("expected error for maxConns=0, got nil")
	}

	_, err = OpenPool(dbPath, -1)
	if err == nil {
		t.Error("expected error for maxConns=-1, got nil")
	}
}
