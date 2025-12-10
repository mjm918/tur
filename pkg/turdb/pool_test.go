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

func TestPool_Get_ReturnsConnection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	pool, err := OpenPool(dbPath, 2)
	if err != nil {
		t.Fatalf("OpenPool failed: %v", err)
	}
	defer pool.Close()

	// Test: Get returns a connection
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if conn == nil {
		t.Fatal("expected non-nil connection")
	}

	// Connection should be usable
	_, err = conn.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Exec on pooled connection failed: %v", err)
	}

	// Put connection back
	pool.Put(conn)
}

func TestPool_Get_ReusesConnections(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	pool, err := OpenPool(dbPath, 2)
	if err != nil {
		t.Fatalf("OpenPool failed: %v", err)
	}
	defer pool.Close()

	// Get a connection and return it
	conn1, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	pool.Put(conn1)

	// Get another connection - should be the same one
	conn2, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer pool.Put(conn2)

	// Verify numOpen is still 1 (connection was reused)
	if pool.NumOpen() != 1 {
		t.Errorf("expected NumOpen=1, got %d", pool.NumOpen())
	}
}

func TestPool_Get_RespectsMaxConns(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Note: Due to exclusive file locking, only one connection can be open
	// at a time to a given database file. The pool still provides value
	// by managing connection reuse and lifecycle.
	pool, err := OpenPool(dbPath, 1)
	if err != nil {
		t.Fatalf("OpenPool failed: %v", err)
	}
	defer pool.Close()

	// Get one connection (the max)
	conn1, err := pool.Get()
	if err != nil {
		t.Fatalf("Get #1 failed: %v", err)
	}

	// NumOpen should be 1
	if pool.NumOpen() != 1 {
		t.Errorf("expected NumOpen=1, got %d", pool.NumOpen())
	}

	// Put connection back
	pool.Put(conn1)

	// Should be able to get it again
	conn2, err := pool.Get()
	if err != nil {
		t.Fatalf("Get #2 failed: %v", err)
	}
	pool.Put(conn2)

	// NumOpen should still be 1 (reused)
	if pool.NumOpen() != 1 {
		t.Errorf("expected NumOpen=1, got %d", pool.NumOpen())
	}
}

func TestPool_Get_OnClosedPool_ReturnsError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	pool, err := OpenPool(dbPath, 2)
	if err != nil {
		t.Fatalf("OpenPool failed: %v", err)
	}

	pool.Close()

	// Test: Get on closed pool returns error
	_, err = pool.Get()
	if err != ErrPoolClosed {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}
}

func TestPool_Put_OnClosedPool_ClosesConnection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	pool, err := OpenPool(dbPath, 2)
	if err != nil {
		t.Fatalf("OpenPool failed: %v", err)
	}

	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	pool.Close()

	// Put on closed pool should close the connection
	pool.Put(conn)

	// Connection should be closed
	if !conn.IsClosed() {
		t.Error("expected connection to be closed after Put on closed pool")
	}
}
