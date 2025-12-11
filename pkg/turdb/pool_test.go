// pkg/turdb/pool_test.go
package turdb

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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
	_, err = conn.Exec("CREATE TABLE test (id INT PRIMARY KEY)")
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

func TestPoolOptions_MaxIdleTime(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create pool with short max idle time
	opts := PoolOptions{
		MaxIdleTime: 100 * time.Millisecond,
	}
	pool, err := OpenPoolWithPoolOptions(dbPath, 1, opts)
	if err != nil {
		t.Fatalf("OpenPoolWithPoolOptions failed: %v", err)
	}
	defer pool.Close()

	// Get and return a connection
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	pool.Put(conn)

	// Verify connection is idle
	if pool.NumIdle() != 1 {
		t.Fatalf("expected NumIdle=1, got %d", pool.NumIdle())
	}

	// Wait for connection to expire
	time.Sleep(200 * time.Millisecond)

	// Trigger cleanup
	pool.CleanupExpired()

	// Connection should have been closed
	if pool.NumIdle() != 0 {
		t.Errorf("expected NumIdle=0 after expiry, got %d", pool.NumIdle())
	}
	if pool.NumOpen() != 0 {
		t.Errorf("expected NumOpen=0 after expiry, got %d", pool.NumOpen())
	}
}

func TestPoolOptions_MaxLifetime(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Create pool with short max lifetime
	opts := PoolOptions{
		MaxLifetime: 100 * time.Millisecond,
	}
	pool, err := OpenPoolWithPoolOptions(dbPath, 1, opts)
	if err != nil {
		t.Fatalf("OpenPoolWithPoolOptions failed: %v", err)
	}
	defer pool.Close()

	// Get a connection
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Wait for connection to exceed lifetime
	time.Sleep(150 * time.Millisecond)

	// Return connection - should be closed due to lifetime exceeded
	pool.Put(conn)

	// Connection should have been closed instead of returned to pool
	if pool.NumIdle() != 0 {
		t.Errorf("expected NumIdle=0 after lifetime exceeded, got %d", pool.NumIdle())
	}
	if pool.NumOpen() != 0 {
		t.Errorf("expected NumOpen=0 after lifetime exceeded, got %d", pool.NumOpen())
	}
}

func TestPool_Close_ClosesAllConnections(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	pool, err := OpenPool(dbPath, 1)
	if err != nil {
		t.Fatalf("OpenPool failed: %v", err)
	}

	// Get and return a connection to create one
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	pool.Put(conn)

	// Verify there's an idle connection
	if pool.NumIdle() != 1 {
		t.Fatalf("expected NumIdle=1, got %d", pool.NumIdle())
	}

	// Close pool
	err = pool.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// All connections should be closed
	if pool.NumIdle() != 0 {
		t.Errorf("expected NumIdle=0 after Close, got %d", pool.NumIdle())
	}
	if pool.NumOpen() != 0 {
		t.Errorf("expected NumOpen=0 after Close, got %d", pool.NumOpen())
	}
}

func TestPool_Stats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	pool, err := OpenPool(dbPath, 1)
	if err != nil {
		t.Fatalf("OpenPool failed: %v", err)
	}
	defer pool.Close()

	// Initial stats should be zero
	stats := pool.Stats()
	if stats.TotalGets != 0 {
		t.Errorf("expected TotalGets=0, got %d", stats.TotalGets)
	}
	if stats.TotalPuts != 0 {
		t.Errorf("expected TotalPuts=0, got %d", stats.TotalPuts)
	}
	if stats.TotalCreated != 0 {
		t.Errorf("expected TotalCreated=0, got %d", stats.TotalCreated)
	}
	if stats.TotalClosed != 0 {
		t.Errorf("expected TotalClosed=0, got %d", stats.TotalClosed)
	}

	// Get a connection (should create one)
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	stats = pool.Stats()
	if stats.TotalGets != 1 {
		t.Errorf("expected TotalGets=1, got %d", stats.TotalGets)
	}
	if stats.TotalCreated != 1 {
		t.Errorf("expected TotalCreated=1, got %d", stats.TotalCreated)
	}
	if stats.HitCount != 0 {
		t.Errorf("expected HitCount=0 (first Get creates new), got %d", stats.HitCount)
	}

	// Put and Get again (should be a hit)
	pool.Put(conn)
	stats = pool.Stats()
	if stats.TotalPuts != 1 {
		t.Errorf("expected TotalPuts=1, got %d", stats.TotalPuts)
	}

	conn2, err := pool.Get()
	if err != nil {
		t.Fatalf("Get #2 failed: %v", err)
	}

	stats = pool.Stats()
	if stats.TotalGets != 2 {
		t.Errorf("expected TotalGets=2, got %d", stats.TotalGets)
	}
	if stats.HitCount != 1 {
		t.Errorf("expected HitCount=1 (second Get reused), got %d", stats.HitCount)
	}
	if stats.TotalCreated != 1 {
		t.Errorf("expected TotalCreated=1 (should reuse), got %d", stats.TotalCreated)
	}

	pool.Put(conn2)
}

func TestPool_Stats_NumInUse(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "turdb_pool_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	pool, err := OpenPool(dbPath, 1)
	if err != nil {
		t.Fatalf("OpenPool failed: %v", err)
	}
	defer pool.Close()

	// Initially no connections in use
	stats := pool.Stats()
	if stats.NumInUse != 0 {
		t.Errorf("expected NumInUse=0, got %d", stats.NumInUse)
	}

	// Get a connection
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	stats = pool.Stats()
	if stats.NumInUse != 1 {
		t.Errorf("expected NumInUse=1, got %d", stats.NumInUse)
	}
	if stats.NumIdle != 0 {
		t.Errorf("expected NumIdle=0, got %d", stats.NumIdle)
	}

	// Return connection
	pool.Put(conn)

	stats = pool.Stats()
	if stats.NumInUse != 0 {
		t.Errorf("expected NumInUse=0 after Put, got %d", stats.NumInUse)
	}
	if stats.NumIdle != 1 {
		t.Errorf("expected NumIdle=1 after Put, got %d", stats.NumIdle)
	}
}
