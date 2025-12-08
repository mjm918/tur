// pkg/pager/wal_integration_test.go
package pager

import (
	"path/filepath"
	"testing"

	"tur/pkg/wal"
)

func TestPagerWALBeginTransaction(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	p, err := Open(dbPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Begin a write transaction
	tx, err := p.BeginWrite()
	if err != nil {
		t.Fatalf("BeginWrite failed: %v", err)
	}

	// Verify transaction is active
	if !p.InTransaction() {
		t.Error("expected to be in transaction")
	}

	tx.Rollback()
}

func TestPagerWALCommit(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	p, err := Open(dbPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Begin transaction
	tx, err := p.BeginWrite()
	if err != nil {
		t.Fatalf("BeginWrite failed: %v", err)
	}

	// Allocate and modify a page
	page, err := p.Allocate()
	if err != nil {
		t.Fatalf("Allocate failed: %v", err)
	}

	// Write some data
	data := page.Data()
	p.MarkDirty(page) // Track for transaction
	data[0] = 42
	page.SetDirty(true)
	p.Release(page)

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify no longer in transaction
	if p.InTransaction() {
		t.Error("should not be in transaction after commit")
	}

	// Verify WAL file was created
	walPath := dbPath + "-wal"
	w, err := wal.Open(walPath, wal.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// WAL should have frames (or be checkpointed)
	// For a simple implementation, we might checkpoint immediately
}

func TestPagerWALRollback(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	p, err := Open(dbPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Allocate a page before transaction
	initialPage, err := p.Allocate()
	if err != nil {
		t.Fatalf("Allocate failed: %v", err)
	}
	initialPageNo := initialPage.PageNo()
	initialPage.Data()[0] = 10
	initialPage.SetDirty(true)
	p.Release(initialPage)
	p.Sync()

	// Begin transaction
	tx, err := p.BeginWrite()
	if err != nil {
		t.Fatalf("BeginWrite failed: %v", err)
	}

	// Modify the page
	page, err := p.Get(initialPageNo)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	p.MarkDirty(page)   // Track for rollback
	page.Data()[0] = 99 // Change value
	page.SetDirty(true)
	p.Release(page)

	// Rollback the transaction
	tx.Rollback()

	// Verify the page has original value
	page2, err := p.Get(initialPageNo)
	if err != nil {
		t.Fatalf("Get after rollback failed: %v", err)
	}

	if page2.Data()[0] != 10 {
		t.Errorf("expected value 10 after rollback, got %d", page2.Data()[0])
	}
	p.Release(page2)
}

func TestPagerWALRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create pager and write data
	p, err := Open(dbPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	tx, err := p.BeginWrite()
	if err != nil {
		t.Fatalf("BeginWrite failed: %v", err)
	}

	page, err := p.Allocate()
	if err != nil {
		t.Fatalf("Allocate failed: %v", err)
	}
	pageNo := page.PageNo()
	p.MarkDirty(page) // Track for transaction
	page.Data()[0] = 77
	page.SetDirty(true)
	p.Release(page)

	tx.Commit()

	// Simulate crash by not checkpointing and closing
	p.Close()

	// Reopen - should recover from WAL
	p2, err := Open(dbPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to reopen pager: %v", err)
	}
	defer p2.Close()

	// Verify data was recovered
	page2, err := p2.Get(pageNo)
	if err != nil {
		t.Fatalf("Get after recovery failed: %v", err)
	}

	if page2.Data()[0] != 77 {
		t.Errorf("expected value 77 after recovery, got %d", page2.Data()[0])
	}
	p2.Release(page2)
}
