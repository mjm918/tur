// pkg/mvcc/store_test.go
package mvcc

import (
	"bytes"
	"path/filepath"
	"testing"

	"tur/pkg/btree"
	"tur/pkg/pager"
)

func setupTestStore(t *testing.T) (*VersionedStore, func()) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	bt, err := btree.Create(p)
	if err != nil {
		p.Close()
		t.Fatalf("failed to create btree: %v", err)
	}

	store := NewVersionedStore(bt)

	cleanup := func() {
		p.Close()
	}

	return store, cleanup
}

func TestVersionedStoreBasicPutGet(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	tx := store.Begin()

	// Put a value
	err := store.Put(tx, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Get the value within same transaction
	value, err := store.Get(tx, []byte("key1"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if !bytes.Equal(value, []byte("value1")) {
		t.Errorf("expected value1, got %s", string(value))
	}

	store.Commit(tx)
}

func TestVersionedStoreReadAfterCommit(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	// tx1 writes
	tx1 := store.Begin()
	store.Put(tx1, []byte("key1"), []byte("value1"))
	store.Commit(tx1)

	// tx2 reads
	tx2 := store.Begin()
	value, err := store.Get(tx2, []byte("key1"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if !bytes.Equal(value, []byte("value1")) {
		t.Errorf("expected value1, got %s", string(value))
	}

	store.Commit(tx2)
}

func TestVersionedStoreSnapshotIsolation(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	// tx1 writes initial value
	tx1 := store.Begin()
	store.Put(tx1, []byte("key1"), []byte("v1"))
	store.Commit(tx1)

	// tx2 starts and reads v1
	tx2 := store.Begin()
	value, _ := store.Get(tx2, []byte("key1"))
	if string(value) != "v1" {
		t.Errorf("tx2 should see v1, got %s", string(value))
	}

	// tx3 writes v2 AFTER tx2 started
	tx3 := store.Begin()
	store.Put(tx3, []byte("key1"), []byte("v2"))
	store.Commit(tx3)

	// tx2 should STILL see v1 (snapshot isolation)
	value, _ = store.Get(tx2, []byte("key1"))
	if string(value) != "v1" {
		t.Errorf("tx2 should still see v1 due to snapshot isolation, got %s", string(value))
	}

	store.Commit(tx2)

	// New transaction should see v2
	tx4 := store.Begin()
	value, _ = store.Get(tx4, []byte("key1"))
	if string(value) != "v2" {
		t.Errorf("tx4 should see v2, got %s", string(value))
	}
	store.Commit(tx4)
}

func TestVersionedStoreDelete(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	// tx1 writes
	tx1 := store.Begin()
	store.Put(tx1, []byte("key1"), []byte("value1"))
	store.Commit(tx1)

	// tx2 deletes
	tx2 := store.Begin()
	err := store.Delete(tx2, []byte("key1"))
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	store.Commit(tx2)

	// tx3 should not see the key
	tx3 := store.Begin()
	value, err := store.Get(tx3, []byte("key1"))
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got err=%v, value=%v", err, value)
	}
	store.Commit(tx3)
}

func TestVersionedStoreDeleteSnapshotIsolation(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	// tx1 writes
	tx1 := store.Begin()
	store.Put(tx1, []byte("key1"), []byte("value1"))
	store.Commit(tx1)

	// tx2 starts
	tx2 := store.Begin()

	// tx3 deletes AFTER tx2 started
	tx3 := store.Begin()
	store.Delete(tx3, []byte("key1"))
	store.Commit(tx3)

	// tx2 should STILL see the value
	value, err := store.Get(tx2, []byte("key1"))
	if err != nil {
		t.Fatalf("tx2 should still see value: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("expected value1, got %s", string(value))
	}

	store.Commit(tx2)
}

func TestVersionedStoreWriteConflict(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	// tx1 starts writing
	tx1 := store.Begin()
	store.Put(tx1, []byte("key1"), []byte("v1"))

	// tx2 tries to write the same key
	tx2 := store.Begin()
	err := store.Put(tx2, []byte("key1"), []byte("v2"))
	if err != ErrWriteConflict {
		t.Errorf("expected ErrWriteConflict, got %v", err)
	}

	store.Commit(tx1)
	store.Rollback(tx2)
}

func TestVersionedStoreRollback(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	// tx1 writes and commits
	tx1 := store.Begin()
	store.Put(tx1, []byte("key1"), []byte("v1"))
	store.Commit(tx1)

	// tx2 updates but rolls back
	tx2 := store.Begin()
	store.Put(tx2, []byte("key1"), []byte("v2"))
	store.Rollback(tx2)

	// tx3 should see v1 (rollback reverted v2)
	tx3 := store.Begin()
	value, _ := store.Get(tx3, []byte("key1"))
	if string(value) != "v1" {
		t.Errorf("expected v1 after rollback, got %s", string(value))
	}
	store.Commit(tx3)
}

func TestVersionedStoreMultipleKeys(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	tx := store.Begin()

	// Write multiple keys
	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		value := []byte{byte('A' + i)}
		store.Put(tx, key, value)
	}

	// Read them back
	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		expected := []byte{byte('A' + i)}
		value, err := store.Get(tx, key)
		if err != nil {
			t.Errorf("get key %s failed: %v", string(key), err)
			continue
		}
		if !bytes.Equal(value, expected) {
			t.Errorf("key %s: expected %s, got %s", string(key), string(expected), string(value))
		}
	}

	store.Commit(tx)
}

func TestVersionedStoreGetNonExistent(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	tx := store.Begin()

	_, err := store.Get(tx, []byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}

	store.Commit(tx)
}

func TestVersionedStoreUpdateValue(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	tx1 := store.Begin()
	store.Put(tx1, []byte("key"), []byte("v1"))
	store.Commit(tx1)

	tx2 := store.Begin()
	store.Put(tx2, []byte("key"), []byte("v2"))
	store.Commit(tx2)

	tx3 := store.Begin()
	value, _ := store.Get(tx3, []byte("key"))
	if string(value) != "v2" {
		t.Errorf("expected v2, got %s", string(value))
	}
	store.Commit(tx3)
}

func TestVersionedStoreStats(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	// Check initial stats
	stats := store.Stats()
	if stats.ActiveTransactions != 0 {
		t.Errorf("expected 0 active transactions, got %d", stats.ActiveTransactions)
	}

	tx1 := store.Begin()
	tx2 := store.Begin()

	stats = store.Stats()
	if stats.ActiveTransactions != 2 {
		t.Errorf("expected 2 active transactions, got %d", stats.ActiveTransactions)
	}

	store.Commit(tx1)

	stats = store.Stats()
	if stats.ActiveTransactions != 1 {
		t.Errorf("expected 1 active transaction, got %d", stats.ActiveTransactions)
	}

	store.Commit(tx2)

	stats = store.Stats()
	if stats.ActiveTransactions != 0 {
		t.Errorf("expected 0 active transactions, got %d", stats.ActiveTransactions)
	}
}
