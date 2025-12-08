// pkg/mvcc/visibility_test.go
package mvcc

import "testing"

func TestIsVersionVisibleToTransaction(t *testing.T) {
	mgr := NewTransactionManager()

	// Create and commit transaction 1 that creates a version
	tx1 := mgr.Begin()
	v := NewRowVersion([]byte("data"), tx1.ID())
	mgr.Commit(tx1)

	// Transaction 2 starts after tx1 commits
	tx2 := mgr.Begin()

	// Version should be visible to tx2 (created by committed tx before tx2 started)
	if !IsVersionVisible(v, tx2, mgr) {
		t.Error("version should be visible to tx2")
	}
}

func TestVersionNotVisibleIfCreatorUncommitted(t *testing.T) {
	mgr := NewTransactionManager()

	// Create transaction 1 but don't commit
	tx1 := mgr.Begin()
	v := NewRowVersion([]byte("data"), tx1.ID())

	// Transaction 2 starts
	tx2 := mgr.Begin()

	// Version should NOT be visible (creator not committed)
	if IsVersionVisible(v, tx2, mgr) {
		t.Error("version should not be visible (creator uncommitted)")
	}
}

func TestVersionNotVisibleIfCreatedAfterSnapshot(t *testing.T) {
	mgr := NewTransactionManager()

	// Transaction 1 starts
	tx1 := mgr.Begin()

	// Transaction 2 creates and commits a version AFTER tx1 started
	tx2 := mgr.Begin()
	v := NewRowVersion([]byte("data"), tx2.ID())
	mgr.Commit(tx2)

	// Version should NOT be visible to tx1 (created after tx1's snapshot)
	if IsVersionVisible(v, tx1, mgr) {
		t.Error("version should not be visible (created after snapshot)")
	}
}

func TestVersionNotVisibleIfDeleted(t *testing.T) {
	mgr := NewTransactionManager()

	// Create and commit version
	tx1 := mgr.Begin()
	v := NewRowVersion([]byte("data"), tx1.ID())
	mgr.Commit(tx1)

	// Delete version
	tx2 := mgr.Begin()
	v.MarkDeleted(tx2.ID())
	mgr.Commit(tx2)

	// Transaction 3 starts after deletion commits
	tx3 := mgr.Begin()

	// Version should NOT be visible (deleted before tx3 started)
	if IsVersionVisible(v, tx3, mgr) {
		t.Error("version should not be visible (deleted)")
	}
}

func TestVersionVisibleIfDeletedAfterSnapshot(t *testing.T) {
	mgr := NewTransactionManager()

	// Create and commit version
	tx1 := mgr.Begin()
	v := NewRowVersion([]byte("data"), tx1.ID())
	mgr.Commit(tx1)

	// Transaction 2 starts
	tx2 := mgr.Begin()

	// Transaction 3 deletes version AFTER tx2 started
	tx3 := mgr.Begin()
	v.MarkDeleted(tx3.ID())
	mgr.Commit(tx3)

	// Version SHOULD be visible to tx2 (deletion happened after tx2's snapshot)
	if !IsVersionVisible(v, tx2, mgr) {
		t.Error("version should be visible (deleted after snapshot)")
	}
}

func TestVersionVisibleIfDeletedButNotCommitted(t *testing.T) {
	mgr := NewTransactionManager()

	// Create and commit version
	tx1 := mgr.Begin()
	v := NewRowVersion([]byte("data"), tx1.ID())
	mgr.Commit(tx1)

	// Transaction 2 marks as deleted but doesn't commit
	tx2 := mgr.Begin()
	v.MarkDeleted(tx2.ID())

	// Transaction 3 should see the version (deletion not committed)
	tx3 := mgr.Begin()
	if !IsVersionVisible(v, tx3, mgr) {
		t.Error("version should be visible (deletion not committed)")
	}
}

func TestOwnTransactionSeesOwnUncommittedVersion(t *testing.T) {
	mgr := NewTransactionManager()

	// Transaction creates a version
	tx1 := mgr.Begin()
	v := NewRowVersion([]byte("data"), tx1.ID())

	// Same transaction should see its own uncommitted version
	if !IsVersionVisible(v, tx1, mgr) {
		t.Error("transaction should see its own uncommitted version")
	}
}

func TestOwnTransactionSeesOwnDeletedVersion(t *testing.T) {
	mgr := NewTransactionManager()

	// Create and commit version
	tx1 := mgr.Begin()
	v := NewRowVersion([]byte("data"), tx1.ID())
	mgr.Commit(tx1)

	// Transaction 2 deletes (uncommitted)
	tx2 := mgr.Begin()
	v.MarkDeleted(tx2.ID())

	// tx2 should NOT see the version (it deleted it)
	if IsVersionVisible(v, tx2, mgr) {
		t.Error("transaction should not see version it deleted")
	}
}

func TestFindVisibleVersion(t *testing.T) {
	mgr := NewTransactionManager()

	chain := NewVersionChain([]byte("key"))

	// Create three versions from committed transactions
	tx1 := mgr.Begin()
	v1 := NewRowVersion([]byte("v1"), tx1.ID())
	chain.AddVersion(v1)
	mgr.Commit(tx1)

	tx2 := mgr.Begin()
	v2 := NewRowVersion([]byte("v2"), tx2.ID())
	chain.AddVersion(v2)
	mgr.Commit(tx2)

	tx3 := mgr.Begin()
	v3 := NewRowVersion([]byte("v3"), tx3.ID())
	chain.AddVersion(v3)
	mgr.Commit(tx3)

	// Reader starts after all commits
	reader := mgr.Begin()

	// Should see v3 (most recent visible)
	visible := FindVisibleVersion(chain, reader, mgr)
	if visible == nil {
		t.Fatal("expected to find visible version")
	}
	if string(visible.Data()) != "v3" {
		t.Errorf("expected v3, got %s", string(visible.Data()))
	}
}

func TestFindVisibleVersionWithSnapshot(t *testing.T) {
	mgr := NewTransactionManager()

	chain := NewVersionChain([]byte("key"))

	// Create first version
	tx1 := mgr.Begin()
	v1 := NewRowVersion([]byte("v1"), tx1.ID())
	chain.AddVersion(v1)
	mgr.Commit(tx1)

	// Reader starts - should see v1
	reader := mgr.Begin()

	// Create second version AFTER reader started
	tx2 := mgr.Begin()
	v2 := NewRowVersion([]byte("v2"), tx2.ID())
	chain.AddVersion(v2)
	mgr.Commit(tx2)

	// Reader should still see v1 (snapshot isolation)
	visible := FindVisibleVersion(chain, reader, mgr)
	if visible == nil {
		t.Fatal("expected to find visible version")
	}
	if string(visible.Data()) != "v1" {
		t.Errorf("expected v1 due to snapshot isolation, got %s", string(visible.Data()))
	}
}

func TestFindVisibleVersionWithDeleted(t *testing.T) {
	mgr := NewTransactionManager()

	chain := NewVersionChain([]byte("key"))

	// Create two versions
	tx1 := mgr.Begin()
	v1 := NewRowVersion([]byte("v1"), tx1.ID())
	chain.AddVersion(v1)
	mgr.Commit(tx1)

	tx2 := mgr.Begin()
	v2 := NewRowVersion([]byte("v2"), tx2.ID())
	chain.AddVersion(v2)
	mgr.Commit(tx2)

	// Delete v2
	tx3 := mgr.Begin()
	v2.MarkDeleted(tx3.ID())
	mgr.Commit(tx3)

	// Reader starts after deletion
	reader := mgr.Begin()

	// Should see v1 (v2 was deleted)
	visible := FindVisibleVersion(chain, reader, mgr)
	if visible == nil {
		t.Fatal("expected to find visible version")
	}
	if string(visible.Data()) != "v1" {
		t.Errorf("expected v1 (v2 deleted), got %s", string(visible.Data()))
	}
}

func TestFindVisibleVersionEmpty(t *testing.T) {
	mgr := NewTransactionManager()

	chain := NewVersionChain([]byte("key"))
	reader := mgr.Begin()

	// Empty chain - should return nil
	visible := FindVisibleVersion(chain, reader, mgr)
	if visible != nil {
		t.Error("expected nil for empty chain")
	}
}

func TestFindVisibleVersionAllDeleted(t *testing.T) {
	mgr := NewTransactionManager()

	chain := NewVersionChain([]byte("key"))

	// Create and delete a version
	tx1 := mgr.Begin()
	v1 := NewRowVersion([]byte("v1"), tx1.ID())
	chain.AddVersion(v1)
	mgr.Commit(tx1)

	tx2 := mgr.Begin()
	v1.MarkDeleted(tx2.ID())
	mgr.Commit(tx2)

	// Reader starts after deletion
	reader := mgr.Begin()

	// No visible version
	visible := FindVisibleVersion(chain, reader, mgr)
	if visible != nil {
		t.Error("expected nil when all versions deleted")
	}
}
