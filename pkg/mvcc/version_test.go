// pkg/mvcc/version_test.go
package mvcc

import (
	"bytes"
	"testing"
)

func TestRowVersionCreate(t *testing.T) {
	data := []byte("hello world")
	v := NewRowVersion(data, 1)

	if !bytes.Equal(v.Data(), data) {
		t.Errorf("expected data %q, got %q", data, v.Data())
	}

	if v.CreatedBy() != 1 {
		t.Errorf("expected createdBy 1, got %d", v.CreatedBy())
	}

	if v.DeletedBy() != 0 {
		t.Errorf("expected deletedBy 0, got %d", v.DeletedBy())
	}

	if v.Next() != nil {
		t.Error("expected nil next pointer")
	}
}

func TestRowVersionChain(t *testing.T) {
	v1 := NewRowVersion([]byte("v1"), 1)
	v2 := NewRowVersion([]byte("v2"), 2)
	v3 := NewRowVersion([]byte("v3"), 3)

	// Link: v3 -> v2 -> v1 (newest to oldest)
	v3.SetNext(v2)
	v2.SetNext(v1)

	// Traverse chain
	current := v3
	expected := []string{"v3", "v2", "v1"}
	for i, exp := range expected {
		if current == nil {
			t.Fatalf("chain ended prematurely at index %d", i)
		}
		if string(current.Data()) != exp {
			t.Errorf("expected %q at index %d, got %q", exp, i, string(current.Data()))
		}
		current = current.Next()
	}

	if current != nil {
		t.Error("expected nil at end of chain")
	}
}

func TestRowVersionDelete(t *testing.T) {
	v := NewRowVersion([]byte("data"), 1)

	if v.IsDeleted() {
		t.Error("expected not deleted initially")
	}

	v.MarkDeleted(5)

	if !v.IsDeleted() {
		t.Error("expected deleted after marking")
	}

	if v.DeletedBy() != 5 {
		t.Errorf("expected deletedBy 5, got %d", v.DeletedBy())
	}
}

func TestVersionChainCreate(t *testing.T) {
	chain := NewVersionChain([]byte("key1"))

	if string(chain.Key()) != "key1" {
		t.Errorf("expected key %q, got %q", "key1", string(chain.Key()))
	}

	if chain.Head() != nil {
		t.Error("expected nil head for new chain")
	}
}

func TestVersionChainInsert(t *testing.T) {
	chain := NewVersionChain([]byte("key1"))

	v1 := NewRowVersion([]byte("data1"), 1)
	chain.AddVersion(v1)

	if chain.Head() != v1 {
		t.Error("expected v1 as head")
	}

	v2 := NewRowVersion([]byte("data2"), 2)
	chain.AddVersion(v2)

	// Newer version should be head
	if chain.Head() != v2 {
		t.Error("expected v2 as head")
	}

	// v1 should be after v2
	if v2.Next() != v1 {
		t.Error("expected v1 after v2")
	}
}

func TestVersionChainFindVersion(t *testing.T) {
	chain := NewVersionChain([]byte("key"))

	// Insert versions created by transactions 1, 3, 5
	v1 := NewRowVersion([]byte("data1"), 1)
	v3 := NewRowVersion([]byte("data3"), 3)
	v5 := NewRowVersion([]byte("data5"), 5)

	chain.AddVersion(v1)
	chain.AddVersion(v3)
	chain.AddVersion(v5)

	// Find by creator
	found := chain.FindVersionByCreator(3)
	if found != v3 {
		t.Error("expected to find v3")
	}

	found = chain.FindVersionByCreator(99)
	if found != nil {
		t.Error("expected nil for non-existent creator")
	}
}

func TestVersionChainLength(t *testing.T) {
	chain := NewVersionChain([]byte("key"))

	if chain.Length() != 0 {
		t.Errorf("expected length 0, got %d", chain.Length())
	}

	chain.AddVersion(NewRowVersion([]byte("v1"), 1))
	if chain.Length() != 1 {
		t.Errorf("expected length 1, got %d", chain.Length())
	}

	chain.AddVersion(NewRowVersion([]byte("v2"), 2))
	if chain.Length() != 2 {
		t.Errorf("expected length 2, got %d", chain.Length())
	}

	chain.AddVersion(NewRowVersion([]byte("v3"), 3))
	if chain.Length() != 3 {
		t.Errorf("expected length 3, got %d", chain.Length())
	}
}

func TestVersionChainPruneOldVersions(t *testing.T) {
	chain := NewVersionChain([]byte("key"))

	// Create transactions that have been committed
	mgr := NewTransactionManager()

	// Simulate 5 committed transactions creating versions
	for i := 0; i < 5; i++ {
		tx := mgr.Begin()
		v := NewRowVersion([]byte{byte(i)}, tx.ID())
		chain.AddVersion(v)
		mgr.Commit(tx)
	}

	// All versions should exist
	if chain.Length() != 5 {
		t.Errorf("expected 5 versions, got %d", chain.Length())
	}

	// Prune versions older than timestamp 3 (keeping 2 most recent)
	// In real usage, minTS would be determined by oldest active transaction
	pruned := chain.PruneOldVersions(mgr, 3)
	if pruned < 1 {
		t.Errorf("expected at least 1 pruned version, got %d", pruned)
	}
}

func TestRowVersionDataCopy(t *testing.T) {
	original := []byte("original")
	v := NewRowVersion(original, 1)

	// Modify original - should not affect version
	original[0] = 'X'

	if v.Data()[0] == 'X' {
		t.Error("version data should be a copy, not reference")
	}

	// Get data and modify - should not affect version
	data := v.Data()
	data[0] = 'Y'

	if v.Data()[0] == 'Y' {
		t.Error("Data() should return a copy")
	}
}
