// pkg/btree/btree_test.go
package btree

import (
	"path/filepath"
	"testing"

	"tur/pkg/pager"
)

func TestBTreeCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	bt, err := Create(p)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	if bt.RootPage() == 0 {
		t.Error("root page should not be 0")
	}
}

func TestBTreeInsertAndGet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	bt, err := Create(p)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// Insert a key-value pair
	if err := bt.Insert([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Get it back
	value, err := bt.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "world" {
		t.Errorf("expected 'world', got '%s'", string(value))
	}
}

func TestBTreeMultipleInserts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	bt, _ := Create(p)

	// Insert multiple keys
	keys := []string{"banana", "apple", "cherry", "date", "elderberry"}
	for _, k := range keys {
		if err := bt.Insert([]byte(k), []byte("value_"+k)); err != nil {
			t.Fatalf("insert %s failed: %v", k, err)
		}
	}

	// Verify all keys
	for _, k := range keys {
		value, err := bt.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s failed: %v", k, err)
		}
		expected := "value_" + k
		if string(value) != expected {
			t.Errorf("key %s: expected '%s', got '%s'", k, expected, string(value))
		}
	}
}

func TestBTreeNotFound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, _ := pager.Open(path, pager.Options{PageSize: 4096})
	defer p.Close()

	bt, _ := Create(p)
	bt.Insert([]byte("exists"), []byte("yes"))

	_, err := bt.Get([]byte("notfound"))
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestBTreeUpdate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, _ := pager.Open(path, pager.Options{PageSize: 4096})
	defer p.Close()

	bt, _ := Create(p)

	// Insert
	bt.Insert([]byte("key"), []byte("value1"))

	// Update (insert same key)
	bt.Insert([]byte("key"), []byte("value2"))

	// Should get updated value
	value, _ := bt.Get([]byte("key"))
	if string(value) != "value2" {
		t.Errorf("expected 'value2', got '%s'", string(value))
	}
}
