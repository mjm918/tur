// pkg/btree/btree_test.go
package btree

import (
	"fmt"
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

func TestBTreeDelete_FromLeafNode(t *testing.T) {
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

	// Insert keys
	keys := []string{"apple", "banana", "cherry"}
	for _, k := range keys {
		if err := bt.Insert([]byte(k), []byte("value_"+k)); err != nil {
			t.Fatalf("insert %s failed: %v", k, err)
		}
	}

	// Delete middle key
	err = bt.Delete([]byte("banana"))
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Verify deleted key is not found
	_, err = bt.Get([]byte("banana"))
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound after delete, got %v", err)
	}

	// Verify other keys still exist
	for _, k := range []string{"apple", "cherry"} {
		value, err := bt.Get([]byte(k))
		if err != nil {
			t.Errorf("get %s failed after delete: %v", k, err)
		}
		expected := "value_" + k
		if string(value) != expected {
			t.Errorf("key %s: expected '%s', got '%s'", k, expected, string(value))
		}
	}
}

func TestBTreeDelete_NonExistentKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, _ := pager.Open(path, pager.Options{PageSize: 4096})
	defer p.Close()

	bt, _ := Create(p)
	bt.Insert([]byte("exists"), []byte("yes"))

	// Try to delete non-existent key
	err := bt.Delete([]byte("notfound"))
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound when deleting non-existent key, got %v", err)
	}
}

func TestBTreeDelete_MultiLevelTree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Use smaller page size to force splits with fewer keys
	p, err := pager.Open(path, pager.Options{PageSize: 512})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	bt, err := Create(p)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// Insert enough keys to create a multi-level tree
	numKeys := 100
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key%03d", i)
		if err := bt.Insert([]byte(keys[i]), []byte(fmt.Sprintf("value%03d", i))); err != nil {
			t.Fatalf("insert %s failed: %v", keys[i], err)
		}
	}

	// Verify tree has depth > 1
	depth := bt.Depth()
	if depth < 2 {
		t.Logf("tree depth is %d, expected >= 2 for underflow testing", depth)
	}

	// Delete every other key
	for i := 0; i < numKeys; i += 2 {
		err := bt.Delete([]byte(keys[i]))
		if err != nil {
			t.Fatalf("delete %s failed: %v", keys[i], err)
		}
	}

	// Verify deleted keys are gone
	for i := 0; i < numKeys; i += 2 {
		_, err := bt.Get([]byte(keys[i]))
		if err != ErrKeyNotFound {
			t.Errorf("key %s should be deleted, got err: %v", keys[i], err)
		}
	}

	// Verify remaining keys still exist
	for i := 1; i < numKeys; i += 2 {
		value, err := bt.Get([]byte(keys[i]))
		if err != nil {
			t.Errorf("get %s failed: %v", keys[i], err)
			continue
		}
		expected := fmt.Sprintf("value%03d", i)
		if string(value) != expected {
			t.Errorf("key %s: expected '%s', got '%s'", keys[i], expected, string(value))
		}
	}

	// Verify cursor iteration still works correctly
	cursor := bt.Cursor()
	defer cursor.Close()
	cursor.First()

	count := 0
	for cursor.Valid() {
		count++
		cursor.Next()
	}

	expectedCount := numKeys / 2
	if count != expectedCount {
		t.Errorf("cursor found %d keys, expected %d", count, expectedCount)
	}
}

func TestBTreeDelete_AllKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 512})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	bt, err := Create(p)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// Insert keys
	numKeys := 50
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key%03d", i)
		if err := bt.Insert([]byte(keys[i]), []byte(fmt.Sprintf("value%03d", i))); err != nil {
			t.Fatalf("insert %s failed: %v", keys[i], err)
		}
	}

	// Delete all keys
	for i := 0; i < numKeys; i++ {
		err := bt.Delete([]byte(keys[i]))
		if err != nil {
			t.Fatalf("delete %s failed: %v", keys[i], err)
		}
	}

	// Verify all keys are gone
	for i := 0; i < numKeys; i++ {
		_, err := bt.Get([]byte(keys[i]))
		if err != ErrKeyNotFound {
			t.Errorf("key %s should be deleted", keys[i])
		}
	}

	// Verify cursor shows empty tree
	cursor := bt.Cursor()
	defer cursor.Close()
	cursor.First()
	if cursor.Valid() {
		t.Error("cursor should be invalid after deleting all keys")
	}
}

func TestBTreeDelete_DeleteFromInteriorNode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Use small page size to force multi-level tree
	p, err := pager.Open(path, pager.Options{PageSize: 256})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	bt, err := Create(p)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// Insert enough keys to create interior nodes
	numKeys := 30
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("k%02d", i)
		if err := bt.Insert([]byte(keys[i]), []byte(fmt.Sprintf("v%02d", i))); err != nil {
			t.Fatalf("insert %s failed: %v", keys[i], err)
		}
	}

	// Tree should have depth > 1
	depth := bt.Depth()
	t.Logf("tree depth: %d", depth)

	// Delete keys systematically and verify tree remains consistent
	for i := numKeys - 1; i >= 0; i-- {
		err := bt.Delete([]byte(keys[i]))
		if err != nil {
			t.Fatalf("delete %s failed: %v", keys[i], err)
		}

		// Verify deleted key is gone
		_, err = bt.Get([]byte(keys[i]))
		if err != ErrKeyNotFound {
			t.Errorf("key %s should be deleted", keys[i])
		}

		// Verify remaining keys are still accessible
		for j := 0; j < i; j++ {
			value, err := bt.Get([]byte(keys[j]))
			if err != nil {
				t.Errorf("after deleting %s, get %s failed: %v", keys[i], keys[j], err)
				continue
			}
			expected := fmt.Sprintf("v%02d", j)
			if string(value) != expected {
				t.Errorf("key %s: expected '%s', got '%s'", keys[j], expected, string(value))
			}
		}
	}
}
