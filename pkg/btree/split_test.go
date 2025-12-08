// pkg/btree/split_test.go
package btree

import (
	"fmt"
	"path/filepath"
	"testing"

	"tur/pkg/pager"
)

func TestBTreeSplitLeaf(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	bt, _ := Create(p)

	// Insert enough keys to force a split
	// With 4096 byte pages, ~100 small keys should trigger split
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		if err := bt.Insert([]byte(key), []byte(value)); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	// Verify all keys can be retrieved
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("key%05d", i)
		expected := fmt.Sprintf("value%05d", i)
		value, err := bt.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %s failed: %v", key, err)
		}
		if string(value) != expected {
			t.Errorf("key %s: expected '%s', got '%s'", key, expected, string(value))
		}
	}
}

func TestBTreeSplitMaintainsSortOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, _ := pager.Open(path, pager.Options{PageSize: 4096})
	defer p.Close()

	bt, _ := Create(p)

	// Insert in reverse order to stress sorting
	for i := 99; i >= 0; i-- {
		key := fmt.Sprintf("key%02d", i)
		bt.Insert([]byte(key), []byte("v"))
	}

	// Iterate and verify sorted order
	cursor := bt.Cursor()
	defer cursor.Close()

	var prev string
	count := 0
	for cursor.First(); cursor.Valid(); cursor.Next() {
		key := string(cursor.Key())
		if prev != "" && key <= prev {
			t.Errorf("keys not sorted: %s came after %s", key, prev)
		}
		prev = key
		count++
	}

	if count != 100 {
		t.Errorf("expected 100 keys, got %d", count)
	}
}

func TestBTreeSplitSmallPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Use smaller page to force more splits
	p, _ := pager.Open(path, pager.Options{PageSize: 512})
	defer p.Close()

	bt, _ := Create(p)

	// Insert keys - should trigger multiple splits with small pages
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("val%04d", i)
		if err := bt.Insert([]byte(key), []byte(value)); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	// Verify all keys
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		expected := fmt.Sprintf("val%04d", i)
		value, err := bt.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %s failed: %v", key, err)
		}
		if string(value) != expected {
			t.Errorf("key %s: expected '%s', got '%s'", key, expected, string(value))
		}
	}
}

func TestBTreeInteriorSplit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Use very small page size to force interior node splits
	p, err := pager.Open(path, pager.Options{PageSize: 256})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	bt, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}

	// Insert many keys to trigger interior node splits (depth > 2)
	numKeys := 200
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("val%04d", i)
		if err := bt.Insert([]byte(key), []byte(value)); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	t.Logf("Tree depth: %d, pages: %d", bt.Depth(), p.PageCount())

	// Verify all keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		expected := fmt.Sprintf("val%04d", i)
		value, err := bt.Get([]byte(key))
		if err != nil {
			t.Errorf("get %s failed: %v", key, err)
			continue
		}
		if string(value) != expected {
			t.Errorf("key %s: expected '%s', got '%s'", key, expected, string(value))
		}
	}

	// Verify cursor iteration returns all keys in order
	cursor := bt.Cursor()
	count := 0
	var prev string
	for cursor.First(); cursor.Valid(); cursor.Next() {
		key := string(cursor.Key())
		if prev != "" && key <= prev {
			t.Errorf("keys not sorted: %s came after %s", key, prev)
		}
		prev = key
		count++
	}
	cursor.Close()

	if count != numKeys {
		t.Errorf("cursor counted %d keys, expected %d", count, numKeys)
	}
}

func TestBTreeDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	bt, _ := Create(p)

	// Insert keys
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("val%04d", i)
		bt.Insert([]byte(key), []byte(value))
	}

	// Delete some keys
	for i := 0; i < numKeys; i += 2 {
		key := fmt.Sprintf("key%04d", i)
		if err := bt.Delete([]byte(key)); err != nil {
			t.Errorf("delete %s failed: %v", key, err)
		}
	}

	// Verify deleted keys are gone
	for i := 0; i < numKeys; i += 2 {
		key := fmt.Sprintf("key%04d", i)
		_, err := bt.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("key %s should be deleted", key)
		}
	}

	// Verify remaining keys exist
	for i := 1; i < numKeys; i += 2 {
		key := fmt.Sprintf("key%04d", i)
		expected := fmt.Sprintf("val%04d", i)
		value, err := bt.Get([]byte(key))
		if err != nil {
			t.Errorf("get %s failed: %v", key, err)
			continue
		}
		if string(value) != expected {
			t.Errorf("key %s: expected '%s', got '%s'", key, expected, string(value))
		}
	}

	// Cursor should only return remaining keys
	cursor := bt.Cursor()
	count := 0
	for cursor.First(); cursor.Valid(); cursor.Next() {
		count++
	}
	cursor.Close()

	expectedCount := numKeys / 2
	if count != expectedCount {
		t.Errorf("cursor counted %d keys, expected %d", count, expectedCount)
	}
}

func TestBTreePersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	numKeys := 50
	var rootPage uint32

	// Phase 1: Create and populate tree
	{
		p, err := pager.Open(path, pager.Options{PageSize: 512})
		if err != nil {
			t.Fatalf("failed to open pager: %v", err)
		}

		bt, err := Create(p)
		if err != nil {
			p.Close()
			t.Fatalf("failed to create btree: %v", err)
		}

		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%04d", i)
			value := fmt.Sprintf("val%04d", i)
			if err := bt.Insert([]byte(key), []byte(value)); err != nil {
				p.Close()
				t.Fatalf("insert %d failed: %v", i, err)
			}
		}

		rootPage = bt.RootPage()
		t.Logf("Created tree with root=%d, depth=%d", rootPage, bt.Depth())

		// Close to flush to disk
		if err := p.Close(); err != nil {
			t.Fatalf("failed to close pager: %v", err)
		}
	}

	// Phase 2: Reopen and verify
	{
		p, err := pager.Open(path, pager.Options{PageSize: 512})
		if err != nil {
			t.Fatalf("failed to reopen pager: %v", err)
		}
		defer p.Close()

		// Open existing tree
		bt := Open(p, rootPage)

		// Verify all keys
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%04d", i)
			expected := fmt.Sprintf("val%04d", i)
			value, err := bt.Get([]byte(key))
			if err != nil {
				t.Errorf("get %s failed: %v", key, err)
				continue
			}
			if string(value) != expected {
				t.Errorf("key %s: expected '%s', got '%s'", key, expected, string(value))
			}
		}

		// Verify cursor iteration
		cursor := bt.Cursor()
		count := 0
		for cursor.First(); cursor.Valid(); cursor.Next() {
			count++
		}
		cursor.Close()

		if count != numKeys {
			t.Errorf("cursor counted %d keys, expected %d", count, numKeys)
		}
	}
}
