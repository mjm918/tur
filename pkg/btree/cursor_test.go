// pkg/btree/cursor_test.go
package btree

import (
	"fmt"
	"path/filepath"
	"testing"

	"tur/pkg/pager"
)

func TestCursorIterate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, _ := pager.Open(path, pager.Options{PageSize: 4096})
	defer p.Close()

	bt, _ := Create(p)

	// Insert keys (will be stored sorted)
	keys := []string{"cherry", "apple", "banana"}
	for _, k := range keys {
		bt.Insert([]byte(k), []byte("v_"+k))
	}

	// Iterate and collect keys
	cursor := bt.Cursor()
	var collected []string

	for cursor.First(); cursor.Valid(); cursor.Next() {
		key, _ := cursor.Key(), cursor.Value()
		collected = append(collected, string(key))
	}

	// Should be in sorted order
	expected := []string{"apple", "banana", "cherry"}
	if len(collected) != len(expected) {
		t.Fatalf("expected %d keys, got %d", len(expected), len(collected))
	}
	for i, k := range expected {
		if collected[i] != k {
			t.Errorf("position %d: expected '%s', got '%s'", i, k, collected[i])
		}
	}
}

func TestCursorSeek(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, _ := pager.Open(path, pager.Options{PageSize: 4096})
	defer p.Close()

	bt, _ := Create(p)

	bt.Insert([]byte("a"), []byte("1"))
	bt.Insert([]byte("c"), []byte("3"))
	bt.Insert([]byte("e"), []byte("5"))

	cursor := bt.Cursor()

	// Seek to existing key
	cursor.Seek([]byte("c"))
	if !cursor.Valid() {
		t.Fatal("cursor should be valid after seek")
	}
	if string(cursor.Key()) != "c" {
		t.Errorf("expected 'c', got '%s'", string(cursor.Key()))
	}

	// Seek to non-existing key (should land on next key)
	cursor.Seek([]byte("b"))
	if string(cursor.Key()) != "c" {
		t.Errorf("expected 'c' after seeking 'b', got '%s'", string(cursor.Key()))
	}

	// Seek past all keys
	cursor.Seek([]byte("z"))
	if cursor.Valid() {
		t.Error("cursor should be invalid after seeking past all keys")
	}
}

func TestCursorEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, _ := pager.Open(path, pager.Options{PageSize: 4096})
	defer p.Close()

	bt, _ := Create(p)

	cursor := bt.Cursor()
	cursor.First()

	if cursor.Valid() {
		t.Error("cursor should be invalid on empty tree")
	}
}

func TestCursorMultiLevel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Use small page size to force splits
	p, err := pager.Open(path, pager.Options{PageSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	bt, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}

	// Insert 50 keys to create a multi-level tree
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		value := []byte(fmt.Sprintf("val%04d", i))
		if err := bt.Insert(key, value); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	// Verify tree is multi-level
	if bt.Depth() < 2 {
		t.Fatalf("expected depth >= 2, got %d", bt.Depth())
	}

	// Test First() and iteration
	cursor := bt.Cursor()
	var collected []string

	for cursor.First(); cursor.Valid(); cursor.Next() {
		collected = append(collected, string(cursor.Key()))
	}
	cursor.Close()

	if len(collected) != numKeys {
		t.Fatalf("expected %d keys, got %d", numKeys, len(collected))
	}

	// Verify sorted order
	for i := 0; i < numKeys; i++ {
		expected := fmt.Sprintf("key%04d", i)
		if collected[i] != expected {
			t.Errorf("position %d: expected '%s', got '%s'", i, expected, collected[i])
		}
	}

	// Test Last() and reverse iteration
	cursor = bt.Cursor()
	var reverse []string

	for cursor.Last(); cursor.Valid(); cursor.Prev() {
		reverse = append(reverse, string(cursor.Key()))
	}
	cursor.Close()

	if len(reverse) != numKeys {
		t.Fatalf("reverse: expected %d keys, got %d", numKeys, len(reverse))
	}

	// Verify reverse sorted order
	for i := 0; i < numKeys; i++ {
		expected := fmt.Sprintf("key%04d", numKeys-1-i)
		if reverse[i] != expected {
			t.Errorf("reverse position %d: expected '%s', got '%s'", i, expected, reverse[i])
		}
	}

	// Test Seek() across leaves
	cursor = bt.Cursor()
	cursor.Seek([]byte("key0025"))
	if !cursor.Valid() {
		t.Fatal("cursor should be valid after seek")
	}
	if string(cursor.Key()) != "key0025" {
		t.Errorf("expected 'key0025', got '%s'", string(cursor.Key()))
	}

	// Continue from seek position
	cursor.Next()
	if string(cursor.Key()) != "key0026" {
		t.Errorf("expected 'key0026' after next, got '%s'", string(cursor.Key()))
	}
	cursor.Close()
}
