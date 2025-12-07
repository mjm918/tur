// pkg/btree/cursor_test.go
package btree

import (
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
