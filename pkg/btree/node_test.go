// pkg/btree/node_test.go
package btree

import (
	"testing"
)

func TestNodeCreate(t *testing.T) {
	data := make([]byte, 4096)
	node := NewNode(data, true) // leaf node

	if !node.IsLeaf() {
		t.Error("expected leaf node")
	}
	if node.CellCount() != 0 {
		t.Errorf("expected 0 cells, got %d", node.CellCount())
	}
}

func TestNodeInsertCell(t *testing.T) {
	data := make([]byte, 4096)
	node := NewNode(data, true)

	key := []byte("key1")
	value := []byte("value1")

	if err := node.InsertCell(0, key, value); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	if node.CellCount() != 1 {
		t.Errorf("expected 1 cell, got %d", node.CellCount())
	}

	gotKey, gotValue := node.GetCell(0)
	if string(gotKey) != "key1" {
		t.Errorf("expected key 'key1', got '%s'", string(gotKey))
	}
	if string(gotValue) != "value1" {
		t.Errorf("expected value 'value1', got '%s'", string(gotValue))
	}
}

func TestNodeMultipleCells(t *testing.T) {
	data := make([]byte, 4096)
	node := NewNode(data, true)

	// Insert multiple cells
	cells := []struct{ key, value string }{
		{"apple", "red"},
		{"banana", "yellow"},
		{"cherry", "red"},
	}

	for i, c := range cells {
		if err := node.InsertCell(i, []byte(c.key), []byte(c.value)); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	if node.CellCount() != 3 {
		t.Errorf("expected 3 cells, got %d", node.CellCount())
	}

	// Verify all cells
	for i, c := range cells {
		gotKey, gotValue := node.GetCell(i)
		if string(gotKey) != c.key || string(gotValue) != c.value {
			t.Errorf("cell %d: expected (%s, %s), got (%s, %s)",
				i, c.key, c.value, string(gotKey), string(gotValue))
		}
	}
}

func TestNodeFreeSpace(t *testing.T) {
	data := make([]byte, 4096)
	node := NewNode(data, true)

	initialFree := node.FreeSpace()

	node.InsertCell(0, []byte("key"), []byte("value"))

	afterInsert := node.FreeSpace()
	if afterInsert >= initialFree {
		t.Error("free space should decrease after insert")
	}
}

func TestNodeUnderflow(t *testing.T) {
	data := make([]byte, 4096)
	node := NewNode(data, true)

	// Empty node has underflow (unless it's root)
	if !node.HasUnderflow(false) {
		t.Error("empty non-root node should have underflow")
	}

	// Root can have 0 keys
	if node.HasUnderflow(true) {
		t.Error("root node should never have underflow")
	}

	// Add one key
	node.InsertCell(0, []byte("key"), []byte("value"))

	// With 1 key, no underflow (MinKeys = 1)
	if node.HasUnderflow(false) {
		t.Error("node with 1 key should not have underflow")
	}
}

func TestNodeCanLendKey(t *testing.T) {
	data := make([]byte, 4096)
	node := NewNode(data, true)

	// Empty node can't lend
	if node.CanLendKey() {
		t.Error("empty node cannot lend key")
	}

	// 1 key = MinKeys, can't lend
	node.InsertCell(0, []byte("key1"), []byte("val1"))
	if node.CanLendKey() {
		t.Error("node at minimum should not be able to lend")
	}

	// 2 keys > MinKeys, can lend
	node.InsertCell(1, []byte("key2"), []byte("val2"))
	if !node.CanLendKey() {
		t.Error("node with 2 keys should be able to lend")
	}
}

func TestNodeRemoveFirstCell(t *testing.T) {
	data := make([]byte, 4096)
	node := NewNode(data, true)

	node.InsertCell(0, []byte("apple"), []byte("red"))
	node.InsertCell(1, []byte("banana"), []byte("yellow"))
	node.InsertCell(2, []byte("cherry"), []byte("red"))

	key, value := node.RemoveFirstCell()
	if string(key) != "apple" || string(value) != "red" {
		t.Errorf("expected (apple, red), got (%s, %s)", string(key), string(value))
	}

	if node.CellCount() != 2 {
		t.Errorf("expected 2 cells after remove, got %d", node.CellCount())
	}

	// First cell should now be banana
	k, v := node.GetCell(0)
	if string(k) != "banana" {
		t.Errorf("expected first cell to be banana, got %s", string(k))
	}
	if string(v) != "yellow" {
		t.Errorf("expected value yellow, got %s", string(v))
	}
}

func TestNodeRemoveLastCell(t *testing.T) {
	data := make([]byte, 4096)
	node := NewNode(data, true)

	node.InsertCell(0, []byte("apple"), []byte("red"))
	node.InsertCell(1, []byte("banana"), []byte("yellow"))
	node.InsertCell(2, []byte("cherry"), []byte("red"))

	key, value := node.RemoveLastCell()
	if string(key) != "cherry" || string(value) != "red" {
		t.Errorf("expected (cherry, red), got (%s, %s)", string(key), string(value))
	}

	if node.CellCount() != 2 {
		t.Errorf("expected 2 cells after remove, got %d", node.CellCount())
	}

	// Last cell should now be banana
	k, v := node.GetCell(1)
	if string(k) != "banana" {
		t.Errorf("expected last cell to be banana, got %s", string(k))
	}
	if string(v) != "yellow" {
		t.Errorf("expected value yellow, got %s", string(v))
	}
}

func TestNodePrependAppendCell(t *testing.T) {
	data := make([]byte, 4096)
	node := NewNode(data, true)

	// Start with one cell
	node.InsertCell(0, []byte("middle"), []byte("m"))

	// Prepend
	if err := node.PrependCell([]byte("first"), []byte("f")); err != nil {
		t.Fatalf("prepend failed: %v", err)
	}

	// Append
	if err := node.AppendCell([]byte("last"), []byte("l")); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	if node.CellCount() != 3 {
		t.Errorf("expected 3 cells, got %d", node.CellCount())
	}

	// Check order: first, middle, last
	k0, _ := node.GetCell(0)
	k1, _ := node.GetCell(1)
	k2, _ := node.GetCell(2)

	if string(k0) != "first" {
		t.Errorf("cell 0: expected 'first', got '%s'", string(k0))
	}
	if string(k1) != "middle" {
		t.Errorf("cell 1: expected 'middle', got '%s'", string(k1))
	}
	if string(k2) != "last" {
		t.Errorf("cell 2: expected 'last', got '%s'", string(k2))
	}
}
