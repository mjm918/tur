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
