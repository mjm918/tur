// pkg/hnsw/node_test.go
package hnsw

import (
	"testing"

	"tur/pkg/types"
)

func TestHNSWNodeCreate(t *testing.T) {
	vec := types.NewVector([]float32{1.0, 2.0, 3.0})
	node := NewHNSWNode(1, 42, vec, 2) // id=1, rowID=42, level=2

	if node.ID() != 1 {
		t.Errorf("expected ID 1, got %d", node.ID())
	}
	if node.RowID() != 42 {
		t.Errorf("expected RowID 42, got %d", node.RowID())
	}
	if node.Level() != 2 {
		t.Errorf("expected level 2, got %d", node.Level())
	}
	if node.Vector().Dimension() != 3 {
		t.Errorf("expected dimension 3, got %d", node.Vector().Dimension())
	}
}

func TestHNSWNodeNeighbors(t *testing.T) {
	vec := types.NewVector([]float32{1.0, 2.0})
	node := NewHNSWNode(1, 1, vec, 2)

	// Add neighbors at level 0
	node.AddNeighbor(0, 10)
	node.AddNeighbor(0, 20)
	node.AddNeighbor(0, 30)

	// Add neighbors at level 1
	node.AddNeighbor(1, 100)

	neighbors0 := node.Neighbors(0)
	if len(neighbors0) != 3 {
		t.Errorf("expected 3 neighbors at level 0, got %d", len(neighbors0))
	}

	neighbors1 := node.Neighbors(1)
	if len(neighbors1) != 1 {
		t.Errorf("expected 1 neighbor at level 1, got %d", len(neighbors1))
	}

	// Check invalid level
	neighbors2 := node.Neighbors(5)
	if neighbors2 != nil {
		t.Error("expected nil for invalid level")
	}
}

func TestHNSWNodeSetNeighbors(t *testing.T) {
	vec := types.NewVector([]float32{1.0})
	node := NewHNSWNode(1, 1, vec, 1)

	// Set all neighbors at once
	node.SetNeighbors(0, []uint64{5, 10, 15})

	neighbors := node.Neighbors(0)
	if len(neighbors) != 3 {
		t.Fatalf("expected 3 neighbors, got %d", len(neighbors))
	}
	if neighbors[0] != 5 || neighbors[1] != 10 || neighbors[2] != 15 {
		t.Errorf("unexpected neighbors: %v", neighbors)
	}
}
