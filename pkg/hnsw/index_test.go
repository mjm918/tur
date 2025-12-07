// pkg/hnsw/index_test.go
package hnsw

import (
	"testing"

	"tur/pkg/types"
)

func TestIndexCreate(t *testing.T) {
	config := DefaultConfig(128) // 128 dimensions
	idx := NewIndex(config)

	if idx.Len() != 0 {
		t.Errorf("expected empty index, got %d nodes", idx.Len())
	}
	if idx.Dimension() != 128 {
		t.Errorf("expected dimension 128, got %d", idx.Dimension())
	}
}

func TestIndexInsertOne(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	vec := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec.Normalize()

	err := idx.Insert(1, vec)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	if idx.Len() != 1 {
		t.Errorf("expected 1 node, got %d", idx.Len())
	}
}

func TestIndexInsertMultiple(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	vectors := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
		{1.0, 1.0, 0.0},
		{1.0, 0.0, 1.0},
	}

	for i, v := range vectors {
		vec := types.NewVector(v)
		vec.Normalize()
		if err := idx.Insert(int64(i+1), vec); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	if idx.Len() != 5 {
		t.Errorf("expected 5 nodes, got %d", idx.Len())
	}
}
