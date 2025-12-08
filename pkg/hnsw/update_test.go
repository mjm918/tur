// pkg/hnsw/update_test.go
package hnsw

import (
	"testing"

	"tur/pkg/types"
)

func TestUpdate(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert a vector
	vec1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec1.Normalize()
	idx.Insert(42, vec1)

	// Verify initial state
	if !idx.Contains(42) {
		t.Error("expected index to contain rowID 42")
	}

	// Update the vector
	vec2 := types.NewVector([]float32{0.0, 1.0, 0.0})
	vec2.Normalize()
	updated, err := idx.Update(42, vec2)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if !updated {
		t.Error("expected update to return true")
	}

	// Verify the update
	if idx.Len() != 1 {
		t.Errorf("expected 1 node after update, got %d", idx.Len())
	}

	// Search should find the updated vector
	query := types.NewVector([]float32{0.0, 1.0, 0.0})
	query.Normalize()
	results, err := idx.SearchKNN(query, 1)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].RowID != 42 {
		t.Errorf("expected rowID 42, got %d", results[0].RowID)
	}
	if results[0].Distance > 0.01 {
		t.Errorf("expected distance ~0, got %f", results[0].Distance)
	}
}

func TestUpdateNonExistent(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert a vector
	vec := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec.Normalize()
	idx.Insert(42, vec)

	// Try to update non-existent rowID
	newVec := types.NewVector([]float32{0.0, 1.0, 0.0})
	newVec.Normalize()
	updated, err := idx.Update(999, newVec)
	if err != nil {
		t.Fatalf("update should not error: %v", err)
	}
	if updated {
		t.Error("expected update of non-existent rowID to return false")
	}

	// Original should still exist
	if idx.Len() != 1 {
		t.Errorf("expected 1 node, got %d", idx.Len())
	}
}

func TestUpdateDimensionMismatch(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert a vector
	vec := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec.Normalize()
	idx.Insert(42, vec)

	// Try to update with wrong dimension
	wrongDim := types.NewVector([]float32{1.0, 0.0})
	_, err := idx.Update(42, wrongDim)
	if err != ErrDimensionMismatch {
		t.Errorf("expected ErrDimensionMismatch, got %v", err)
	}
}

func TestGetByRowID(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert vectors
	vec1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec1.Normalize()
	idx.Insert(1, vec1)

	vec2 := types.NewVector([]float32{0.0, 1.0, 0.0})
	vec2.Normalize()
	idx.Insert(2, vec2)

	// Get by rowID
	found := idx.GetByRowID(1)
	if found == nil {
		t.Fatal("expected to find vector for rowID 1")
	}

	// Verify it's the correct vector
	dist := found.CosineDistance(vec1)
	if dist > 0.01 {
		t.Errorf("expected same vector, got distance %f", dist)
	}

	// Get non-existent
	notFound := idx.GetByRowID(999)
	if notFound != nil {
		t.Error("expected nil for non-existent rowID")
	}
}

func TestContains(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Empty index
	if idx.Contains(42) {
		t.Error("expected empty index to not contain any rowID")
	}

	// Insert a vector
	vec := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec.Normalize()
	idx.Insert(42, vec)

	if !idx.Contains(42) {
		t.Error("expected index to contain rowID 42")
	}
	if idx.Contains(999) {
		t.Error("expected index to not contain rowID 999")
	}

	// Delete and check again
	idx.Delete(42)
	if idx.Contains(42) {
		t.Error("expected index to not contain deleted rowID")
	}
}

func TestUpdateMultiple(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert multiple vectors pointing in x direction
	for i := 1; i <= 10; i++ {
		vec := types.NewVector([]float32{float32(i), 0.0, 0.0})
		vec.Normalize()
		idx.Insert(int64(i), vec)
	}

	if idx.Len() != 10 {
		t.Fatalf("expected 10 nodes, got %d", idx.Len())
	}

	// Update rowIDs 1-5 to point in different directions in the y-z plane
	// Use distinct angles so they have different normalized vectors
	for i := 1; i <= 5; i++ {
		angle := float32(i) * 0.3 // Different angles
		newVec := types.NewVector([]float32{0.0, float32(1.0) * angle, float32(1.0) * (1.0 - angle*0.5)})
		newVec.Normalize()
		updated, err := idx.Update(int64(i), newVec)
		if err != nil {
			t.Fatalf("update %d failed: %v", i, err)
		}
		if !updated {
			t.Errorf("expected update %d to succeed", i)
		}
	}

	// Count should be the same
	if idx.Len() != 10 {
		t.Errorf("expected 10 nodes after updates, got %d", idx.Len())
	}

	// Search for a vector in the y-z plane should find one of the updated rowIDs (1-5)
	query := types.NewVector([]float32{0.0, 0.5, 0.5})
	query.Normalize()
	results, _ := idx.SearchKNN(query, 5)
	if len(results) < 1 {
		t.Fatal("expected at least 1 result")
	}

	// The top results should be from the updated vectors (rowIDs 1-5)
	// since they're in the y-z plane while rowIDs 6-10 are in the x direction
	foundUpdated := false
	for _, r := range results {
		if r.RowID >= 1 && r.RowID <= 5 {
			foundUpdated = true
			break
		}
	}
	if !foundUpdated {
		t.Errorf("expected to find an updated rowID (1-5) in results: %v", results)
	}
}
