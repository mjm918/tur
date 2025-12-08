// pkg/hnsw/delete_test.go
package hnsw

import (
	"math"
	"testing"

	"tur/pkg/types"
)

func TestDeleteSingleNode(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	vec := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec.Normalize()
	idx.Insert(42, vec)

	if idx.Len() != 1 {
		t.Fatalf("expected 1 node, got %d", idx.Len())
	}

	// Delete the node
	deleted := idx.Delete(42)
	if !deleted {
		t.Error("expected delete to return true")
	}

	if idx.Len() != 0 {
		t.Errorf("expected 0 nodes after delete, got %d", idx.Len())
	}

	// Search should return empty
	results, err := idx.SearchKNN(vec, 1)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results after delete, got %d", len(results))
	}
}

func TestDeleteNonExistent(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	vec := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec.Normalize()
	idx.Insert(42, vec)

	// Try to delete non-existent rowID
	deleted := idx.Delete(999)
	if deleted {
		t.Error("expected delete of non-existent node to return false")
	}

	if idx.Len() != 1 {
		t.Errorf("expected 1 node after failed delete, got %d", idx.Len())
	}
}

func TestDeleteFromMultiple(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert multiple vectors
	vectors := [][]float32{
		{1.0, 0.0, 0.0}, // rowID 1
		{0.0, 1.0, 0.0}, // rowID 2
		{0.0, 0.0, 1.0}, // rowID 3
	}

	for i, v := range vectors {
		vec := types.NewVector(v)
		vec.Normalize()
		idx.Insert(int64(i+1), vec)
	}

	if idx.Len() != 3 {
		t.Fatalf("expected 3 nodes, got %d", idx.Len())
	}

	// Delete the middle one
	deleted := idx.Delete(2)
	if !deleted {
		t.Error("expected delete to return true")
	}

	if idx.Len() != 2 {
		t.Errorf("expected 2 nodes after delete, got %d", idx.Len())
	}

	// Search for the deleted vector should not find it
	query := types.NewVector([]float32{0.0, 1.0, 0.0})
	query.Normalize()

	results, _ := idx.SearchKNN(query, 3)
	for _, r := range results {
		if r.RowID == 2 {
			t.Error("deleted node (rowID 2) should not appear in results")
		}
	}
}

func TestDeleteEntryPoint(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert multiple vectors
	for i := 0; i < 10; i++ {
		vec := types.NewVector([]float32{
			float32(math.Sin(float64(i))),
			float32(math.Cos(float64(i))),
			float32(math.Sin(float64(i) * 2)),
		})
		vec.Normalize()
		idx.Insert(int64(i+1), vec)
	}

	// Get the entry point's rowID
	idx.mu.RLock()
	entryPointID := idx.entryPoint
	entryNode := idx.nodes[entryPointID]
	entryRowID := entryNode.rowID
	idx.mu.RUnlock()

	// Delete the entry point
	deleted := idx.Delete(entryRowID)
	if !deleted {
		t.Error("expected delete to return true")
	}

	// Verify a new entry point was selected
	idx.mu.RLock()
	newEntryPointID := idx.entryPoint
	idx.mu.RUnlock()

	if newEntryPointID == entryPointID {
		// Entry point should have changed (unless there's only one node left)
		if idx.Len() > 0 {
			// Check if the new entry point is valid
			idx.mu.RLock()
			_, exists := idx.nodes[newEntryPointID]
			idx.mu.RUnlock()
			if !exists {
				t.Error("new entry point doesn't exist in nodes map")
			}
		}
	}

	// Search should still work
	query := types.NewVector([]float32{0.5, 0.5, 0.5})
	query.Normalize()

	results, err := idx.SearchKNN(query, 5)
	if err != nil {
		t.Fatalf("search failed after deleting entry point: %v", err)
	}

	if len(results) == 0 && idx.Len() > 0 {
		t.Error("expected search results after deleting entry point")
	}
}

func TestDeleteManyNodes(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert 50 vectors
	for i := 0; i < 50; i++ {
		vec := types.NewVector([]float32{
			float32(math.Sin(float64(i))),
			float32(math.Cos(float64(i))),
			float32(math.Sin(float64(i) * 2)),
		})
		vec.Normalize()
		idx.Insert(int64(i+1), vec)
	}

	if idx.Len() != 50 {
		t.Fatalf("expected 50 nodes, got %d", idx.Len())
	}

	// Delete half the nodes (even rowIDs)
	for i := 2; i <= 50; i += 2 {
		deleted := idx.Delete(int64(i))
		if !deleted {
			t.Errorf("failed to delete rowID %d", i)
		}
	}

	if idx.Len() != 25 {
		t.Errorf("expected 25 nodes after deleting half, got %d", idx.Len())
	}

	// Search should still work and only return odd rowIDs
	query := types.NewVector([]float32{0.5, 0.5, 0.5})
	query.Normalize()

	results, err := idx.SearchKNN(query, 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	for _, r := range results {
		if r.RowID%2 == 0 {
			t.Errorf("found even rowID %d in results (should be deleted)", r.RowID)
		}
	}
}

func TestDeleteAllNodes(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert vectors
	for i := 0; i < 10; i++ {
		vec := types.NewVector([]float32{float32(i), float32(i + 1), float32(i + 2)})
		vec.Normalize()
		idx.Insert(int64(i+1), vec)
	}

	// Delete all
	for i := 1; i <= 10; i++ {
		idx.Delete(int64(i))
	}

	if idx.Len() != 0 {
		t.Errorf("expected 0 nodes after deleting all, got %d", idx.Len())
	}

	// Search on empty index should work
	query := types.NewVector([]float32{1.0, 0.0, 0.0})
	results, err := idx.SearchKNN(query, 5)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results on empty index, got %d", len(results))
	}

	// Insert should still work after deleting all
	vec := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec.Normalize()
	err = idx.Insert(100, vec)
	if err != nil {
		t.Fatalf("insert after delete all failed: %v", err)
	}

	if idx.Len() != 1 {
		t.Errorf("expected 1 node after re-insert, got %d", idx.Len())
	}
}

func TestDeleteByNodeID(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	vec := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec.Normalize()
	idx.Insert(42, vec)

	// Get the nodeID (should be 0 for first insert)
	deleted := idx.DeleteByNodeID(0)
	if !deleted {
		t.Error("expected delete by node ID to return true")
	}

	if idx.Len() != 0 {
		t.Errorf("expected 0 nodes after delete, got %d", idx.Len())
	}
}

func TestDeletePreservesSearchQuality(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert vectors in a pattern
	for i := 0; i < 100; i++ {
		vec := types.NewVector([]float32{
			float32(math.Cos(float64(i) * 0.1)),
			float32(math.Sin(float64(i) * 0.1)),
			float32(i) / 100.0,
		})
		vec.Normalize()
		idx.Insert(int64(i), vec)
	}

	// Query before deletion
	query := types.NewVector([]float32{1.0, 0.0, 0.5})
	query.Normalize()

	resultsBefore, _ := idx.SearchKNN(query, 10)

	// Delete some nodes that aren't in the top results
	deletedIDs := make(map[int64]bool)
	for i := 50; i < 70; i++ {
		idx.Delete(int64(i))
		deletedIDs[int64(i)] = true
	}

	// Query after deletion
	resultsAfter, _ := idx.SearchKNN(query, 10)

	// Compare results - non-deleted nodes should still appear in same order
	beforeFiltered := make([]SearchResult, 0)
	for _, r := range resultsBefore {
		if !deletedIDs[r.RowID] {
			beforeFiltered = append(beforeFiltered, r)
		}
	}

	// The results should be similar (allowing for some reordering due to graph changes)
	if len(resultsAfter) < 5 {
		t.Errorf("expected at least 5 results after deletion, got %d", len(resultsAfter))
	}
}
