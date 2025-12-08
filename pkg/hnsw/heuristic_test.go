// pkg/hnsw/heuristic_test.go
package hnsw

import (
	"math"
	"testing"

	"tur/pkg/types"
)

func TestHeuristicSelection(t *testing.T) {
	config := DefaultConfig(3)
	config.UseHeuristic = true
	config.ExtendCandidates = true
	idx := NewIndex(config)

	// Insert vectors
	for i := 0; i < 50; i++ {
		vec := types.NewVector([]float32{
			float32(math.Sin(float64(i) * 0.2)),
			float32(math.Cos(float64(i) * 0.2)),
			float32(math.Sin(float64(i) * 0.4)),
		})
		vec.Normalize()
		idx.Insert(int64(i), vec)
	}

	if idx.Len() != 50 {
		t.Errorf("expected 50 nodes, got %d", idx.Len())
	}

	// Search should work correctly
	query := types.NewVector([]float32{0.5, 0.5, 0.5})
	query.Normalize()

	results, err := idx.SearchKNN(query, 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}

	// Results should be sorted by distance
	for i := 1; i < len(results); i++ {
		if results[i].Distance < results[i-1].Distance {
			t.Error("results not sorted by distance")
		}
	}
}

func TestHeuristicVsSimple(t *testing.T) {
	// Create two indices with same data - one with heuristic, one without
	configSimple := DefaultConfig(32)
	configHeuristic := DefaultConfig(32)
	configHeuristic.UseHeuristic = true
	configHeuristic.ExtendCandidates = true

	idxSimple := NewIndex(configSimple)
	idxHeuristic := NewIndex(configHeuristic)

	// Insert same vectors into both
	for i := 0; i < 200; i++ {
		v := make([]float32, 32)
		for j := range v {
			v[j] = float32(math.Sin(float64(i*32+j) * 0.1))
		}
		vec := types.NewVector(v)
		vec.Normalize()
		idxSimple.Insert(int64(i), vec)

		vec2 := types.NewVector(v)
		vec2.Normalize()
		idxHeuristic.Insert(int64(i), vec2)
	}

	// Query both indices
	query := make([]float32, 32)
	for i := range query {
		query[i] = float32(math.Cos(float64(i) * 0.1))
	}
	queryVec := types.NewVector(query)
	queryVec.Normalize()

	resultsSimple, _ := idxSimple.SearchKNN(queryVec, 10)
	resultsHeuristic, _ := idxHeuristic.SearchKNN(queryVec, 10)

	t.Logf("Simple: best distance = %f", resultsSimple[0].Distance)
	t.Logf("Heuristic: best distance = %f", resultsHeuristic[0].Distance)

	// Both should return 10 results
	if len(resultsSimple) != 10 || len(resultsHeuristic) != 10 {
		t.Errorf("expected 10 results each, got %d and %d", len(resultsSimple), len(resultsHeuristic))
	}
}

func TestHeuristicWithSerialization(t *testing.T) {
	config := DefaultConfig(16)
	config.UseHeuristic = true
	config.ExtendCandidates = true
	idx := NewIndex(config)

	// Insert vectors
	for i := 0; i < 100; i++ {
		v := make([]float32, 16)
		for j := range v {
			v[j] = float32(math.Sin(float64(i*16 + j)))
		}
		vec := types.NewVector(v)
		vec.Normalize()
		idx.Insert(int64(i), vec)
	}

	// Serialize
	data, err := idx.SerializeToBytes()
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}

	// Deserialize
	restored, err := DeserializeFromBytes(data)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	// Verify config preserved
	if restored.config.UseHeuristic != idx.config.UseHeuristic {
		t.Errorf("UseHeuristic not preserved: %v vs %v", restored.config.UseHeuristic, idx.config.UseHeuristic)
	}

	// Verify search works
	query := make([]float32, 16)
	for i := range query {
		query[i] = float32(math.Cos(float64(i)))
	}
	queryVec := types.NewVector(query)
	queryVec.Normalize()

	origResults, _ := idx.SearchKNN(queryVec, 5)
	restoredResults, _ := restored.SearchKNN(queryVec, 5)

	for i := range origResults {
		if origResults[i].RowID != restoredResults[i].RowID {
			t.Errorf("result %d: rowID mismatch", i)
		}
	}
}

func TestHeuristicWithDelete(t *testing.T) {
	config := DefaultConfig(3)
	config.UseHeuristic = true
	config.ExtendCandidates = true
	idx := NewIndex(config)

	// Insert vectors
	for i := 0; i < 50; i++ {
		vec := types.NewVector([]float32{
			float32(math.Sin(float64(i))),
			float32(math.Cos(float64(i))),
			float32(math.Sin(float64(i) * 2)),
		})
		vec.Normalize()
		idx.Insert(int64(i+1), vec)
	}

	// Delete half
	for i := 2; i <= 50; i += 2 {
		idx.Delete(int64(i))
	}

	if idx.Len() != 25 {
		t.Errorf("expected 25 nodes, got %d", idx.Len())
	}

	// Search should still work
	query := types.NewVector([]float32{0.5, 0.5, 0.5})
	query.Normalize()

	results, err := idx.SearchKNN(query, 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	// All results should be odd numbers
	for _, r := range results {
		if r.RowID%2 == 0 {
			t.Errorf("found deleted rowID %d in results", r.RowID)
		}
	}
}
