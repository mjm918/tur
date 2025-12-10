// pkg/hnsw/search_test.go
package hnsw

import (
	"math"
	"testing"

	"tur/pkg/types"
)

func TestSearchKNN(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert some vectors
	vectors := [][]float32{
		{1.0, 0.0, 0.0}, // rowID 1
		{0.9, 0.1, 0.0}, // rowID 2 - close to first
		{0.0, 1.0, 0.0}, // rowID 3
		{0.0, 0.0, 1.0}, // rowID 4
		{0.8, 0.2, 0.0}, // rowID 5 - close to first
	}

	for i, v := range vectors {
		vec := types.NewVector(v)
		vec.Normalize()
		idx.Insert(int64(i+1), vec)
	}

	// Search for vectors similar to [1, 0, 0]
	query := types.NewVector([]float32{1.0, 0.0, 0.0})
	query.Normalize()

	results, err := idx.SearchKNN(query, 3)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// First result should be rowID 1 (exact match)
	if results[0].RowID != 1 {
		t.Errorf("expected rowID 1 as first result, got %d", results[0].RowID)
	}

	// Distance should be ~0 for exact match
	if results[0].Distance > 0.01 {
		t.Errorf("expected distance ~0, got %f", results[0].Distance)
	}

	// Results should be sorted by distance
	for i := 1; i < len(results); i++ {
		if results[i].Distance < results[i-1].Distance {
			t.Errorf("results not sorted by distance")
		}
	}
}

func TestSearchKNNWithEf(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert 100 random-ish vectors
	for i := 0; i < 100; i++ {
		v := []float32{
			float32(math.Sin(float64(i))),
			float32(math.Cos(float64(i))),
			float32(math.Sin(float64(i) * 2)),
		}
		vec := types.NewVector(v)
		vec.Normalize()
		idx.Insert(int64(i+1), vec)
	}

	query := types.NewVector([]float32{1.0, 0.0, 0.0})
	query.Normalize()

	// Search with different ef values
	results10, _ := idx.SearchKNNWithEf(query, 5, 10)
	results100, _ := idx.SearchKNNWithEf(query, 5, 100)

	if len(results10) != 5 || len(results100) != 5 {
		t.Error("expected 5 results each")
	}

	// Higher ef should generally give better (or equal) results
	// (not always guaranteed due to HNSW approximation)
	t.Logf("ef=10: first result distance = %f", results10[0].Distance)
	t.Logf("ef=100: first result distance = %f", results100[0].Distance)
}

func TestSearchEmpty(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	query := types.NewVector([]float32{1.0, 0.0, 0.0})
	results, err := idx.SearchKNN(query, 5)

	if err != nil {
		t.Fatalf("search on empty index should not error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results on empty index, got %d", len(results))
	}
}

func TestSearchKNN_WithEuclideanDistance(t *testing.T) {
	config := DefaultConfig(3)
	config.DistanceMetric = types.DistanceMetricEuclidean
	idx := NewIndex(config)

	// Insert vectors (not normalized - Euclidean doesn't require normalization)
	vectors := [][]float32{
		{1.0, 0.0, 0.0}, // rowID 1
		{2.0, 0.0, 0.0}, // rowID 2 - 1 unit away from first
		{4.0, 0.0, 0.0}, // rowID 3 - 3 units away from first
		{0.0, 1.0, 0.0}, // rowID 4 - sqrt(2) away from first
	}

	for i, v := range vectors {
		vec := types.NewVector(v)
		idx.Insert(int64(i+1), vec)
	}

	// Search for vectors close to [1, 0, 0]
	query := types.NewVector([]float32{1.0, 0.0, 0.0})
	results, err := idx.SearchKNN(query, 3)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// First result should be rowID 1 (exact match, distance = 0)
	if results[0].RowID != 1 {
		t.Errorf("expected rowID 1 as first result, got %d", results[0].RowID)
	}
	if results[0].Distance > 0.01 {
		t.Errorf("expected distance ~0, got %f", results[0].Distance)
	}

	// Second result should be rowID 2 (distance = 1)
	if results[1].RowID != 2 {
		t.Errorf("expected rowID 2 as second result, got %d", results[1].RowID)
	}
	if math.Abs(float64(results[1].Distance-1.0)) > 0.01 {
		t.Errorf("expected distance ~1.0, got %f", results[1].Distance)
	}
}

func TestSearchKNN_WithManhattanDistance(t *testing.T) {
	config := DefaultConfig(3)
	config.DistanceMetric = types.DistanceMetricManhattan
	idx := NewIndex(config)

	// Insert vectors
	vectors := [][]float32{
		{1.0, 1.0, 1.0}, // rowID 1
		{2.0, 1.0, 1.0}, // rowID 2 - Manhattan distance 1 from first
		{2.0, 2.0, 1.0}, // rowID 3 - Manhattan distance 2 from first
		{2.0, 2.0, 2.0}, // rowID 4 - Manhattan distance 3 from first
	}

	for i, v := range vectors {
		vec := types.NewVector(v)
		idx.Insert(int64(i+1), vec)
	}

	// Search for vectors close to [1, 1, 1]
	query := types.NewVector([]float32{1.0, 1.0, 1.0})
	results, err := idx.SearchKNN(query, 3)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// First result should be rowID 1 (exact match, distance = 0)
	if results[0].RowID != 1 {
		t.Errorf("expected rowID 1 as first result, got %d", results[0].RowID)
	}

	// Second result should be rowID 2 (Manhattan distance = 1)
	if results[1].RowID != 2 {
		t.Errorf("expected rowID 2 as second result, got %d", results[1].RowID)
	}
	if math.Abs(float64(results[1].Distance-1.0)) > 0.01 {
		t.Errorf("expected Manhattan distance ~1.0, got %f", results[1].Distance)
	}

	// Third result should be rowID 3 (Manhattan distance = 2)
	if results[2].RowID != 3 {
		t.Errorf("expected rowID 3 as third result, got %d", results[2].RowID)
	}
	if math.Abs(float64(results[2].Distance-2.0)) > 0.01 {
		t.Errorf("expected Manhattan distance ~2.0, got %f", results[2].Distance)
	}
}

func TestConfig_DistanceMetricDefault(t *testing.T) {
	config := DefaultConfig(128)
	if config.DistanceMetric != types.DistanceMetricCosine {
		t.Errorf("default DistanceMetric should be Cosine, got %v", config.DistanceMetric)
	}
}
