// pkg/types/vector_test.go
package types

import (
	"math"
	"testing"
)

func TestVectorCreate(t *testing.T) {
	data := []float32{0.1, 0.2, 0.3}
	v := NewVector(data)
	if v.Dimension() != 3 {
		t.Errorf("expected dimension 3, got %d", v.Dimension())
	}
	if v.Data()[0] != 0.1 {
		t.Errorf("expected 0.1, got %f", v.Data()[0])
	}
}

func TestVectorNormalize(t *testing.T) {
	v := NewVector([]float32{3, 4})
	v.Normalize()
	// magnitude should be 1.0
	mag := float32(math.Sqrt(float64(v.Data()[0]*v.Data()[0] + v.Data()[1]*v.Data()[1])))
	if math.Abs(float64(mag-1.0)) > 0.0001 {
		t.Errorf("expected magnitude 1.0, got %f", mag)
	}
}

func TestVectorCosineDistance(t *testing.T) {
	v1 := NewVector([]float32{1, 0})
	v2 := NewVector([]float32{0, 1})
	v1.Normalize()
	v2.Normalize()
	dist := v1.CosineDistance(v2)
	// orthogonal vectors: cosine similarity = 0, distance = 1
	if math.Abs(float64(dist-1.0)) > 0.0001 {
		t.Errorf("expected distance 1.0, got %f", dist)
	}
}

func TestVectorCosineDistanceSame(t *testing.T) {
	v1 := NewVector([]float32{1, 2, 3})
	v2 := NewVector([]float32{1, 2, 3})
	v1.Normalize()
	v2.Normalize()
	dist := v1.CosineDistance(v2)
	// same vectors: cosine similarity = 1, distance = 0
	if math.Abs(float64(dist)) > 0.0001 {
		t.Errorf("expected distance 0.0, got %f", dist)
	}
}

func TestVectorToFromBytes(t *testing.T) {
	original := NewVector([]float32{1.5, 2.5, 3.5})
	bytes := original.ToBytes()
	restored, err := VectorFromBytes(bytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if restored.Dimension() != original.Dimension() {
		t.Errorf("dimension mismatch")
	}
	for i := 0; i < original.Dimension(); i++ {
		if original.Data()[i] != restored.Data()[i] {
			t.Errorf("data mismatch at %d", i)
		}
	}
}

// Tests for Multiple Distance Metrics feature

func TestDistanceMetric_Enum(t *testing.T) {
	// Test that DistanceMetric enum values exist
	if DistanceMetricCosine != 0 {
		t.Errorf("expected DistanceMetricCosine to be 0, got %d", DistanceMetricCosine)
	}
	if DistanceMetricEuclidean != 1 {
		t.Errorf("expected DistanceMetricEuclidean to be 1, got %d", DistanceMetricEuclidean)
	}
	if DistanceMetricManhattan != 2 {
		t.Errorf("expected DistanceMetricManhattan to be 2, got %d", DistanceMetricManhattan)
	}
}

func TestDistanceMetric_String(t *testing.T) {
	tests := []struct {
		metric DistanceMetric
		want   string
	}{
		{DistanceMetricCosine, "cosine"},
		{DistanceMetricEuclidean, "euclidean"},
		{DistanceMetricManhattan, "manhattan"},
	}
	for _, tt := range tests {
		got := tt.metric.String()
		if got != tt.want {
			t.Errorf("DistanceMetric(%d).String() = %q, want %q", tt.metric, got, tt.want)
		}
	}
}

func TestParseDistanceMetric(t *testing.T) {
	tests := []struct {
		input   string
		want    DistanceMetric
		wantErr bool
	}{
		{"cosine", DistanceMetricCosine, false},
		{"euclidean", DistanceMetricEuclidean, false},
		{"manhattan", DistanceMetricManhattan, false},
		{"l2", DistanceMetricEuclidean, false},
		{"l1", DistanceMetricManhattan, false},
		{"COSINE", DistanceMetricCosine, false},    // case insensitive
		{"Euclidean", DistanceMetricEuclidean, false},
		{"invalid", 0, true},
		{"", 0, true},
	}
	for _, tt := range tests {
		got, err := ParseDistanceMetric(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseDistanceMetric(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if !tt.wantErr && got != tt.want {
			t.Errorf("ParseDistanceMetric(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestVectorEuclideanDistance(t *testing.T) {
	// Test Euclidean distance (L2 norm)
	// For vectors [3,0] and [0,4], Euclidean distance = sqrt(3^2 + 4^2) = 5
	v1 := NewVector([]float32{3, 0})
	v2 := NewVector([]float32{0, 4})
	dist := v1.EuclideanDistance(v2)
	expected := float32(5.0)
	if math.Abs(float64(dist-expected)) > 0.0001 {
		t.Errorf("EuclideanDistance([3,0], [0,4]) = %f, want %f", dist, expected)
	}
}

func TestVectorEuclideanDistance_SameVector(t *testing.T) {
	v1 := NewVector([]float32{1, 2, 3})
	v2 := NewVector([]float32{1, 2, 3})
	dist := v1.EuclideanDistance(v2)
	if dist != 0 {
		t.Errorf("EuclideanDistance of same vector = %f, want 0", dist)
	}
}

func TestVectorEuclideanDistance_MismatchedDimensions(t *testing.T) {
	v1 := NewVector([]float32{1, 2, 3})
	v2 := NewVector([]float32{1, 2})
	dist := v1.EuclideanDistance(v2)
	// Should return max float32 for mismatched dimensions
	if dist != math.MaxFloat32 {
		t.Errorf("EuclideanDistance with mismatched dims = %f, want MaxFloat32", dist)
	}
}

func TestVectorManhattanDistance(t *testing.T) {
	// Test Manhattan distance (L1 norm)
	// For vectors [1,2,3] and [4,5,6], Manhattan distance = |1-4| + |2-5| + |3-6| = 3+3+3 = 9
	v1 := NewVector([]float32{1, 2, 3})
	v2 := NewVector([]float32{4, 5, 6})
	dist := v1.ManhattanDistance(v2)
	expected := float32(9.0)
	if math.Abs(float64(dist-expected)) > 0.0001 {
		t.Errorf("ManhattanDistance([1,2,3], [4,5,6]) = %f, want %f", dist, expected)
	}
}

func TestVectorManhattanDistance_SameVector(t *testing.T) {
	v1 := NewVector([]float32{1, 2, 3})
	v2 := NewVector([]float32{1, 2, 3})
	dist := v1.ManhattanDistance(v2)
	if dist != 0 {
		t.Errorf("ManhattanDistance of same vector = %f, want 0", dist)
	}
}

func TestVectorManhattanDistance_MismatchedDimensions(t *testing.T) {
	v1 := NewVector([]float32{1, 2, 3})
	v2 := NewVector([]float32{1, 2})
	dist := v1.ManhattanDistance(v2)
	// Should return max float32 for mismatched dimensions
	if dist != math.MaxFloat32 {
		t.Errorf("ManhattanDistance with mismatched dims = %f, want MaxFloat32", dist)
	}
}

func TestVectorDistance_WithMetric(t *testing.T) {
	v1 := NewVector([]float32{1, 0})
	v2 := NewVector([]float32{0, 1})
	v1.Normalize()
	v2.Normalize()

	// Test Distance method with different metrics
	cosineDist := v1.Distance(v2, DistanceMetricCosine)
	if math.Abs(float64(cosineDist-1.0)) > 0.0001 {
		t.Errorf("Distance with Cosine = %f, want 1.0", cosineDist)
	}

	// Euclidean distance between [1,0] and [0,1] = sqrt(2)
	euclideanDist := v1.Distance(v2, DistanceMetricEuclidean)
	expectedEuclidean := float32(math.Sqrt(2))
	if math.Abs(float64(euclideanDist-expectedEuclidean)) > 0.0001 {
		t.Errorf("Distance with Euclidean = %f, want %f", euclideanDist, expectedEuclidean)
	}

	// Manhattan distance between [1,0] and [0,1] = |1-0| + |0-1| = 2
	manhattanDist := v1.Distance(v2, DistanceMetricManhattan)
	if math.Abs(float64(manhattanDist-2.0)) > 0.0001 {
		t.Errorf("Distance with Manhattan = %f, want 2.0", manhattanDist)
	}
}
