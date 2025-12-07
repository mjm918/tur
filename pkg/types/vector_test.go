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
