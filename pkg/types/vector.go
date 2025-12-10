// pkg/types/vector.go
package types

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

// DistanceMetric represents the type of distance calculation to use
type DistanceMetric int

const (
	// DistanceMetricCosine uses cosine distance (1 - cosine similarity)
	DistanceMetricCosine DistanceMetric = iota
	// DistanceMetricEuclidean uses Euclidean distance (L2 norm)
	DistanceMetricEuclidean
	// DistanceMetricManhattan uses Manhattan distance (L1 norm)
	DistanceMetricManhattan
)

// String returns the string representation of the distance metric
func (m DistanceMetric) String() string {
	switch m {
	case DistanceMetricCosine:
		return "cosine"
	case DistanceMetricEuclidean:
		return "euclidean"
	case DistanceMetricManhattan:
		return "manhattan"
	default:
		return "unknown"
	}
}

// ParseDistanceMetric parses a string into a DistanceMetric
func ParseDistanceMetric(s string) (DistanceMetric, error) {
	switch strings.ToLower(s) {
	case "cosine":
		return DistanceMetricCosine, nil
	case "euclidean", "l2":
		return DistanceMetricEuclidean, nil
	case "manhattan", "l1":
		return DistanceMetricManhattan, nil
	default:
		return 0, fmt.Errorf("unknown distance metric: %q", s)
	}
}

// Vector represents a float32 vector for similarity search
type Vector struct {
	data []float32
}

// NewVector creates a new vector from float32 slice
func NewVector(data []float32) *Vector {
	// Copy to avoid external mutation
	copied := make([]float32, len(data))
	copy(copied, data)
	return &Vector{data: copied}
}

// Dimension returns the number of dimensions
func (v *Vector) Dimension() int {
	return len(v.data)
}

// Data returns the underlying float32 slice
func (v *Vector) Data() []float32 {
	return v.data
}

// Normalize normalizes the vector to unit length (for cosine similarity)
func (v *Vector) Normalize() {
	var sum float32
	for _, val := range v.data {
		sum += val * val
	}
	if sum == 0 {
		return
	}
	mag := float32(math.Sqrt(float64(sum)))
	for i := range v.data {
		v.data[i] /= mag
	}
}

// NormalizedCopy returns a new normalized copy of the vector
func (v *Vector) NormalizedCopy() *Vector {
	copied := make([]float32, len(v.data))
	copy(copied, v.data)

	var sum float32
	for _, val := range copied {
		sum += val * val
	}
	if sum == 0 {
		return &Vector{data: copied}
	}
	mag := float32(math.Sqrt(float64(sum)))
	for i := range copied {
		copied[i] /= mag
	}
	return &Vector{data: copied}
}

// DotProduct computes the dot product of two vectors
func (v *Vector) DotProduct(other *Vector) float32 {
	if len(v.data) != len(other.data) {
		return 0 // return 0 for mismatched dimensions
	}
	var dot float32
	for i := range v.data {
		dot += v.data[i] * other.data[i]
	}
	return dot
}

// CosineDistance returns 1 - dot_product (assumes normalized vectors)
func (v *Vector) CosineDistance(other *Vector) float32 {
	if len(v.data) != len(other.data) {
		return 2.0 // max distance for mismatched dimensions
	}
	var dot float32
	for i := range v.data {
		dot += v.data[i] * other.data[i]
	}
	return 1.0 - dot
}

// EuclideanDistance returns the L2 (Euclidean) distance between two vectors
func (v *Vector) EuclideanDistance(other *Vector) float32 {
	if len(v.data) != len(other.data) {
		return math.MaxFloat32 // max distance for mismatched dimensions
	}
	var sum float32
	for i := range v.data {
		diff := v.data[i] - other.data[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

// ManhattanDistance returns the L1 (Manhattan) distance between two vectors
func (v *Vector) ManhattanDistance(other *Vector) float32 {
	if len(v.data) != len(other.data) {
		return math.MaxFloat32 // max distance for mismatched dimensions
	}
	var sum float32
	for i := range v.data {
		diff := v.data[i] - other.data[i]
		if diff < 0 {
			diff = -diff
		}
		sum += diff
	}
	return sum
}

// Distance computes the distance between two vectors using the specified metric
func (v *Vector) Distance(other *Vector, metric DistanceMetric) float32 {
	switch metric {
	case DistanceMetricCosine:
		return v.CosineDistance(other)
	case DistanceMetricEuclidean:
		return v.EuclideanDistance(other)
	case DistanceMetricManhattan:
		return v.ManhattanDistance(other)
	default:
		return v.CosineDistance(other) // default to cosine
	}
}

// ToBytes serializes vector to bytes (little-endian float32)
func (v *Vector) ToBytes() []byte {
	buf := make([]byte, 4+len(v.data)*4) // 4 bytes for dimension + data
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(v.data)))
	for i, val := range v.data {
		binary.LittleEndian.PutUint32(buf[4+i*4:], math.Float32bits(val))
	}
	return buf
}

// VectorFromBytes deserializes vector from bytes
func VectorFromBytes(data []byte) (*Vector, error) {
	if len(data) < 4 {
		return nil, errors.New("invalid vector data: too short")
	}
	dim := binary.LittleEndian.Uint32(data[0:4])
	if len(data) < 4+int(dim)*4 {
		return nil, errors.New("invalid vector data: incomplete")
	}
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[4+i*4:]))
	}
	return &Vector{data: vec}, nil
}
