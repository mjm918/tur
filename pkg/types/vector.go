// pkg/types/vector.go
package types

import (
	"encoding/binary"
	"errors"
	"math"
)

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
