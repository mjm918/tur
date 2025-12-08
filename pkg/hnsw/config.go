// pkg/hnsw/config.go
package hnsw

import "math"

// Config holds HNSW index parameters
type Config struct {
	// M is the maximum number of connections per node at layers > 0
	M int

	// MMax0 is the maximum number of connections at layer 0
	MMax0 int

	// EfConstruction is the size of the dynamic candidate list during construction
	EfConstruction int

	// EfSearch is the default size of the dynamic candidate list during search
	EfSearch int

	// Dimension is the vector dimension
	Dimension int

	// ML is the level generation factor (1/ln(M))
	ML float64

	// UseHeuristic enables the heuristic neighbor selection algorithm
	// from the HNSW paper for better graph quality (slightly slower construction)
	UseHeuristic bool

	// ExtendCandidates enables extending candidates with their neighbors
	// during heuristic selection (recommended when UseHeuristic is true)
	ExtendCandidates bool
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig(dimension int) Config {
	m := 16
	return Config{
		M:              m,
		MMax0:          m * 2,
		EfConstruction: 200,
		EfSearch:       50,
		Dimension:      dimension,
		ML:             1.0 / math.Log(float64(m)),
	}
}
