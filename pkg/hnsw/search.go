// pkg/hnsw/search.go
package hnsw

import (
	"tur/pkg/types"
)

// SearchResult represents a single search result
type SearchResult struct {
	RowID    int64
	Distance float32
}

// SearchKNN finds the k nearest neighbors to the query vector
func (idx *Index) SearchKNN(query *types.Vector, k int) ([]SearchResult, error) {
	return idx.SearchKNNWithEf(query, k, idx.config.EfSearch)
}

// SearchKNNWithEf finds the k nearest neighbors with a custom ef parameter
func (idx *Index) SearchKNNWithEf(query *types.Vector, k int, ef int) ([]SearchResult, error) {
	if query.Dimension() != idx.config.Dimension {
		return nil, ErrDimensionMismatch
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.nodes) == 0 {
		return []SearchResult{}, nil
	}

	// Start at entry point
	ep := idx.entryPoint

	// Phase 1: Greedily descend from top level to level 1
	for l := idx.maxLevel; l > 0; l-- {
		ep = idx.searchLayerClosest(query, ep, l)
	}

	// Phase 2: Search at level 0 with ef candidates
	candidates := idx.searchLayer(query, ep, ef, 0)

	// Take top k results
	if len(candidates) > k {
		candidates = candidates[:k]
	}

	// Convert to SearchResult
	results := make([]SearchResult, 0, len(candidates))
	for _, nodeID := range candidates {
		node := idx.nodes[nodeID]
		if node == nil {
			continue
		}
		results = append(results, SearchResult{
			RowID:    node.RowID(),
			Distance: query.CosineDistance(node.Vector()),
		})
	}

	// Sort by distance (should already be sorted, but ensure)
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Distance < results[i].Distance {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	return results, nil
}
