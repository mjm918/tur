// pkg/sql/optimizer/cost.go
package optimizer

import (
	"tur/pkg/schema"
)

// Cost constants for estimation
// These values are based on typical database system costs
const (
	PAGE_READ_COST  = 1.0   // Cost to read one page from disk
	CPU_TUPLE_COST  = 0.01  // Cost to process one tuple in memory
	INDEX_SCAN_COST = 0.005 // Cost per tuple for index lookup
	ROWS_PER_PAGE   = 100   // Estimated average rows per page
)

// CostEstimator estimates query execution costs
type CostEstimator struct {
	// In the future, this could hold statistics from the database
}

// NewCostEstimator creates a new cost estimator
func NewCostEstimator() *CostEstimator {
	return &CostEstimator{}
}

// EstimateTableScan estimates the cost of scanning a full table
// Returns (cost, estimatedRows)
func (e *CostEstimator) EstimateTableScan(table *schema.TableDef, tableRows int64) (float64, int64) {
	// Even empty tables have a minimum cost (reading root page)
	if tableRows == 0 {
		return PAGE_READ_COST, 0
	}

	// Calculate number of pages needed
	// Assuming rows are packed into pages
	numPages := (tableRows + ROWS_PER_PAGE - 1) / ROWS_PER_PAGE

	// Cost = (number of pages * cost to read page) + (number of rows * CPU cost)
	ioCost := float64(numPages) * PAGE_READ_COST
	cpuCost := float64(tableRows) * CPU_TUPLE_COST
	totalCost := ioCost + cpuCost

	return totalCost, tableRows
}

// EstimateSelectivity estimates the selectivity of a predicate
// Returns a value between 0.0 and 1.0 representing the fraction of rows
// that will pass the predicate
func (e *CostEstimator) EstimateSelectivity(operator string) float64 {
	// These are default selectivity estimates without statistics
	// In a real optimizer, these would be refined using column statistics
	switch operator {
	case "=":
		return 0.01 // 1% - equality is highly selective
	case "!=":
		return 0.9 // 90% - inequality is not very selective
	case "<", ">", "<=", ">=":
		return 0.33 // 33% - range operators are moderately selective
	case "LIKE":
		return 0.1 // 10% - pattern matching is fairly selective
	case "IN":
		return 0.05 // 5% - IN clause selectivity depends on list size
	case "IS NULL":
		return 0.01 // 1% - most columns are not null
	case "IS NOT NULL":
		return 0.99 // 99% - most columns are not null
	default:
		return 0.1 // Default conservative estimate
	}
}

// EstimateCombinedSelectivity estimates selectivity for multiple predicates
// combined with AND or OR
func (e *CostEstimator) EstimateCombinedSelectivity(selectivities []float64, operator string) float64 {
	if len(selectivities) == 0 {
		return 1.0
	}

	if operator == "AND" {
		// For AND: multiply selectivities (assumes independence)
		result := 1.0
		for _, sel := range selectivities {
			result *= sel
		}
		return result
	}

	// For OR: use inclusion-exclusion principle
	// P(A OR B) = P(A) + P(B) - P(A AND B)
	// Simplified: P(A OR B) ≈ P(A) + P(B) - P(A)*P(B)
	result := 0.0
	for _, sel := range selectivities {
		result = result + sel - (result * sel)
	}
	return result
}
