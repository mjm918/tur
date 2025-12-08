// pkg/sql/optimizer/index_selection.go
package optimizer

import (
	"tur/pkg/schema"
	"tur/pkg/sql/lexer"
	"tur/pkg/sql/parser"
)

// IndexCandidate represents an index that could be used for a query predicate
type IndexCandidate struct {
	Index    *schema.IndexDef   // The index definition
	Column   string             // The column that matched
	Operator lexer.TokenType    // The operator used in the predicate
	Value    parser.Expression  // The value being compared (for selectivity estimation)
}

// FindCandidateIndexes analyzes a WHERE clause and returns all indexes
// that could potentially be used to satisfy the predicates.
func FindCandidateIndexes(table *schema.TableDef, where parser.Expression, catalog *schema.Catalog) []IndexCandidate {
	if where == nil {
		return nil
	}

	// Extract all column predicates from the WHERE clause
	predicates := extractPredicates(where)

	// Find indexes for each predicate
	var candidates []IndexCandidate
	for _, pred := range predicates {
		// Look up index for this column
		idx := catalog.GetIndexByColumn(table.Name, pred.Column)
		if idx != nil {
			candidates = append(candidates, IndexCandidate{
				Index:    idx,
				Column:   pred.Column,
				Operator: pred.Operator,
				Value:    pred.Value,
			})
		}
	}

	return candidates
}

// predicate represents a simple column comparison predicate
type predicate struct {
	Column   string
	Operator lexer.TokenType
	Value    parser.Expression
}

// extractPredicates extracts all simple predicates from a WHERE expression
func extractPredicates(expr parser.Expression) []predicate {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *parser.BinaryExpr:
		// Check if this is a logical operator (AND/OR)
		if e.Op == lexer.AND || e.Op == lexer.OR {
			// Recursively extract from both sides
			left := extractPredicates(e.Left)
			right := extractPredicates(e.Right)
			return append(left, right...)
		}

		// Check if this is a comparison operator
		if isComparisonOperator(e.Op) {
			// Try to extract column and value
			pred := extractSimplePredicate(e)
			if pred != nil {
				return []predicate{*pred}
			}
		}
	}

	return nil
}

// isComparisonOperator returns true if the operator is a comparison operator
func isComparisonOperator(op lexer.TokenType) bool {
	switch op {
	case lexer.EQ, lexer.NEQ, lexer.LT, lexer.GT, lexer.LTE, lexer.GTE:
		return true
	default:
		return false
	}
}

// extractSimplePredicate extracts a column predicate from a binary expression
// where one side is a column reference and the other is a literal value
func extractSimplePredicate(expr *parser.BinaryExpr) *predicate {
	// Check if left is column and right is value
	if colRef, ok := expr.Left.(*parser.ColumnRef); ok {
		if _, ok := expr.Right.(*parser.Literal); ok {
			return &predicate{
				Column:   colRef.Name,
				Operator: expr.Op,
				Value:    expr.Right,
			}
		}
	}

	// Check if right is column and left is value (e.g., 100 = id)
	if colRef, ok := expr.Right.(*parser.ColumnRef); ok {
		if _, ok := expr.Left.(*parser.Literal); ok {
			return &predicate{
				Column:   colRef.Name,
				Operator: expr.Op,
				Value:    expr.Left,
			}
		}
	}

	return nil
}

// AccessPathResult contains the recommendation for how to access a table
type AccessPathResult struct {
	RecommendedIndex *schema.IndexDef // The best index to use, nil if table scan is better
	UseTableScan     bool             // True if table scan is recommended
	EstimatedCost    float64          // Estimated cost of the recommended access path
	EstimatedRows    int64            // Estimated number of rows returned
}

// SelectBestAccessPath analyzes a WHERE clause and selects the best access path
// for a table query. It considers all available indexes and compares their costs
// against a full table scan.
func SelectBestAccessPath(table *schema.TableDef, where parser.Expression, catalog *schema.Catalog, tableRows int64) AccessPathResult {
	estimator := NewCostEstimator()

	// Calculate table scan cost as baseline
	tableScanCost, _ := estimator.EstimateTableScan(table, tableRows)

	// If no WHERE clause, table scan is the only option
	if where == nil {
		return AccessPathResult{
			RecommendedIndex: nil,
			UseTableScan:     true,
			EstimatedCost:    tableScanCost,
			EstimatedRows:    tableRows,
		}
	}

	// Find all candidate indexes
	candidates := FindCandidateIndexes(table, where, catalog)

	// If no indexes match, use table scan
	if len(candidates) == 0 {
		return AccessPathResult{
			RecommendedIndex: nil,
			UseTableScan:     true,
			EstimatedCost:    tableScanCost,
			EstimatedRows:    tableRows,
		}
	}

	// Evaluate each candidate index and find the best one
	var bestIndex *schema.IndexDef
	var bestCost float64 = tableScanCost
	var bestRows int64 = tableRows
	useTableScan := true

	for _, candidate := range candidates {
		comparison := estimator.CompareAccessPaths(table, candidate, tableRows)

		if comparison.UseIndex && comparison.IndexCost < bestCost {
			bestIndex = candidate.Index
			bestCost = comparison.IndexCost
			bestRows = comparison.IndexRows
			useTableScan = false
		}
	}

	return AccessPathResult{
		RecommendedIndex: bestIndex,
		UseTableScan:     useTableScan,
		EstimatedCost:    bestCost,
		EstimatedRows:    bestRows,
	}
}
