// pkg/sql/optimizer/index_selection.go
package optimizer

import (
	"fmt"
	"strings"

	"tur/pkg/schema"
	"tur/pkg/sql/lexer"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

// IndexCandidate represents an index that could be used for a query predicate
type IndexCandidate struct {
	Index           *schema.IndexDef  // The index definition
	Column          string            // The column that matched (first matched column for multi-column)
	Operator        lexer.TokenType   // The operator used in the predicate
	Value           parser.Expression // The value being compared (for selectivity estimation)
	PrefixLength    int               // Number of index columns matched (for composite indexes)
	ExpressionMatch bool              // True if this is an expression index match
}

// FindCandidateIndexes analyzes a WHERE clause and returns all indexes
// that could potentially be used to satisfy the predicates.
// For composite indexes, it checks for prefix matching (leftmost columns first).
// For expression indexes, it matches expressions in the WHERE clause against index expressions.
func FindCandidateIndexes(table *schema.TableDef, where parser.Expression, catalog *schema.Catalog) []IndexCandidate {
	if where == nil {
		return nil
	}

	// Extract all column predicates from the WHERE clause
	predicates := extractPredicates(where)

	// Extract all expression predicates from the WHERE clause
	exprPredicates := extractExpressionPredicates(where)

	if len(predicates) == 0 && len(exprPredicates) == 0 {
		return nil
	}

	// Build a map of column -> predicate for quick lookup
	predMap := make(map[string]predicate)
	for _, pred := range predicates {
		predMap[pred.Column] = pred
	}

	// Get all indexes for this table
	indexes := catalog.GetIndexesForTable(table.Name)

	var candidates []IndexCandidate
	for _, idx := range indexes {
		// For partial indexes, check if query implies the index predicate
		if idx.IsPartial() {
			if !queryImpliesPartialIndexPredicate(idx, predicates) {
				continue // Skip this partial index
			}
		}

		// First, try to match expression indexes
		if idx.IsExpressionIndex() {
			candidate := matchExpressionIndex(idx, exprPredicates, predMap)
			if candidate != nil {
				candidates = append(candidates, *candidate)
				continue
			}
		}

		// Check if this index can be used with the column predicates
		// For composite indexes, we need to match a prefix of columns
		prefixLength := 0
		var firstPred *predicate

		for i, col := range idx.Columns {
			pred, exists := predMap[col]
			if !exists {
				// Can't skip columns in a composite index
				break
			}
			prefixLength = i + 1
			if firstPred == nil {
				firstPred = &pred
			}
		}

		// Only add if at least the first column matches
		if prefixLength > 0 && firstPred != nil {
			candidates = append(candidates, IndexCandidate{
				Index:           idx,
				Column:          firstPred.Column,
				Operator:        firstPred.Operator,
				Value:           firstPred.Value,
				PrefixLength:    prefixLength,
				ExpressionMatch: false,
			})
		}
	}

	return candidates
}

// queryImpliesPartialIndexPredicate checks if the query's predicates
// imply (are at least as restrictive as) the partial index predicate.
// For example, if index has WHERE active = 1, query must include active = 1.
func queryImpliesPartialIndexPredicate(idx *schema.IndexDef, queryPredicates []predicate) bool {
	if !idx.IsPartial() {
		return true
	}

	// Parse the index predicate
	whereSQL := "SELECT 1 FROM _dummy WHERE " + idx.WhereClause
	p := parser.New(whereSQL)
	stmt, err := p.Parse()
	if err != nil {
		return false // Can't parse = can't use
	}

	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt.Where == nil {
		return false
	}

	// Extract predicates from the index's WHERE clause
	indexPredicates := extractPredicates(selectStmt.Where)

	// Build a map of query predicates for quick lookup
	queryPredMap := make(map[string]predicate)
	for _, pred := range queryPredicates {
		queryPredMap[pred.Column] = pred
	}

	// Check that every predicate in the index is implied by a query predicate
	for _, indexPred := range indexPredicates {
		queryPred, exists := queryPredMap[indexPred.Column]
		if !exists {
			return false // Query doesn't have this predicate
		}

		// For now, check simple equality: same column, same operator, same value
		if indexPred.Operator != queryPred.Operator {
			return false
		}

		// Compare values
		if !predicateValuesMatch(indexPred.Value, queryPred.Value) {
			return false
		}
	}

	return true
}

// predicateValuesMatch checks if two predicate values are equal
func predicateValuesMatch(a, b parser.Expression) bool {
	litA, okA := a.(*parser.Literal)
	litB, okB := b.(*parser.Literal)
	if !okA || !okB {
		return false
	}

	// Compare the values
	valA := litA.Value
	valB := litB.Value

	// Use types.Compare which handles different integer types correctly
	return types.Compare(valA, valB) == 0
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

// expressionPredicate represents a predicate where the left side is an expression
type expressionPredicate struct {
	Expression parser.Expression
	Operator   lexer.TokenType
	Value      parser.Expression
}

// extractExpressionPredicates extracts predicates where one side is an expression (not just column ref)
func extractExpressionPredicates(expr parser.Expression) []expressionPredicate {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *parser.BinaryExpr:
		// Check if this is a logical operator (AND/OR)
		if e.Op == lexer.AND || e.Op == lexer.OR {
			left := extractExpressionPredicates(e.Left)
			right := extractExpressionPredicates(e.Right)
			return append(left, right...)
		}

		// Check if this is a comparison operator
		if isComparisonOperator(e.Op) {
			// Check if left is an expression (function call or binary op), not just a column
			if isIndexableExpression(e.Left) {
				return []expressionPredicate{{
					Expression: e.Left,
					Operator:   e.Op,
					Value:      e.Right,
				}}
			}
			// Check if right is an expression
			if isIndexableExpression(e.Right) {
				return []expressionPredicate{{
					Expression: e.Right,
					Operator:   e.Op,
					Value:      e.Left,
				}}
			}
		}
	}

	return nil
}

// isIndexableExpression returns true if the expression could be matched to an expression index
func isIndexableExpression(expr parser.Expression) bool {
	switch expr.(type) {
	case *parser.FunctionCall:
		return true
	case *parser.BinaryExpr:
		// Binary expressions (like price * quantity) can be indexed
		return true
	}
	return false
}

// matchExpressionIndex checks if an expression index matches any of the expression predicates
func matchExpressionIndex(idx *schema.IndexDef, exprPreds []expressionPredicate, colPreds map[string]predicate) *IndexCandidate {
	// First check if column parts match (for mixed indexes)
	prefixLength := 0
	var firstColPred *predicate
	for i, col := range idx.Columns {
		pred, exists := colPreds[col]
		if !exists {
			break
		}
		prefixLength = i + 1
		if firstColPred == nil {
			firstColPred = &pred
		}
	}

	// Now check expression parts
	for _, idxExprStr := range idx.Expressions {
		for _, exprPred := range exprPreds {
			if expressionsMatch(idxExprStr, exprPred.Expression) {
				// Found a match!
				column := ""
				if firstColPred != nil {
					column = firstColPred.Column
				}
				return &IndexCandidate{
					Index:           idx,
					Column:          column,
					Operator:        exprPred.Operator,
					Value:           exprPred.Value,
					PrefixLength:    prefixLength + 1, // Count expression as part of prefix
					ExpressionMatch: true,
				}
			}
		}
	}

	// If no expression matched but columns did, still consider as candidate
	if prefixLength > 0 && firstColPred != nil {
		return &IndexCandidate{
			Index:           idx,
			Column:          firstColPred.Column,
			Operator:        firstColPred.Operator,
			Value:           firstColPred.Value,
			PrefixLength:    prefixLength,
			ExpressionMatch: false,
		}
	}

	return nil
}

// expressionsMatch checks if an expression from a query matches an index expression string
func expressionsMatch(indexExprStr string, queryExpr parser.Expression) bool {
	// Normalize the query expression to a string
	queryExprStr := expressionToString(queryExpr)

	// Normalize both for comparison (remove extra whitespace, case-insensitive for functions)
	normalizedIdx := normalizeExpressionString(indexExprStr)
	normalizedQuery := normalizeExpressionString(queryExprStr)

	return normalizedIdx == normalizedQuery
}

// expressionToString converts a parsed expression to a normalized string
func expressionToString(expr parser.Expression) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *parser.Literal:
		return fmt.Sprintf("%v", e.Value)
	case *parser.ColumnRef:
		return e.Name
	case *parser.FunctionCall:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = expressionToString(arg)
		}
		return fmt.Sprintf("%s(%s)", strings.ToUpper(e.Name), strings.Join(args, ", "))
	case *parser.BinaryExpr:
		left := expressionToString(e.Left)
		right := expressionToString(e.Right)
		op := operatorToString(e.Op)
		return fmt.Sprintf("(%s %s %s)", left, op, right)
	case *parser.UnaryExpr:
		right := expressionToString(e.Right)
		op := operatorToString(e.Op)
		return fmt.Sprintf("%s%s", op, right)
	default:
		return ""
	}
}

// operatorToString converts a token type to its string representation
func operatorToString(op lexer.TokenType) string {
	switch op {
	case lexer.PLUS:
		return "+"
	case lexer.MINUS:
		return "-"
	case lexer.STAR:
		return "*"
	case lexer.SLASH:
		return "/"
	case lexer.EQ:
		return "="
	case lexer.NEQ:
		return "!="
	case lexer.LT:
		return "<"
	case lexer.GT:
		return ">"
	case lexer.LTE:
		return "<="
	case lexer.GTE:
		return ">="
	default:
		return ""
	}
}

// normalizeExpressionString normalizes an expression string for comparison
func normalizeExpressionString(s string) string {
	// Convert to uppercase for case-insensitive comparison of function names
	s = strings.ToUpper(s)
	// Remove all whitespace
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "\t", "")
	s = strings.ReplaceAll(s, "\n", "")
	return s
}
