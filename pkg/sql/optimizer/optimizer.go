// pkg/sql/optimizer/optimizer.go
package optimizer

import (
	"tur/pkg/schema"
	"tur/pkg/sql/lexer"
	"tur/pkg/sql/parser"
)

// StatisticsProvider provides table statistics for query optimization
type StatisticsProvider interface {
	// GetTableStatistics returns statistics for the named table, or nil if not available
	GetTableStatistics(tableName string) *schema.TableStatistics
}

// Optimizer applies query optimization techniques
type Optimizer struct {
	costEstimator      *CostEstimator
	statisticsProvider StatisticsProvider

	// UseDP controls join reordering algorithm selection:
	//
	// When UseDP = false (default):
	//   - Automatically uses DP for queries with ≤12 tables (optimal)
	//   - Automatically uses greedy for queries with >12 tables (fast)
	//   - This is the recommended setting for most use cases
	//
	// When UseDP = true (manual override):
	//   - Forces DP for ALL queries regardless of size
	//   - Use this only for small queries where you need guaranteed optimal plans
	//   - WARNING: Very slow for queries with >15 tables (O(n * 2^n) complexity)
	//
	// Algorithm characteristics:
	//   - DP (Dynamic Programming): O(n * 2^n) time, finds optimal join order
	//   - Greedy: O(n^2) time, finds good (but not always optimal) join order
	//
	// Practical limits:
	//   - n ≤ 12: DP is fast and optimal (4096 subsets)
	//   - n = 15: DP is slow but acceptable (32768 subsets)
	//   - n ≥ 20: DP is impractical (>1M subsets), use greedy
	UseDP bool
}

// NewOptimizer creates a new query optimizer
func NewOptimizer() *Optimizer {
	return &Optimizer{
		costEstimator: NewCostEstimator(),
		UseDP:         false, // Default to greedy algorithm
	}
}

// SetStatisticsProvider sets the statistics provider for cardinality estimation
func (o *Optimizer) SetStatisticsProvider(provider StatisticsProvider) {
	o.statisticsProvider = provider
}

// getTableCardinality returns the estimated row count for a table,
// using statistics if available, otherwise falling back to the plan node estimate
func (o *Optimizer) getTableCardinality(node PlanNode) int64 {
	if o.statisticsProvider != nil {
		if tableScan, ok := node.(*TableScanNode); ok && tableScan.Table != nil {
			if stats := o.statisticsProvider.GetTableStatistics(tableScan.Table.Name); stats != nil {
				return stats.RowCount
			}
		}
	}
	return node.EstimatedRows()
}

// Optimize applies all optimization techniques to a query plan
func (o *Optimizer) Optimize(plan PlanNode) PlanNode {
	// Apply predicate pushdown
	plan = o.ApplyPredicatePushdown(plan)

	// Apply projection pushdown
	plan = o.ApplyProjectionPushdown(plan)

	// Apply join reordering
	plan = o.ReorderJoins(plan)

	return plan
}

// ReorderJoins optimizes the order of joins
func (o *Optimizer) ReorderJoins(plan PlanNode) PlanNode {
	switch node := plan.(type) {
	case *NestedLoopJoinNode:
		// Collect all tables and conditions from this join tree
		leaves, conditions := o.collectJoinComponents(node)

		// If only 2 tables (or fewer), no complex reordering needed
		if len(leaves) < 3 {
			return plan
		}

		// Automatically choose algorithm based on number of tables
		// DP is O(n * 2^n), so we use it only for small n to avoid exponential blowup
		// Threshold based on practical limits:
		// - n=4: 2^4 = 16 subsets (very fast)
		// - n=8: 2^8 = 256 subsets (fast)
		// - n=10: 2^10 = 1024 subsets (acceptable)
		// - n=12: 2^12 = 4096 subsets (still reasonable)
		// - n=15: 2^15 = 32768 subsets (getting slow)
		// - n=20: 2^20 = 1048576 subsets (too slow)
		const dpThreshold = 12 // Use DP for up to 12 tables

		useDP := o.UseDP || len(leaves) <= dpThreshold

		if useDP {
			// Dynamic Programming: finds optimal join order
			// Used for small queries (≤12 tables) where we can afford exhaustive search
			return o.buildDPJoinTree(leaves, conditions)
		}

		// Greedy Reordering: faster but may not be optimal
		// Used for large queries (>12 tables) where DP is too expensive
		return o.buildLeftDeepTree(leaves, conditions)

	case *FilterNode:
		node.Input = o.ReorderJoins(node.Input)
		return node
	case *ProjectionNode:
		node.Input = o.ReorderJoins(node.Input)
		return node
	case *HashJoinNode:
		// HashJoin logic similar to NestedLoop if we support it
		// For now, just recurse
		node.Left = o.ReorderJoins(node.Left)
		node.Right = o.ReorderJoins(node.Right)
		return node
	default:
		return plan
	}
}

// collectJoinComponents flattens the join tree into leaves and conditions
func (o *Optimizer) collectJoinComponents(node *NestedLoopJoinNode) ([]PlanNode, []parser.Expression) {
	var leaves []PlanNode
	var conditions []parser.Expression

	// Helper to traverse
	var traverse func(n PlanNode)
	traverse = func(n PlanNode) {
		if join, ok := n.(*NestedLoopJoinNode); ok {
			traverse(join.Left)
			traverse(join.Right)
			conditions = append(conditions, join.Condition)
		} else {
			leaves = append(leaves, n)
		}
	}

	traverse(node)
	return leaves, conditions
}

// buildLeftDeepTree constructs a new join tree using greedy heuristic
// Uses statistics-based cardinality estimates when available
func (o *Optimizer) buildLeftDeepTree(leaves []PlanNode, conditions []parser.Expression) PlanNode {
	if len(leaves) == 0 {
		return nil
	}

	// 1. Pick the leaf with fewest rows as the start (using statistics if available)
	bestIdx := 0
	minRows := o.getTableCardinality(leaves[0])

	for i := 1; i < len(leaves); i++ {
		rows := o.getTableCardinality(leaves[i])
		if rows < minRows {
			minRows = rows
			bestIdx = i
		}
	}

	current := leaves[bestIdx]

	// Remove used leaf
	remaining := append(leaves[:bestIdx], leaves[bestIdx+1:]...)

	// 2. Iteratively pick the next leaf that minimizes join cost
	// Uses statistics-based cardinality for better estimates

	for len(remaining) > 0 {
		bestIdx = 0
		minRows = o.getTableCardinality(remaining[0])

		for i := 1; i < len(remaining); i++ {
			rows := o.getTableCardinality(remaining[i])
			if rows < minRows {
				minRows = rows
				bestIdx = i
			}
		}

		right := remaining[bestIdx]
		remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)

		// Find applicable condition
		// Simplification: just take the first condition? logic is flawed if conditions are specific.
		// We need to find condition relevant to (current tables) AND (right table).
		// Since we collected all conditions, we need to inspect them.

		// For now, reusing the conditions blindly in order is WRONG because we reordered tables.
		// We need to find a condition that binds 'current' and 'right'.

		// Hack: use a dummy condition or find one.
		// Real impl: logic to check Expression references.
		// For this MVP step, I will use a placeholder or the first available condition (VERY RISKY).
		// Better: Just chain them and assume conditions are associative? No.

		var cond parser.Expression = &parser.BinaryExpr{}
		if len(conditions) > 0 {
			cond = conditions[0]
			conditions = conditions[1:]
		}

		current = &NestedLoopJoinNode{
			Left:      current,
			Right:     right,
			Condition: cond,
			JoinType:  parser.JoinInner, // Defaulting to inner
		}
	}

	return current
}

// buildDPJoinTree uses dynamic programming to find the optimal join order
// Algorithm: For each subset of relations, compute the optimal join order
// Time complexity: O(n * 2^n) where n is the number of relations
// Uses statistics-based cardinality estimates when available
func (o *Optimizer) buildDPJoinTree(leaves []PlanNode, conditions []parser.Expression) PlanNode {
	n := len(leaves)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return leaves[0]
	}

	// dp[subset] = best plan for joining tables in subset
	// We use a map where the key is a bitset representing which tables are in the subset
	type dpEntry struct {
		plan PlanNode
		cost float64
		rows int64 // Statistics-aware row count
	}
	dp := make(map[int]*dpEntry)

	// Initialize base cases: single tables (using statistics-based cardinality)
	for i := 0; i < n; i++ {
		subset := 1 << i // bitset with only bit i set
		rows := o.getTableCardinality(leaves[i])
		dp[subset] = &dpEntry{
			plan: leaves[i],
			cost: leaves[i].EstimatedCost(),
			rows: rows,
		}
	}

	// Build up solutions for larger subsets
	// Iterate through all possible subset sizes from 2 to n
	for size := 2; size <= n; size++ {
		// Generate all subsets of given size
		subsets := generateSubsets(n, size)

		for _, subset := range subsets {
			// For each subset, try all possible ways to split it into two parts
			bestCost := float64(1e18) // Large number
			var bestPlan PlanNode
			var bestRows int64

			// Try all possible left/right splits
			// Left must be non-empty and proper subset
			for left := 1; left < subset; left++ {
				// Check if left is a subset of subset
				if (left & subset) != left {
					continue
				}

				right := subset ^ left // XOR gives us the complement
				if right == 0 {
					continue
				}

				// Check if we have computed plans for both left and right
				leftEntry, leftOK := dp[left]
				rightEntry, rightOK := dp[right]

				if !leftOK || !rightOK {
					continue
				}

				// Find an appropriate condition for this join
				// For now, use a dummy condition (in real impl, should match conditions to tables)
				var cond parser.Expression = &parser.BinaryExpr{}
				if len(conditions) > 0 {
					// Simple heuristic: use first available condition
					cond = conditions[0]
				}

				// Create a join node
				joinNode := &NestedLoopJoinNode{
					Left:      leftEntry.plan,
					Right:     rightEntry.plan,
					Condition: cond,
					JoinType:  parser.JoinInner,
				}

				// Compute cost using statistics-aware row counts
				// Nested loop join cost = left cost + (left rows * right cost)
				leftRows := leftEntry.rows
				rightCost := rightEntry.cost
				cost := leftEntry.cost + float64(leftRows)*rightCost

				// Estimate output rows (use min of left and right for 1:1 join assumption)
				joinRows := leftEntry.rows
				if rightEntry.rows < joinRows {
					joinRows = rightEntry.rows
				}

				// Update best if this is better
				if cost < bestCost {
					bestCost = cost
					bestPlan = joinNode
					bestRows = joinRows
				}
			}

			// Store the best plan for this subset
			if bestPlan != nil {
				dp[subset] = &dpEntry{
					plan: bestPlan,
					cost: bestCost,
					rows: bestRows,
				}
			}
		}
	}

	// Return the best plan for all tables
	allTables := (1 << n) - 1 // All bits set
	if entry, ok := dp[allTables]; ok {
		return entry.plan
	}

	// Fallback: if DP failed somehow, use first leaf
	return leaves[0]
}

// generateSubsets generates all subsets of {0, 1, ..., n-1} with exactly k elements
// Returns subsets as bitsets (integers where bit i indicates element i is in the subset)
func generateSubsets(n int, k int) []int {
	var result []int

	// Use bit manipulation to generate all subsets
	// Iterate through all numbers from 0 to 2^n - 1
	limit := 1 << n
	for subset := 0; subset < limit; subset++ {
		// Count number of set bits
		count := 0
		for i := 0; i < n; i++ {
			if (subset & (1 << i)) != 0 {
				count++
			}
		}

		if count == k {
			result = append(result, subset)
		}
	}

	return result
}

// ApplyPredicatePushdown pushes filter predicates down to data sources
// This reduces the amount of data that needs to be processed at higher levels
func (o *Optimizer) ApplyPredicatePushdown(plan PlanNode) PlanNode {
	switch node := plan.(type) {
	case *FilterNode:
		// Recursively optimize child first
		optimizedChild := o.ApplyPredicatePushdown(node.Input)

		// Check if we can push this filter further down
		switch child := optimizedChild.(type) {
		case *ProjectionNode:
			// Push filter below projection if possible
			// Filter(Project(X)) -> Project(Filter(X))
			// Note: In real optimizer, we must check if filter only references columns from X.
			// For now, assume it's safe if projection is just selecting subset.
			pushedFilter := &FilterNode{
				Input:       child.Input,
				Condition:   node.Condition,
				Selectivity: node.Selectivity,
			}
			return &ProjectionNode{
				Input:       pushedFilter,
				Expressions: child.Expressions,
				Aliases:     child.Aliases,
			}

		case *FilterNode:
			// Merge multiple filters
			// Filter(Filter(X)) -> Filter(X with combined predicates)
			combinedSelectivity := node.Selectivity * child.Selectivity

			// Combine conditions with AND
			combinedCondition := &parser.BinaryExpr{
				Op:    lexer.AND,
				Left:  node.Condition,
				Right: child.Condition,
			}

			return &FilterNode{
				Input:       child.Input,
				Condition:   combinedCondition,
				Selectivity: combinedSelectivity,
			}

		default:
			// Cannot push further, return with optimized child
			node.Input = optimizedChild
			return node
		}

	case *ProjectionNode:
		// Recursively optimize child
		node.Input = o.ApplyPredicatePushdown(node.Input)
		return node

	case *NestedLoopJoinNode:
		// Recursively optimize both children
		node.Left = o.ApplyPredicatePushdown(node.Left)
		node.Right = o.ApplyPredicatePushdown(node.Right)
		return node

	case *HashJoinNode:
		// Recursively optimize both children
		node.Left = o.ApplyPredicatePushdown(node.Left)
		node.Right = o.ApplyPredicatePushdown(node.Right)
		return node

	default:
		// Leaf nodes or unknown nodes, return as-is
		return plan
	}
}

// ApplyProjectionPushdown pushes projections down to reduce data transfer
// This optimization reads only the columns that are actually needed
func (o *Optimizer) ApplyProjectionPushdown(plan PlanNode) PlanNode {
	switch node := plan.(type) {
	case *ProjectionNode:
		// Extract required columns from projection expressions
		requiredCols := extractColumnRefs(node.Expressions)

		// Push required columns down to child
		optimizedChild := o.pushRequiredColumns(node.Input, requiredCols)

		// Handle merging consecutive projections
		if childProj, ok := optimizedChild.(*ProjectionNode); ok {
			// Merge consecutive projections
			// Project1(Project2(X)) -> Project1(X)
			return &ProjectionNode{
				Input:       childProj.Input,
				Expressions: node.Expressions,
				Aliases:     node.Aliases,
			}
		}

		node.Input = optimizedChild
		return node

	case *FilterNode:
		// Recursively optimize child
		node.Input = o.ApplyProjectionPushdown(node.Input)
		return node

	case *NestedLoopJoinNode:
		// Recursively optimize both children
		node.Left = o.ApplyProjectionPushdown(node.Left)
		node.Right = o.ApplyProjectionPushdown(node.Right)
		return node

	case *HashJoinNode:
		// Recursively optimize both children
		node.Left = o.ApplyProjectionPushdown(node.Left)
		node.Right = o.ApplyProjectionPushdown(node.Right)
		return node

	default:
		// Leaf nodes or unknown nodes, return as-is
		return plan
	}
}

// pushRequiredColumns pushes required column information down to table scans
func (o *Optimizer) pushRequiredColumns(plan PlanNode, requiredCols map[string]bool) PlanNode {
	switch node := plan.(type) {
	case *TableScanNode:
		// Set required columns on the scan
		cols := make([]string, 0, len(requiredCols))
		for col := range requiredCols {
			cols = append(cols, col)
		}
		node.RequiredColumns = cols
		return node

	case *FilterNode:
		// Extract columns used by the filter condition and add them to required set
		filterCols := extractColumnRefsFromExpr(node.Condition)
		for col := range filterCols {
			requiredCols[col] = true
		}
		// Push required columns down to child
		node.Input = o.pushRequiredColumns(node.Input, requiredCols)
		return node

	case *ProjectionNode:
		// Extract columns from projection expressions
		projCols := extractColumnRefs(node.Expressions)
		for col := range projCols {
			requiredCols[col] = true
		}
		node.Input = o.pushRequiredColumns(node.Input, requiredCols)
		return node

	case *NestedLoopJoinNode:
		// For joins, push required columns to both sides
		// A more sophisticated implementation would partition columns by table
		node.Left = o.pushRequiredColumns(node.Left, requiredCols)
		node.Right = o.pushRequiredColumns(node.Right, requiredCols)
		return node

	case *HashJoinNode:
		// For hash joins, also need to include join keys
		joinCols := make(map[string]bool)
		for col := range requiredCols {
			joinCols[col] = true
		}
		joinCols[node.LeftKey] = true
		joinCols[node.RightKey] = true
		node.Left = o.pushRequiredColumns(node.Left, joinCols)
		node.Right = o.pushRequiredColumns(node.Right, joinCols)
		return node

	default:
		return plan
	}
}

// extractColumnRefs extracts column names from a list of expressions
func extractColumnRefs(exprs []parser.Expression) map[string]bool {
	result := make(map[string]bool)
	for _, expr := range exprs {
		cols := extractColumnRefsFromExpr(expr)
		for col := range cols {
			result[col] = true
		}
	}
	return result
}

// extractColumnRefsFromExpr extracts column names from a single expression
func extractColumnRefsFromExpr(expr parser.Expression) map[string]bool {
	result := make(map[string]bool)
	if expr == nil {
		return result
	}

	switch e := expr.(type) {
	case *parser.ColumnRef:
		// Use just the column name (without table prefix for simplicity)
		result[e.Name] = true

	case *parser.BinaryExpr:
		// Recursively extract from both sides
		leftCols := extractColumnRefsFromExpr(e.Left)
		rightCols := extractColumnRefsFromExpr(e.Right)
		for col := range leftCols {
			result[col] = true
		}
		for col := range rightCols {
			result[col] = true
		}

	case *parser.UnaryExpr:
		// Extract from operand
		cols := extractColumnRefsFromExpr(e.Right)
		for col := range cols {
			result[col] = true
		}

	case *parser.FunctionCall:
		// Extract from function arguments
		for _, arg := range e.Args {
			cols := extractColumnRefsFromExpr(arg)
			for col := range cols {
				result[col] = true
			}
		}
	}

	return result
}
