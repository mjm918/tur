// pkg/sql/optimizer/plan.go
package optimizer

import (
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
)

// PlanNode is the interface for all query plan nodes
type PlanNode interface {
	EstimatedCost() float64 // Estimated cost to execute this node
	EstimatedRows() int64   // Estimated number of rows produced
}

// TableScanNode represents a full table scan
type TableScanNode struct {
	Table *schema.TableDef
	Alias string
	Cost  float64
	Rows  int64
	// RequiredColumns specifies which columns need to be read.
	// If nil or empty, all columns are read.
	// This is set by projection pushdown optimization.
	RequiredColumns []string
}

func (n *TableScanNode) EstimatedCost() float64 {
	// If RequiredColumns is set, cost is proportional to columns read
	if len(n.RequiredColumns) > 0 && n.Table != nil && len(n.Table.Columns) > 0 {
		totalColumns := len(n.Table.Columns)
		requiredColumns := len(n.RequiredColumns)
		// Cost scales with the fraction of columns being read
		return n.Cost * float64(requiredColumns) / float64(totalColumns)
	}
	return n.Cost
}

func (n *TableScanNode) EstimatedRows() int64 {
	return n.Rows
}

// SubqueryScanNode represents a scan over a subquery (derived table)
type SubqueryScanNode struct {
	SubqueryPlan PlanNode
	Alias        string
}

func (n *SubqueryScanNode) EstimatedCost() float64 {
	return n.SubqueryPlan.EstimatedCost()
}

func (n *SubqueryScanNode) EstimatedRows() int64 {
	return n.SubqueryPlan.EstimatedRows()
}

// IndexScanNode represents an index scan
type IndexScanNode struct {
	Table     *schema.TableDef
	Alias     string
	IndexName string
	Cost      float64
	Rows      int64
}

func (n *IndexScanNode) EstimatedCost() float64 {
	return n.Cost
}

func (n *IndexScanNode) EstimatedRows() int64 {
	return n.Rows
}

// FilterNode represents a filter operation (WHERE clause)
type FilterNode struct {
	Input       PlanNode
	Condition   parser.Expression
	Selectivity float64 // Fraction of rows that pass the filter (0.0 to 1.0)
}

func (n *FilterNode) EstimatedCost() float64 {
	// Filter cost = child cost + (input rows * cost per row check)
	const costPerRowCheck = 0.01
	inputRows := float64(n.Input.EstimatedRows())
	return n.Input.EstimatedCost() + (inputRows * costPerRowCheck)
}

func (n *FilterNode) EstimatedRows() int64 {
	// Output rows = input rows * selectivity
	inputRows := float64(n.Input.EstimatedRows())
	return int64(inputRows * n.Selectivity)
}

// ProjectionNode represents a projection operation (SELECT columns)
type ProjectionNode struct {
	Input       PlanNode
	Expressions []parser.Expression
}

func (n *ProjectionNode) EstimatedCost() float64 {
	// Projection cost = child cost + (rows * cost per projection)
	const costPerProjection = 0.001
	rows := float64(n.Input.EstimatedRows())
	return n.Input.EstimatedCost() + (rows * costPerProjection)
}

func (n *ProjectionNode) EstimatedRows() int64 {
	// Projection doesn't change row count
	return n.Input.EstimatedRows()
}

// NestedLoopJoinNode represents a nested loop join
type NestedLoopJoinNode struct {
	Left      PlanNode
	Right     PlanNode
	Condition parser.Expression
	JoinType  parser.JoinType
}

func (n *NestedLoopJoinNode) EstimatedCost() float64 {
	// Nested loop join cost = left cost + (left rows * right cost)
	leftCost := n.Left.EstimatedCost()
	rightCost := n.Right.EstimatedCost()
	leftRows := float64(n.Left.EstimatedRows())
	return leftCost + (leftRows * rightCost)
}

func (n *NestedLoopJoinNode) EstimatedRows() int64 {
	// For now, assume 1:1 join (will be refined with statistics later)
	// Output rows = min(left rows, right rows)
	leftRows := n.Left.EstimatedRows()
	rightRows := n.Right.EstimatedRows()
	if leftRows < rightRows {
		return leftRows
	}
	return rightRows
}

// HashJoinNode represents a hash join
type HashJoinNode struct {
	Left     PlanNode
	Right    PlanNode
	LeftKey  string
	RightKey string
}

func (n *HashJoinNode) EstimatedCost() float64 {
	// Hash join cost = left cost + right cost + hash build cost + hash probe cost
	const costPerHashBuild = 0.01
	const costPerHashProbe = 0.001

	leftCost := n.Left.EstimatedCost()
	rightCost := n.Right.EstimatedCost()
	leftRows := float64(n.Left.EstimatedRows())
	rightRows := float64(n.Right.EstimatedRows())

	hashBuildCost := leftRows * costPerHashBuild
	hashProbeCost := rightRows * costPerHashProbe

	return leftCost + rightCost + hashBuildCost + hashProbeCost
}

func (n *HashJoinNode) EstimatedRows() int64 {
	// For now, assume 1:1 join (will be refined with statistics later)
	// Output rows = min(left rows, right rows)
	leftRows := n.Left.EstimatedRows()
	rightRows := n.Right.EstimatedRows()
	if leftRows < rightRows {
		return leftRows
	}
	return rightRows
}

// SortNode represents an ORDER BY operation
type SortNode struct {
	Input   PlanNode
	OrderBy []parser.OrderByExpr
}

func (n *SortNode) EstimatedCost() float64 {
	// Sort cost = child cost + (rows * log(rows) * cost per comparison)
	const costPerComparison = 0.01
	rows := float64(n.Input.EstimatedRows())
	if rows < 1 {
		rows = 1
	}
	// O(n log n) sort cost estimate
	sortCost := rows * costPerComparison
	if rows > 1 {
		sortCost = rows * log2(rows) * costPerComparison
	}
	return n.Input.EstimatedCost() + sortCost
}

func (n *SortNode) EstimatedRows() int64 {
	// Sort doesn't change row count
	return n.Input.EstimatedRows()
}

// LimitNode represents a LIMIT/OFFSET operation
type LimitNode struct {
	Input  PlanNode
	Limit  parser.Expression // May be nil if no LIMIT
	Offset parser.Expression // May be nil if no OFFSET
}

func (n *LimitNode) EstimatedCost() float64 {
	// Limit is cheap, just count rows
	return n.Input.EstimatedCost() + 0.001
}

func (n *LimitNode) EstimatedRows() int64 {
	// Returns at most Limit rows
	// For cost estimation, we need to evaluate the limit expression
	// For now, use a conservative estimate
	inputRows := n.Input.EstimatedRows()
	// We can't evaluate expression at planning time easily
	// Return a conservative estimate assuming 50% of input rows
	return inputRows / 2
}

// AggregateExpr represents an aggregate function in a query
type AggregateExpr struct {
	FuncName string            // e.g., "COUNT", "SUM", "AVG"
	Arg      parser.Expression // The argument to the aggregate (e.g., column ref)
}

// AggregateNode represents a GROUP BY operation with aggregations
type AggregateNode struct {
	Input      PlanNode            // Input plan (filtered data)
	GroupBy    []parser.Expression // GROUP BY expressions (e.g., column refs)
	Aggregates []AggregateExpr     // Aggregate functions to compute
	Having     parser.Expression   // Optional HAVING filter (nil if none)
}

func (n *AggregateNode) EstimatedCost() float64 {
	// Aggregate cost = child cost + (rows * cost per group operation)
	const costPerGroupOp = 0.02
	inputRows := float64(n.Input.EstimatedRows())
	return n.Input.EstimatedCost() + (inputRows * costPerGroupOp)
}

func (n *AggregateNode) EstimatedRows() int64 {
	// Estimate: number of groups = sqrt(input rows) as a heuristic
	// This is a rough estimate since we don't know actual group cardinality
	inputRows := n.Input.EstimatedRows()
	if inputRows <= 1 {
		return 1
	}
	// Use sqrt as rough group count estimate
	groupCount := int64(1)
	for groupCount*groupCount < inputRows {
		groupCount++
	}
	if groupCount > inputRows {
		groupCount = inputRows
	}
	return groupCount
}

// Helper for log base 2
func log2(x float64) float64 {
	if x <= 1 {
		return 0
	}
	// log2(x) = ln(x) / ln(2)
	return 3.321928 * (x - 1) / x // Approximation for small values
}

// CTEScanNode represents a scan over materialized CTE results
type CTEScanNode struct {
	CTEName string   // Name of the CTE
	Alias   string   // Optional alias
	Columns []string // Column names from CTE
	Rows    int64    // Estimated rows (set after materialization)
}

func (n *CTEScanNode) EstimatedCost() float64 {
	// CTE scan is cheap since data is already materialized in memory
	return float64(n.Rows) * 0.001
}

func (n *CTEScanNode) EstimatedRows() int64 {
	return n.Rows
}

// DualNode represents a dummy single-row source for queries without FROM clause
// (e.g., SELECT 1+1, SELECT function())
type DualNode struct{}

func (n *DualNode) EstimatedCost() float64 {
	return 0.001 // Negligible cost
}

func (n *DualNode) EstimatedRows() int64 {
	return 1 // Always produces exactly one row
}

// WindowNode represents a window function computation
// It wraps the input plan, sorts/partitions the data, and computes window functions
type WindowNode struct {
	Input           PlanNode                 // Input plan
	WindowFunctions []*parser.WindowFunction // Window function expressions
	AllExpressions  []parser.Expression      // All SELECT expressions (including window functions)
}

func (n *WindowNode) EstimatedCost() float64 {
	// Window function cost = child cost + sort/partition cost + computation cost
	const costPerRow = 0.03
	rows := float64(n.Input.EstimatedRows())
	return n.Input.EstimatedCost() + (rows * costPerRow)
}

func (n *WindowNode) EstimatedRows() int64 {
	// Window functions don't change row count
	return n.Input.EstimatedRows()
}

// TableFunctionNode represents a table-valued function call
// e.g., vector_quantize_scan('table', 'column', query_vec, k)
type TableFunctionNode struct {
	Name  string              // Function name (e.g., "vector_quantize_scan")
	Args  []parser.Expression // Function arguments
	Alias string              // Optional alias
}

func (n *TableFunctionNode) EstimatedCost() float64 {
	// Table function cost is roughly linear in expected output rows
	// For vector search, this is typically the k parameter
	return 1.0 // Default cost
}

func (n *TableFunctionNode) EstimatedRows() int64 {
	// For vector_quantize_scan, rows = k parameter (4th argument)
	// Default to 10 if we can't determine
	return 10
}
