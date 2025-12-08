// pkg/sql/optimizer/plan.go
package optimizer

// PlanNode is the interface for all query plan nodes
type PlanNode interface {
	EstimatedCost() float64 // Estimated cost to execute this node
	EstimatedRows() int64   // Estimated number of rows produced
}

// TableScanNode represents a full table scan
type TableScanNode struct {
	TableName string
	Cost      float64
	Rows      int64
}

func (n *TableScanNode) EstimatedCost() float64 {
	return n.Cost
}

func (n *TableScanNode) EstimatedRows() int64 {
	return n.Rows
}

// IndexScanNode represents an index scan
type IndexScanNode struct {
	TableName string
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
	Child       PlanNode
	Predicate   string
	Selectivity float64 // Fraction of rows that pass the filter (0.0 to 1.0)
}

func (n *FilterNode) EstimatedCost() float64 {
	// Filter cost = child cost + (input rows * cost per row check)
	const costPerRowCheck = 0.01
	inputRows := float64(n.Child.EstimatedRows())
	return n.Child.EstimatedCost() + (inputRows * costPerRowCheck)
}

func (n *FilterNode) EstimatedRows() int64 {
	// Output rows = input rows * selectivity
	inputRows := float64(n.Child.EstimatedRows())
	return int64(inputRows * n.Selectivity)
}

// ProjectionNode represents a projection operation (SELECT columns)
type ProjectionNode struct {
	Child   PlanNode
	Columns []string
}

func (n *ProjectionNode) EstimatedCost() float64 {
	// Projection cost = child cost + (rows * cost per projection)
	const costPerProjection = 0.001
	rows := float64(n.Child.EstimatedRows())
	return n.Child.EstimatedCost() + (rows * costPerProjection)
}

func (n *ProjectionNode) EstimatedRows() int64 {
	// Projection doesn't change row count
	return n.Child.EstimatedRows()
}

// NestedLoopJoinNode represents a nested loop join
type NestedLoopJoinNode struct {
	Left      PlanNode
	Right     PlanNode
	Condition string
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
