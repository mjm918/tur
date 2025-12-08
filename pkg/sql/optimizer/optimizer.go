// pkg/sql/optimizer/optimizer.go
package optimizer

// Optimizer applies query optimization techniques
type Optimizer struct {
	costEstimator *CostEstimator
}

// NewOptimizer creates a new query optimizer
func NewOptimizer() *Optimizer {
	return &Optimizer{
		costEstimator: NewCostEstimator(),
	}
}

// Optimize applies all optimization techniques to a query plan
func (o *Optimizer) Optimize(plan PlanNode) PlanNode {
	// Apply predicate pushdown
	plan = o.ApplyPredicatePushdown(plan)

	// Apply projection pushdown
	plan = o.ApplyProjectionPushdown(plan)

	// Future: Add join reordering, index selection, etc.

	return plan
}

// ApplyPredicatePushdown pushes filter predicates down to data sources
// This reduces the amount of data that needs to be processed at higher levels
func (o *Optimizer) ApplyPredicatePushdown(plan PlanNode) PlanNode {
	switch node := plan.(type) {
	case *FilterNode:
		// Recursively optimize child first
		optimizedChild := o.ApplyPredicatePushdown(node.Child)

		// Check if we can push this filter further down
		switch child := optimizedChild.(type) {
		case *ProjectionNode:
			// Push filter below projection if possible
			// Filter(Project(X)) -> Project(Filter(X))
			pushedFilter := &FilterNode{
				Child:       child.Child,
				Predicate:   node.Predicate,
				Selectivity: node.Selectivity,
			}
			return &ProjectionNode{
				Child:   pushedFilter,
				Columns: child.Columns,
			}

		case *FilterNode:
			// Merge multiple filters
			// Filter(Filter(X)) -> Filter(X with combined predicates)
			combinedSelectivity := node.Selectivity * child.Selectivity
			return &FilterNode{
				Child:       child.Child,
				Predicate:   node.Predicate + " AND " + child.Predicate,
				Selectivity: combinedSelectivity,
			}

		default:
			// Cannot push further, return with optimized child
			node.Child = optimizedChild
			return node
		}

	case *ProjectionNode:
		// Recursively optimize child
		node.Child = o.ApplyPredicatePushdown(node.Child)
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
		// Recursively optimize child first
		optimizedChild := o.ApplyProjectionPushdown(node.Child)

		switch child := optimizedChild.(type) {
		case *ProjectionNode:
			// Merge consecutive projections
			// Project1(Project2(X)) -> Project1(X)
			// Keep only the outermost projection
			return &ProjectionNode{
				Child:   child.Child,
				Columns: node.Columns,
			}

		case *FilterNode:
			// Cannot push projection through filter easily
			// (would need to analyze which columns the filter needs)
			// For now, keep the structure
			node.Child = optimizedChild
			return node

		case *NestedLoopJoinNode, *HashJoinNode:
			// For joins, we could push projections to each side
			// This is more complex as we need to analyze which columns come from which side
			// For now, keep the structure
			node.Child = optimizedChild
			return node

		default:
			node.Child = optimizedChild
			return node
		}

	case *FilterNode:
		// Recursively optimize child
		node.Child = o.ApplyProjectionPushdown(node.Child)
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
