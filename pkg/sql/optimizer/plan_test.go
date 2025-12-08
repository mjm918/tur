// pkg/sql/optimizer/plan_test.go
package optimizer

import (
	"testing"
)

// TestPlanNode_TableScan tests creating a table scan plan node
func TestPlanNode_TableScan(t *testing.T) {
	plan := &TableScanNode{
		TableName: "users",
		Cost:      100.0,
		Rows:      1000,
	}

	if plan.TableName != "users" {
		t.Errorf("expected table name 'users', got '%s'", plan.TableName)
	}
	if plan.EstimatedCost() != 100.0 {
		t.Errorf("expected cost 100.0, got %f", plan.EstimatedCost())
	}
	if plan.EstimatedRows() != 1000 {
		t.Errorf("expected rows 1000, got %d", plan.EstimatedRows())
	}
}

// TestPlanNode_IndexScan tests creating an index scan plan node
func TestPlanNode_IndexScan(t *testing.T) {
	plan := &IndexScanNode{
		TableName: "users",
		IndexName: "idx_email",
		Cost:      10.0,
		Rows:      50,
	}

	if plan.TableName != "users" {
		t.Errorf("expected table name 'users', got '%s'", plan.TableName)
	}
	if plan.IndexName != "idx_email" {
		t.Errorf("expected index name 'idx_email', got '%s'", plan.IndexName)
	}
	if plan.EstimatedCost() != 10.0 {
		t.Errorf("expected cost 10.0, got %f", plan.EstimatedCost())
	}
	if plan.EstimatedRows() != 50 {
		t.Errorf("expected rows 50, got %d", plan.EstimatedRows())
	}
}

// TestPlanNode_Filter tests creating a filter (WHERE) plan node
func TestPlanNode_Filter(t *testing.T) {
	child := &TableScanNode{
		TableName: "users",
		Cost:      100.0,
		Rows:      1000,
	}

	plan := &FilterNode{
		Child:      child,
		Predicate:  "age > 18",
		Selectivity: 0.3, // 30% of rows pass the filter
	}

	// Filter cost = child cost + (rows * cost_per_row_check)
	expectedCost := 100.0 + (1000 * 0.01) // assuming 0.01 cost per row check
	if plan.EstimatedCost() != expectedCost {
		t.Errorf("expected cost %f, got %f", expectedCost, plan.EstimatedCost())
	}

	expectedRows := int64(float64(1000) * 0.3) // 300 rows
	if plan.EstimatedRows() != expectedRows {
		t.Errorf("expected rows %d, got %d", expectedRows, plan.EstimatedRows())
	}
}

// TestPlanNode_Projection tests creating a projection (SELECT columns) plan node
func TestPlanNode_Projection(t *testing.T) {
	child := &TableScanNode{
		TableName: "users",
		Cost:      100.0,
		Rows:      1000,
	}

	plan := &ProjectionNode{
		Child:   child,
		Columns: []string{"id", "name", "email"},
	}

	// Projection cost = child cost + (rows * cost_per_projection)
	expectedCost := 100.0 + (1000 * 0.001) // assuming 0.001 cost per projection
	if plan.EstimatedCost() != expectedCost {
		t.Errorf("expected cost %f, got %f", expectedCost, plan.EstimatedCost())
	}

	if plan.EstimatedRows() != 1000 {
		t.Errorf("expected rows 1000, got %d", plan.EstimatedRows())
	}
}

// TestPlanNode_NestedLoopJoin tests creating a nested loop join plan node
func TestPlanNode_NestedLoopJoin(t *testing.T) {
	left := &TableScanNode{
		TableName: "users",
		Cost:      100.0,
		Rows:      1000,
	}

	right := &TableScanNode{
		TableName: "orders",
		Cost:      200.0,
		Rows:      5000,
	}

	plan := &NestedLoopJoinNode{
		Left:      left,
		Right:     right,
		Condition: "users.id = orders.user_id",
	}

	// Nested loop join cost = left cost + (left rows * right cost)
	expectedCost := 100.0 + (1000 * 200.0)
	if plan.EstimatedCost() != expectedCost {
		t.Errorf("expected cost %f, got %f", expectedCost, plan.EstimatedCost())
	}

	// Output rows depend on join selectivity (assume 1:1 for now)
	expectedRows := int64(1000) // assuming each user has 1 order
	if plan.EstimatedRows() != expectedRows {
		t.Errorf("expected rows %d, got %d", expectedRows, plan.EstimatedRows())
	}
}

// TestPlanNode_HashJoin tests creating a hash join plan node
func TestPlanNode_HashJoin(t *testing.T) {
	left := &TableScanNode{
		TableName: "users",
		Cost:      100.0,
		Rows:      1000,
	}

	right := &TableScanNode{
		TableName: "orders",
		Cost:      200.0,
		Rows:      5000,
	}

	plan := &HashJoinNode{
		Left:      left,
		Right:     right,
		LeftKey:   "id",
		RightKey:  "user_id",
	}

	// Hash join cost = left cost + right cost + (left rows * hash_build_cost) + (right rows * hash_probe_cost)
	hashBuildCost := float64(1000) * 0.01
	hashProbeCost := float64(5000) * 0.001
	expectedCost := 100.0 + 200.0 + hashBuildCost + hashProbeCost
	if plan.EstimatedCost() != expectedCost {
		t.Errorf("expected cost %f, got %f", expectedCost, plan.EstimatedCost())
	}

	// Output rows depend on join selectivity (assume 1:1 for now)
	expectedRows := int64(1000) // assuming each user has 1 order
	if plan.EstimatedRows() != expectedRows {
		t.Errorf("expected rows %d, got %d", expectedRows, plan.EstimatedRows())
	}
}
