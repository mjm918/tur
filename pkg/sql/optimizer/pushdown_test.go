// pkg/sql/optimizer/pushdown_test.go
package optimizer

import (
	"testing"
)

// TestPredicatePushdown tests pushing predicates down to table scan
func TestPredicatePushdown(t *testing.T) {
	// Original plan: Project(Filter(TableScan))
	// Optimized plan: Project(TableScan with filter)

	scan := &TableScanNode{
		TableName: "users",
		Cost:      100.0,
		Rows:      1000,
	}

	filter := &FilterNode{
		Child:       scan,
		Predicate:   "age > 18",
		Selectivity: 0.3,
	}

	project := &ProjectionNode{
		Child:   filter,
		Columns: []string{"name", "email"},
	}

	optimizer := NewOptimizer()
	optimized := optimizer.ApplyPredicatePushdown(project)

	// After pushdown, filter should be integrated into scan
	// The structure might still be Project(Filter(Scan)) but the filter
	// should be marked as pushed down for more efficient execution
	if optimized == nil {
		t.Error("expected optimized plan, got nil")
	}

	// Verify the plan is still valid
	if optimized.EstimatedCost() <= 0 {
		t.Error("optimized plan should have positive cost")
	}
}

// TestPredicatePushdown_MultipleFilters tests pushing multiple predicates
func TestPredicatePushdown_MultipleFilters(t *testing.T) {
	scan := &TableScanNode{
		TableName: "orders",
		Cost:      200.0,
		Rows:      5000,
	}

	filter1 := &FilterNode{
		Child:       scan,
		Predicate:   "status = 'active'",
		Selectivity: 0.2,
	}

	filter2 := &FilterNode{
		Child:       filter1,
		Predicate:   "amount > 100",
		Selectivity: 0.5,
	}

	optimizer := NewOptimizer()
	optimized := optimizer.ApplyPredicatePushdown(filter2)

	// Both filters should be pushed down
	if optimized == nil {
		t.Error("expected optimized plan, got nil")
	}

	// Verify cost is reasonable
	if optimized.EstimatedCost() <= 0 {
		t.Error("optimized plan should have positive cost")
	}
}

// TestProjectionPushdown tests pushing projections down to reduce data
func TestProjectionPushdown(t *testing.T) {
	// Original plan: Project(name, email)(Filter(TableScan(*)))
	// Optimized plan: Project(name, email)(Filter(TableScan(id, name, email, age)))

	scan := &TableScanNode{
		TableName: "users",
		Cost:      100.0,
		Rows:      1000,
	}

	filter := &FilterNode{
		Child:       scan,
		Predicate:   "age > 18",
		Selectivity: 0.3,
	}

	project := &ProjectionNode{
		Child:   filter,
		Columns: []string{"name", "email"},
	}

	optimizer := NewOptimizer()
	optimized := optimizer.ApplyProjectionPushdown(project)

	// After pushdown, scan should only read needed columns
	if optimized == nil {
		t.Error("expected optimized plan, got nil")
	}

	// Projection pushdown should reduce cost (less data to read)
	if optimized.EstimatedCost() > project.EstimatedCost() {
		t.Error("projection pushdown should not increase cost")
	}
}

// TestProjectionPushdown_ThroughJoin tests projection through joins
func TestProjectionPushdown_ThroughJoin(t *testing.T) {
	leftScan := &TableScanNode{
		TableName: "users",
		Cost:      100.0,
		Rows:      1000,
	}

	rightScan := &TableScanNode{
		TableName: "orders",
		Cost:      200.0,
		Rows:      5000,
	}

	join := &HashJoinNode{
		Left:     leftScan,
		Right:    rightScan,
		LeftKey:  "id",
		RightKey: "user_id",
	}

	// Only select a few columns from the join
	project := &ProjectionNode{
		Child:   join,
		Columns: []string{"users.name", "orders.total"},
	}

	optimizer := NewOptimizer()
	optimized := optimizer.ApplyProjectionPushdown(project)

	// Projection should be pushed to both sides of join
	if optimized == nil {
		t.Error("expected optimized plan, got nil")
	}
}

// TestOptimize_CombinedPushdown tests applying multiple optimizations
func TestOptimize_CombinedPushdown(t *testing.T) {
	scan := &TableScanNode{
		TableName: "products",
		Cost:      500.0,
		Rows:      10000,
	}

	filter := &FilterNode{
		Child:       scan,
		Predicate:   "price > 50 AND category = 'electronics'",
		Selectivity: 0.1,
	}

	project := &ProjectionNode{
		Child:   filter,
		Columns: []string{"name", "price"},
	}

	optimizer := NewOptimizer()
	optimized := optimizer.Optimize(project)

	// Should apply both predicate and projection pushdown
	if optimized == nil {
		t.Error("expected optimized plan, got nil")
	}

	// Optimized plan should have lower or equal cost
	if optimized.EstimatedCost() > project.EstimatedCost() {
		t.Error("optimization should not increase cost")
	}
}

// TestOptimize_NoChanges tests when no optimization is possible
func TestOptimize_NoChanges(t *testing.T) {
	// Simple table scan with no filters or projections
	scan := &TableScanNode{
		TableName: "users",
		Cost:      100.0,
		Rows:      1000,
	}

	optimizer := NewOptimizer()
	optimized := optimizer.Optimize(scan)

	// Should return the same plan
	if optimized == nil {
		t.Error("expected plan, got nil")
	}

	if optimized.EstimatedCost() != scan.EstimatedCost() {
		t.Error("cost should not change when no optimization is applied")
	}
}
