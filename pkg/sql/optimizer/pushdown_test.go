// pkg/sql/optimizer/pushdown_test.go
package optimizer

import (
	"testing"
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
)

// TestPredicatePushdown tests pushing predicates down to table scan
func TestPredicatePushdown(t *testing.T) {
	// Original plan: Project(Filter(TableScan))
	// Optimized plan: Project(TableScan with filter)

	scan := &TableScanNode{
		Table: &schema.TableDef{Name: "users"},
		Cost:  100.0,
		Rows:  1000,
	}

	filter := &FilterNode{
		Input:       scan,
		Condition:   &parser.BinaryExpr{}, // Mock
		Selectivity: 0.3,
	}

	project := &ProjectionNode{
		Input:       filter,
		Expressions: []parser.Expression{&parser.ColumnRef{Name: "name"}, &parser.ColumnRef{Name: "email"}},
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
		Table: &schema.TableDef{Name: "orders"},
		Cost:  200.0,
		Rows:  5000,
	}

	filter1 := &FilterNode{
		Input:       scan,
		Condition:   &parser.BinaryExpr{},
		Selectivity: 0.2,
	}

	filter2 := &FilterNode{
		Input:       filter1,
		Condition:   &parser.BinaryExpr{},
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
		Table: &schema.TableDef{Name: "users"},
		Cost:  100.0,
		Rows:  1000,
	}

	filter := &FilterNode{
		Input:       scan,
		Condition:   &parser.BinaryExpr{},
		Selectivity: 0.3,
	}

	project := &ProjectionNode{
		Input:       filter,
		Expressions: []parser.Expression{&parser.ColumnRef{Name: "name"}, &parser.ColumnRef{Name: "email"}},
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
		Table: &schema.TableDef{Name: "users"},
		Cost:  100.0,
		Rows:  1000,
	}

	rightScan := &TableScanNode{
		Table: &schema.TableDef{Name: "orders"},
		Cost:  200.0,
		Rows:  5000,
	}

	join := &HashJoinNode{
		Left:     leftScan,
		Right:    rightScan,
		LeftKey:  "id",
		RightKey: "user_id",
	}

	// Only select a few columns from the join
	project := &ProjectionNode{
		Input:       join,
		Expressions: []parser.Expression{&parser.ColumnRef{Name: "users.name"}, &parser.ColumnRef{Name: "orders.total"}},
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
		Table: &schema.TableDef{Name: "products"},
		Cost:  500.0,
		Rows:  10000,
	}

	filter := &FilterNode{
		Input:       scan,
		Condition:   &parser.BinaryExpr{},
		Selectivity: 0.1,
	}

	project := &ProjectionNode{
		Input:       filter,
		Expressions: []parser.Expression{&parser.ColumnRef{Name: "name"}, &parser.ColumnRef{Name: "price"}},
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
		Table: &schema.TableDef{Name: "users"},
		Cost:  100.0,
		Rows:  1000,
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

// TestProjectionPushdown_RequiredColumns tests that projection pushdown
// correctly identifies and propagates required columns to the table scan
func TestProjectionPushdown_RequiredColumns(t *testing.T) {
	// Create a table with 5 columns
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id"},
			{Name: "name"},
			{Name: "email"},
			{Name: "age"},
			{Name: "created_at"},
		},
	}

	scan := &TableScanNode{
		Table: tableDef,
		Cost:  100.0,
		Rows:  1000,
	}

	// Project only 2 columns: name and email
	project := &ProjectionNode{
		Input: scan,
		Expressions: []parser.Expression{
			&parser.ColumnRef{Name: "name"},
			&parser.ColumnRef{Name: "email"},
		},
	}

	optimizer := NewOptimizer()
	optimized := optimizer.ApplyProjectionPushdown(project)

	// Verify the optimized plan is not nil
	if optimized == nil {
		t.Fatal("expected optimized plan, got nil")
	}

	// Find the TableScanNode in the optimized plan
	var foundScan *TableScanNode
	findTableScan(optimized, &foundScan)

	if foundScan == nil {
		t.Fatal("expected to find TableScanNode in optimized plan")
	}

	// Verify RequiredColumns is set on the scan
	if foundScan.RequiredColumns == nil {
		t.Fatal("expected RequiredColumns to be set on TableScanNode after projection pushdown")
	}

	// Verify exactly the right columns are required
	if len(foundScan.RequiredColumns) != 2 {
		t.Errorf("expected 2 required columns, got %d", len(foundScan.RequiredColumns))
	}

	// Check that name and email are in RequiredColumns
	hasName := false
	hasEmail := false
	for _, col := range foundScan.RequiredColumns {
		if col == "name" {
			hasName = true
		}
		if col == "email" {
			hasEmail = true
		}
	}

	if !hasName {
		t.Error("expected 'name' to be in RequiredColumns")
	}
	if !hasEmail {
		t.Error("expected 'email' to be in RequiredColumns")
	}
}

// TestProjectionPushdown_ThroughFilter tests that projection pushdown
// works correctly when there's a filter between projection and scan
func TestProjectionPushdown_ThroughFilter(t *testing.T) {
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id"},
			{Name: "name"},
			{Name: "email"},
			{Name: "age"},
		},
	}

	scan := &TableScanNode{
		Table: tableDef,
		Cost:  100.0,
		Rows:  1000,
	}

	// Filter uses the 'age' column
	filter := &FilterNode{
		Input: scan,
		Condition: &parser.BinaryExpr{
			Left: &parser.ColumnRef{Name: "age"},
			Op:   0, // doesn't matter for this test
		},
		Selectivity: 0.5,
	}

	// Project only 'name' column
	project := &ProjectionNode{
		Input:       filter,
		Expressions: []parser.Expression{&parser.ColumnRef{Name: "name"}},
	}

	optimizer := NewOptimizer()
	optimized := optimizer.ApplyProjectionPushdown(project)

	if optimized == nil {
		t.Fatal("expected optimized plan, got nil")
	}

	// Find the TableScanNode
	var foundScan *TableScanNode
	findTableScan(optimized, &foundScan)

	if foundScan == nil {
		t.Fatal("expected to find TableScanNode")
	}

	// RequiredColumns should include both 'name' (from projection) and 'age' (from filter)
	if foundScan.RequiredColumns == nil {
		t.Fatal("expected RequiredColumns to be set")
	}

	hasName := false
	hasAge := false
	for _, col := range foundScan.RequiredColumns {
		if col == "name" {
			hasName = true
		}
		if col == "age" {
			hasAge = true
		}
	}

	if !hasName {
		t.Error("expected 'name' to be in RequiredColumns (used in projection)")
	}
	if !hasAge {
		t.Error("expected 'age' to be in RequiredColumns (used in filter)")
	}
}

// TestProjectionPushdown_CostReduction tests that projection pushdown reduces cost
func TestProjectionPushdown_CostReduction(t *testing.T) {
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id"},
			{Name: "name"},
			{Name: "email"},
			{Name: "age"},
			{Name: "address"},
			{Name: "phone"},
			{Name: "created_at"},
			{Name: "updated_at"},
		},
	}

	scan := &TableScanNode{
		Table: tableDef,
		Cost:  100.0, // Base cost for reading all 8 columns
		Rows:  1000,
	}

	// Project only 2 out of 8 columns
	project := &ProjectionNode{
		Input: scan,
		Expressions: []parser.Expression{
			&parser.ColumnRef{Name: "name"},
			&parser.ColumnRef{Name: "email"},
		},
	}

	originalCost := project.EstimatedCost()

	optimizer := NewOptimizer()
	optimized := optimizer.ApplyProjectionPushdown(project)

	optimizedCost := optimized.EstimatedCost()

	// Cost should be lower because we're reading fewer columns
	if optimizedCost >= originalCost {
		t.Errorf("expected optimized cost (%f) to be less than original cost (%f)", optimizedCost, originalCost)
	}
}

// findTableScan is a helper to recursively find a TableScanNode in a plan tree
func findTableScan(node PlanNode, result **TableScanNode) {
	if node == nil {
		return
	}

	switch n := node.(type) {
	case *TableScanNode:
		*result = n
	case *ProjectionNode:
		findTableScan(n.Input, result)
	case *FilterNode:
		findTableScan(n.Input, result)
	case *NestedLoopJoinNode:
		findTableScan(n.Left, result)
		if *result == nil {
			findTableScan(n.Right, result)
		}
	case *HashJoinNode:
		findTableScan(n.Left, result)
		if *result == nil {
			findTableScan(n.Right, result)
		}
	}
}
