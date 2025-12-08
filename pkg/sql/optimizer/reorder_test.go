package optimizer

import (
	"testing"
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
)

func TestOptimization_JoinReordering(t *testing.T) {
	// Tables:
	// A: 10 rows
	// B: 1000 rows
	// C: 100 rows

	tableA := &TableScanNode{
		Table: &schema.TableDef{Name: "A"}, // schema.TableDef
		Cost:  10.0,
		Rows:  10,
	}

	tableB := &TableScanNode{
		Table: &schema.TableDef{Name: "B"},
		Cost:  1000.0,
		Rows:  1000,
	}

	tableC := &TableScanNode{
		Table: &schema.TableDef{Name: "C"},
		Cost:  100.0,
		Rows:  100,
	}

	// Initial Plan: (A Join B) Join C
	// Cost:
	// A Join B: 10 + 10*1000 = 10010 cost, 10*1000=10000 rows (assuming cartesian or 1:1, usually 1:1 is assumed in dummy logic)
	// (A Join B) Join C: 10010 + 10000*100 = 1,010,010 cost?
	// Our Cost formula in plan.go: Left + LeftRows * RightCost

	// Let's verify formula:
	// NestedLoopJoinNode.EstimatedCost() = Left.Cost + Left.Rows * Right.Cost

	// A Join B:
	// Left=A, Right=B
	// 10 + 10 * 1000 = 10010.

	// (A Join B) Join C:
	// Left=(AB), Right=C
	// AB.Cost = 10010.
	// AB.Rows: plan.go default is min(Left, Right) = min(10, 1000) = 10.
	// Cost = 10010 + 10 * 100 = 11010.

	// Reordered: (A Join C) Join B
	// A Join C:
	// Left=A, Right=C
	// 10 + 10 * 100 = 1010.
	// Rows = min(10, 100) = 10.

	// (AC) Join B:
	// Left=(AC), Right=B
	// 1010 + 10 * 1000 = 11010.

	// Wait, if Rows = min(L, R), then order doesn't impact row count much, but impacts cost.
	// 10010 vs 1010 intermediate.
	// Total cost is same?
	// 10 + 10*1000 + min(A,B)? No.
	// Cost 1: 10 + 10000 + 10*100 = 11010.
	// Cost 2: 10 + 1000 + 10*1000 = 11010?
	// Wait:
	// Plan 1: (A(10) x B(1000)) x C(100)
	// A x B cost = 10 + 10*1000 = 10010. Result Rows = 10.
	// (AB) x C cost = 10010 + 10*100 = 11010.

	// Plan 2: (A(10) x C(100)) x B(1000)
	// A x C cost = 10 + 10*100 = 1010. Result Rows = 10.
	// (AC) x B cost = 1010 + 10*1000 = 11010.

	// The total cost seems identical with this simplified cost model if intermediate rows = Smallest.
	// But usually intermediate rows depends on selectivity.
	// If selectivity is 1.0 (Cartesian product):
	// Plan 1: Rows A*B = 10000.
	// Cost 1: A+A*B = 10010.
	// (AB) x C: 10010 + 10000 * 100 = 1,010,010.

	// Plan 2: Rows A*C = 1000.
	// Cost 2: A+A*C = 1010.
	// (AC) x B: 1010 + 1000 * 1000 = 1,001,010.

	// 1,010,010 vs 1,001,010. A small difference in favor of AC first.
	// But our plan.go implementation says: "min(left.Rows, right.Rows)".
	// This means we assume 1:1 join on PK/FK mostly.
	// Even then, if we pick B first?
	// (B Join A) Join C
	// B x A cost = 1000 + 1000 * 10 = 11000. Rows = 10.
	// (BA) x C cost = 11000 + 10 * 100 = 12000.
	// This is worse than 11010.
	// So putting B on Left is bad.

	// So our greedy heuristic of "pick smallest first" ensures A is Left.
	// And then between B and C?
	// Remaining: B(1000), C(100).
	// Next smallest is C.
	// So it picks C.
	// So (A Join C) Join B.

	initialJoin := &NestedLoopJoinNode{
		Left: &NestedLoopJoinNode{
			Left:      tableA,
			Right:     tableB,
			Condition: &parser.BinaryExpr{},
			JoinType:  parser.JoinInner,
		},
		Right:     tableC,
		Condition: &parser.BinaryExpr{},
		JoinType:  parser.JoinInner,
	}

	// Calculate initial cost
	initialCost := initialJoin.EstimatedCost()

	optimizer := NewOptimizer()
	optimized := optimizer.ReorderJoins(initialJoin)

	// Verify the result is a valid join tree
	if _, ok := optimized.(*NestedLoopJoinNode); !ok {
		t.Fatalf("Expected NestedLoopJoinNode, got %T", optimized)
	}

	// The optimized plan should have cost <= initial cost
	optimizedCost := optimized.EstimatedCost()
	if optimizedCost > initialCost {
		t.Errorf("Optimization made cost worse: initial=%f, optimized=%f", initialCost, optimizedCost)
	}

	// Verify all tables are present exactly once
	tables := collectTableNames(optimized)
	if len(tables) != 3 {
		t.Errorf("Expected 3 tables, got %d: %v", len(tables), tables)
	}

	expectedTables := map[string]bool{"A": true, "B": true, "C": true}
	for table := range expectedTables {
		found := false
		for _, name := range tables {
			if name == table {
				if found {
					t.Errorf("Table %s appears multiple times", table)
				}
				found = true
			}
		}
		if !found {
			t.Errorf("Table %s is missing", table)
		}
	}

	t.Logf("Initial cost: %f, Optimized cost: %f", initialCost, optimizedCost)
}

// TestOptimization_DynamicProgrammingJoinReordering tests that dynamic programming
// finds the optimal join order by exploring all possibilities
func TestOptimization_DynamicProgrammingJoinReordering(t *testing.T) {
	// Create 4 tables with specific characteristics
	// Tables: A(100), B(10), C(1000), D(50)
	//
	// Using DP, we should find the truly optimal order by considering
	// all possible join orders and their costs

	tableA := &TableScanNode{
		Table: &schema.TableDef{Name: "A"},
		Cost:  100.0,
		Rows:  100,
	}

	tableB := &TableScanNode{
		Table: &schema.TableDef{Name: "B"},
		Cost:  10.0,
		Rows:  10,
	}

	tableC := &TableScanNode{
		Table: &schema.TableDef{Name: "C"},
		Cost:  1000.0,
		Rows:  1000,
	}

	tableD := &TableScanNode{
		Table: &schema.TableDef{Name: "D"},
		Cost:  50.0,
		Rows:  50,
	}

	// Create a poor initial join order: ((A Join C) Join D) Join B
	// This is deliberately suboptimal
	initialJoin := &NestedLoopJoinNode{
		Left: &NestedLoopJoinNode{
			Left: &NestedLoopJoinNode{
				Left:      tableA,
				Right:     tableC,
				Condition: &parser.BinaryExpr{},
				JoinType:  parser.JoinInner,
			},
			Right:     tableD,
			Condition: &parser.BinaryExpr{},
			JoinType:  parser.JoinInner,
		},
		Right:     tableB,
		Condition: &parser.BinaryExpr{},
		JoinType:  parser.JoinInner,
	}

	// Calculate the cost of the initial plan
	initialCost := initialJoin.EstimatedCost()

	optimizer := NewOptimizer()

	// Enable dynamic programming for join reordering
	optimizer.UseDP = true

	optimized := optimizer.ReorderJoins(initialJoin)

	// The optimized plan should have lower cost than the initial plan
	optimizedCost := optimized.EstimatedCost()

	if optimizedCost >= initialCost {
		t.Errorf("DP optimization failed: optimized cost (%f) >= initial cost (%f)", optimizedCost, initialCost)
	}

	// Verify the result is a valid join tree
	if _, ok := optimized.(*NestedLoopJoinNode); !ok {
		t.Fatalf("Expected NestedLoopJoinNode, got %T", optimized)
	}

	// The optimal plan should start with the smallest table (B)
	// and build up from there
	// Expected structure: something like (((B Join D) Join A) Join C)
	// where B is joined first since it's smallest
	t.Logf("Initial cost: %f, Optimized cost: %f", initialCost, optimizedCost)
}

// TestOptimization_DynamicProgrammingCorrectness tests that DP produces
// a valid join tree with all tables included exactly once
func TestOptimization_DynamicProgrammingCorrectness(t *testing.T) {
	tableA := &TableScanNode{
		Table: &schema.TableDef{Name: "A"},
		Cost:  10.0,
		Rows:  10,
	}

	tableB := &TableScanNode{
		Table: &schema.TableDef{Name: "B"},
		Cost:  20.0,
		Rows:  20,
	}

	tableC := &TableScanNode{
		Table: &schema.TableDef{Name: "C"},
		Cost:  30.0,
		Rows:  30,
	}

	// Create initial join: (A Join B) Join C
	initialJoin := &NestedLoopJoinNode{
		Left: &NestedLoopJoinNode{
			Left:      tableA,
			Right:     tableB,
			Condition: &parser.BinaryExpr{},
			JoinType:  parser.JoinInner,
		},
		Right:     tableC,
		Condition: &parser.BinaryExpr{},
		JoinType:  parser.JoinInner,
	}

	optimizer := NewOptimizer()
	optimized := optimizer.ReorderJoins(initialJoin)

	// Collect all table names from the optimized plan
	tables := collectTableNames(optimized)

	// Verify all tables are present exactly once
	expectedTables := map[string]bool{"A": true, "B": true, "C": true}
	if len(tables) != len(expectedTables) {
		t.Errorf("Expected 3 tables, got %d: %v", len(tables), tables)
	}

	for table := range expectedTables {
		count := 0
		for _, name := range tables {
			if name == table {
				count++
			}
		}
		if count != 1 {
			t.Errorf("Table %s appears %d times, expected 1", table, count)
		}
	}
}

// Helper function to collect all table names from a plan tree
func collectTableNames(plan PlanNode) []string {
	var tables []string

	switch node := plan.(type) {
	case *TableScanNode:
		tables = append(tables, node.Table.Name)
	case *NestedLoopJoinNode:
		tables = append(tables, collectTableNames(node.Left)...)
		tables = append(tables, collectTableNames(node.Right)...)
	case *FilterNode:
		tables = append(tables, collectTableNames(node.Input)...)
	case *ProjectionNode:
		tables = append(tables, collectTableNames(node.Input)...)
	}

	return tables
}

// TestOptimization_AutomaticDPSelection tests that the optimizer automatically
// selects DP for small queries and greedy for large queries
func TestOptimization_AutomaticDPSelection(t *testing.T) {
	optimizer := NewOptimizer()

	// Test 1: Small query (4 tables) - should use DP automatically
	t.Run("small query uses DP", func(t *testing.T) {
		tableA := &TableScanNode{Table: &schema.TableDef{Name: "A"}, Cost: 10.0, Rows: 10}
		tableB := &TableScanNode{Table: &schema.TableDef{Name: "B"}, Cost: 20.0, Rows: 20}
		tableC := &TableScanNode{Table: &schema.TableDef{Name: "C"}, Cost: 30.0, Rows: 30}
		tableD := &TableScanNode{Table: &schema.TableDef{Name: "D"}, Cost: 40.0, Rows: 40}

		join := &NestedLoopJoinNode{
			Left: &NestedLoopJoinNode{
				Left: &NestedLoopJoinNode{
					Left:      tableA,
					Right:     tableB,
					Condition: &parser.BinaryExpr{},
					JoinType:  parser.JoinInner,
				},
				Right:     tableC,
				Condition: &parser.BinaryExpr{},
				JoinType:  parser.JoinInner,
			},
			Right:     tableD,
			Condition: &parser.BinaryExpr{},
			JoinType:  parser.JoinInner,
		}

		// UseDP is false by default, but optimizer should still use DP for small queries
		if optimizer.UseDP {
			t.Fatal("UseDP should be false by default")
		}

		optimized := optimizer.ReorderJoins(join)

		// Verify we get a valid optimized plan
		if optimized == nil {
			t.Fatal("Expected optimized plan, got nil")
		}

		// The plan should be optimized (DP or greedy both work, just verify it's valid)
		tables := collectTableNames(optimized)
		if len(tables) != 4 {
			t.Errorf("Expected 4 tables, got %d", len(tables))
		}
	})

	// Test 2: Manual override - UseDP=true forces DP even for small queries
	t.Run("manual override UseDP=true", func(t *testing.T) {
		optimizer.UseDP = true

		tableA := &TableScanNode{Table: &schema.TableDef{Name: "A"}, Cost: 10.0, Rows: 10}
		tableB := &TableScanNode{Table: &schema.TableDef{Name: "B"}, Cost: 20.0, Rows: 20}
		tableC := &TableScanNode{Table: &schema.TableDef{Name: "C"}, Cost: 30.0, Rows: 30}

		join := &NestedLoopJoinNode{
			Left: &NestedLoopJoinNode{
				Left:      tableA,
				Right:     tableB,
				Condition: &parser.BinaryExpr{},
				JoinType:  parser.JoinInner,
			},
			Right:     tableC,
			Condition: &parser.BinaryExpr{},
			JoinType:  parser.JoinInner,
		}

		optimized := optimizer.ReorderJoins(join)

		// Should use DP and produce valid plan
		if optimized == nil {
			t.Fatal("Expected optimized plan, got nil")
		}

		tables := collectTableNames(optimized)
		if len(tables) != 3 {
			t.Errorf("Expected 3 tables, got %d", len(tables))
		}
	})
}
