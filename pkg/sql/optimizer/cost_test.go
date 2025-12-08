// pkg/sql/optimizer/cost_test.go
package optimizer

import (
	"testing"
	"tur/pkg/schema"
	"tur/pkg/types"
)

// TestCostEstimator_TableScanCost tests table scan cost estimation
func TestCostEstimator_TableScanCost(t *testing.T) {
	// Create a simple table definition
	table := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "name", Type: types.TypeText},
			{Name: "age", Type: types.TypeInt},
		},
		RootPage: 1,
	}

	estimator := NewCostEstimator()

	// Test with 1000 rows
	cost, rows := estimator.EstimateTableScan(table, 1000)

	// Cost should be proportional to number of rows
	// Base formula: rows * PAGE_READ_COST
	// With 1000 rows and assuming 100 rows per page = 10 pages
	// Cost should be around 10 * PAGE_READ_COST
	if cost <= 0 {
		t.Errorf("expected positive cost, got %f", cost)
	}

	if rows != 1000 {
		t.Errorf("expected 1000 rows, got %d", rows)
	}
}

// TestCostEstimator_TableScanCost_Empty tests cost for empty table
func TestCostEstimator_TableScanCost_Empty(t *testing.T) {
	table := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
		},
		RootPage: 1,
	}

	estimator := NewCostEstimator()
	cost, rows := estimator.EstimateTableScan(table, 0)

	// Even empty table has minimum cost (reading root page)
	if cost <= 0 {
		t.Errorf("expected positive minimum cost, got %f", cost)
	}

	if rows != 0 {
		t.Errorf("expected 0 rows, got %d", rows)
	}
}

// TestCostEstimator_TableScanCost_Large tests cost for large table
func TestCostEstimator_TableScanCost_Large(t *testing.T) {
	table := &schema.TableDef{
		Name: "large_table",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "data", Type: types.TypeBlob},
		},
		RootPage: 1,
	}

	estimator := NewCostEstimator()
	cost, rows := estimator.EstimateTableScan(table, 1000000)

	// Cost should scale with row count
	if cost <= 0 {
		t.Errorf("expected positive cost, got %f", cost)
	}

	if rows != 1000000 {
		t.Errorf("expected 1000000 rows, got %d", rows)
	}

	// Cost should be higher for more rows
	smallCost, _ := estimator.EstimateTableScan(table, 1000)
	if cost <= smallCost {
		t.Errorf("expected large table cost (%f) > small table cost (%f)", cost, smallCost)
	}
}

// TestCostEstimator_SelectivityEstimate tests selectivity estimation for WHERE clauses
func TestCostEstimator_SelectivityEstimate(t *testing.T) {
	estimator := NewCostEstimator()

	tests := []struct {
		name         string
		operator     string
		wantMin      float64
		wantMax      float64
	}{
		{"equality", "=", 0.0, 0.1},   // Equality is very selective
		{"inequality", "!=", 0.8, 1.0}, // Inequality is not very selective
		{"less than", "<", 0.2, 0.5},   // Range operators are moderately selective
		{"greater than", ">", 0.2, 0.5},
		{"less equal", "<=", 0.2, 0.5},
		{"greater equal", ">=", 0.2, 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selectivity := estimator.EstimateSelectivity(tt.operator)
			if selectivity < tt.wantMin || selectivity > tt.wantMax {
				t.Errorf("selectivity for %s = %f, want between %f and %f",
					tt.operator, selectivity, tt.wantMin, tt.wantMax)
			}
		})
	}
}

// TestCostEstimator_Constants tests cost constants are reasonable
func TestCostEstimator_Constants(t *testing.T) {
	estimator := NewCostEstimator()

	// Page read should be more expensive than CPU operations
	if PAGE_READ_COST <= CPU_TUPLE_COST {
		t.Error("PAGE_READ_COST should be greater than CPU_TUPLE_COST")
	}

	// Check that constants are positive
	if PAGE_READ_COST <= 0 {
		t.Error("PAGE_READ_COST should be positive")
	}
	if CPU_TUPLE_COST <= 0 {
		t.Error("CPU_TUPLE_COST should be positive")
	}

	// Sanity check estimator is initialized
	if estimator == nil {
		t.Error("estimator should not be nil")
	}
}
