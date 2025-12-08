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

// TestCostEstimator_IndexScanCost tests index scan cost estimation
func TestCostEstimator_IndexScanCost(t *testing.T) {
	index := &schema.IndexDef{
		Name:      "idx_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
		RootPage:  2,
	}

	estimator := NewCostEstimator()

	// Test with equality predicate (highly selective)
	selectivity := 0.01 // 1% selectivity
	cost, rows := estimator.EstimateIndexScan(index, 10000, selectivity)

	// Index scan should be cheaper than full table scan for selective queries
	tableScanCost, _ := estimator.EstimateTableScan(&schema.TableDef{
		Name:     "users",
		RootPage: 1,
	}, 10000)

	if cost >= tableScanCost {
		t.Errorf("expected index scan cost (%f) < table scan cost (%f)", cost, tableScanCost)
	}

	// Should return only selective rows
	expectedRows := int64(float64(10000) * selectivity)
	if rows != expectedRows {
		t.Errorf("expected %d rows, got %d", expectedRows, rows)
	}
}

// TestCostEstimator_IndexScanCost_LowSelectivity tests index scan with low selectivity
func TestCostEstimator_IndexScanCost_LowSelectivity(t *testing.T) {
	index := &schema.IndexDef{
		Name:      "idx_status",
		TableName: "users",
		Columns:   []string{"status"},
		Type:      schema.IndexTypeBTree,
		RootPage:  2,
	}

	estimator := NewCostEstimator()

	// Test with low selectivity (range query returning 50% of rows)
	selectivity := 0.5
	cost, rows := estimator.EstimateIndexScan(index, 10000, selectivity)

	// For low selectivity, index scan might be more expensive than table scan
	// But we still calculate it correctly
	if cost <= 0 {
		t.Errorf("expected positive cost, got %f", cost)
	}

	expectedRows := int64(float64(10000) * selectivity)
	if rows != expectedRows {
		t.Errorf("expected %d rows, got %d", expectedRows, rows)
	}
}

// TestCostEstimator_IndexScanCost_HNSW tests HNSW index scan cost
func TestCostEstimator_IndexScanCost_HNSW(t *testing.T) {
	index := &schema.IndexDef{
		Name:      "idx_embedding",
		TableName: "documents",
		Columns:   []string{"embedding"},
		Type:      schema.IndexTypeHNSW,
		RootPage:  3,
	}

	estimator := NewCostEstimator()

	// HNSW index for KNN search (typically returns small number of results)
	selectivity := 0.001 // 0.1% - top-K results
	cost, rows := estimator.EstimateIndexScan(index, 1000000, selectivity)

	// HNSW should be very efficient even for large datasets
	if cost <= 0 {
		t.Errorf("expected positive cost, got %f", cost)
	}

	// Should return small number of nearest neighbors
	expectedRows := int64(float64(1000000) * selectivity)
	if rows != expectedRows {
		t.Errorf("expected %d rows, got %d", expectedRows, rows)
	}

	// HNSW cost should scale logarithmically, not linearly
	// Test with larger dataset
	largeCost, _ := estimator.EstimateIndexScan(index, 10000000, selectivity)

	// Cost shouldn't increase by 10x even though data increased by 10x
	if largeCost >= cost*10 {
		t.Errorf("HNSW cost should scale logarithmically, got %fx increase for 10x data", largeCost/cost)
	}
}

// TestCostEstimator_CompareIndexVsTableScan tests choosing between index and table scan
func TestCostEstimator_CompareIndexVsTableScan(t *testing.T) {
	table := &schema.TableDef{
		Name:     "users",
		RootPage: 1,
	}

	index := &schema.IndexDef{
		Name:      "idx_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
		RootPage:  2,
	}

	estimator := NewCostEstimator()
	tableRows := int64(100000)

	tests := []struct {
		name         string
		selectivity  float64
		preferIndex  bool
	}{
		{"highly selective", 0.001, true},  // Index should win
		{"moderately selective", 0.1, true}, // Index should win
		{"low selectivity", 0.5, false},     // Table scan might win
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableCost, _ := estimator.EstimateTableScan(table, tableRows)
			indexCost, _ := estimator.EstimateIndexScan(index, tableRows, tt.selectivity)

			if tt.preferIndex {
				if indexCost >= tableCost {
					t.Errorf("expected index scan (%f) < table scan (%f) for %s",
						indexCost, tableCost, tt.name)
				}
			}
		})
	}
}
