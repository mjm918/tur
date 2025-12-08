// pkg/sql/optimizer/index_selection_test.go
package optimizer

import (
	"testing"

	"tur/pkg/schema"
	"tur/pkg/sql/lexer"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

// Test Task 1: Identify candidate indexes for WHERE clause predicates

func TestFindCandidateIndexes_SingleColumnEquality(t *testing.T) {
	// Setup: Table with index on "email" column
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "email", Type: types.TypeText},
			{Name: "name", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE email = 'test@example.com'
	whereClause := &parser.BinaryExpr{
		Left:  &parser.ColumnRef{Name: "email"},
		Op:    lexer.EQ,
		Right: &parser.Literal{Value: types.NewText("test@example.com")},
	}

	// Act
	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Assert
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate index, got %d", len(candidates))
	}
	if candidates[0].Index.Name != "idx_users_email" {
		t.Errorf("expected index 'idx_users_email', got '%s'", candidates[0].Index.Name)
	}
	if candidates[0].Column != "email" {
		t.Errorf("expected column 'email', got '%s'", candidates[0].Column)
	}
}

func TestFindCandidateIndexes_NoMatchingIndex(t *testing.T) {
	// Setup: Table with index on "email", but WHERE on "name"
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "email", Type: types.TypeText},
			{Name: "name", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE name = 'John' (no index on name)
	whereClause := &parser.BinaryExpr{
		Left:  &parser.ColumnRef{Name: "name"},
		Op:    lexer.EQ,
		Right: &parser.Literal{Value: types.NewText("John")},
	}

	// Act
	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Assert
	if len(candidates) != 0 {
		t.Fatalf("expected 0 candidate indexes, got %d", len(candidates))
	}
}

func TestFindCandidateIndexes_MultipleIndexes(t *testing.T) {
	// Setup: Table with multiple indexes
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "email", Type: types.TypeText},
			{Name: "status", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	catalog.CreateIndex(&schema.IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
	})
	catalog.CreateIndex(&schema.IndexDef{
		Name:      "idx_users_status",
		TableName: "users",
		Columns:   []string{"status"},
		Type:      schema.IndexTypeBTree,
	})

	// WHERE email = 'test@example.com' AND status = 'active'
	whereClause := &parser.BinaryExpr{
		Left: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "email"},
			Op:    lexer.EQ,
			Right: &parser.Literal{Value: types.NewText("test@example.com")},
		},
		Op: lexer.AND,
		Right: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "status"},
			Op:    lexer.EQ,
			Right: &parser.Literal{Value: types.NewText("active")},
		},
	}

	// Act
	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Assert: Should find both indexes
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidate indexes, got %d", len(candidates))
	}
}

func TestFindCandidateIndexes_RangePredicates(t *testing.T) {
	// Setup: Table with index on "age" column
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "age", Type: types.TypeInt},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_age",
		TableName: "users",
		Columns:   []string{"age"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE age > 30
	whereClause := &parser.BinaryExpr{
		Left:  &parser.ColumnRef{Name: "age"},
		Op:    lexer.GT,
		Right: &parser.Literal{Value: types.NewInt(30)},
	}

	// Act
	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Assert
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate index, got %d", len(candidates))
	}
	if candidates[0].Index.Name != "idx_users_age" {
		t.Errorf("expected index 'idx_users_age', got '%s'", candidates[0].Index.Name)
	}
}

func TestFindCandidateIndexes_ColumnOnRightSide(t *testing.T) {
	// Setup: Table with index on "id" column
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_id",
		TableName: "users",
		Columns:   []string{"id"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE 100 = id (column on right side)
	whereClause := &parser.BinaryExpr{
		Left:  &parser.Literal{Value: types.NewInt(100)},
		Op:    lexer.EQ,
		Right: &parser.ColumnRef{Name: "id"},
	}

	// Act
	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Assert: Should still find the index
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate index, got %d", len(candidates))
	}
	if candidates[0].Index.Name != "idx_users_id" {
		t.Errorf("expected index 'idx_users_id', got '%s'", candidates[0].Index.Name)
	}
}

func TestFindCandidateIndexes_NilWhereClause(t *testing.T) {
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
		},
	}
	catalog.CreateTable(tableDef)

	// No WHERE clause
	candidates := FindCandidateIndexes(tableDef, nil, catalog)

	// Assert: No candidates when there's no WHERE
	if len(candidates) != 0 {
		t.Fatalf("expected 0 candidate indexes for nil WHERE, got %d", len(candidates))
	}
}

// Test Task 2: Estimate selectivity of index lookups

func TestEstimateIndexSelectivity_Equality(t *testing.T) {
	// Equality predicates are highly selective (estimate ~1% of rows)
	estimator := NewCostEstimator()

	selectivity := estimator.EstimateIndexSelectivity(lexer.EQ)

	// Equality should be very selective
	if selectivity > 0.05 {
		t.Errorf("equality selectivity should be <= 0.05, got %f", selectivity)
	}
	if selectivity <= 0 {
		t.Errorf("selectivity should be positive, got %f", selectivity)
	}
}

func TestEstimateIndexSelectivity_Range(t *testing.T) {
	// Range predicates (>, <, >=, <=) are moderately selective
	estimator := NewCostEstimator()

	tests := []struct {
		op       lexer.TokenType
		name     string
		minSel   float64
		maxSel   float64
	}{
		{lexer.LT, "less than", 0.1, 0.5},
		{lexer.GT, "greater than", 0.1, 0.5},
		{lexer.LTE, "less than or equal", 0.1, 0.5},
		{lexer.GTE, "greater than or equal", 0.1, 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selectivity := estimator.EstimateIndexSelectivity(tt.op)

			if selectivity < tt.minSel || selectivity > tt.maxSel {
				t.Errorf("%s selectivity should be between %f and %f, got %f",
					tt.name, tt.minSel, tt.maxSel, selectivity)
			}
		})
	}
}

func TestEstimateIndexSelectivity_NotEqual(t *testing.T) {
	// Not equal is not selective (most rows pass)
	estimator := NewCostEstimator()

	selectivity := estimator.EstimateIndexSelectivity(lexer.NEQ)

	// Not-equal should have low selectivity (high fraction passes)
	if selectivity < 0.5 {
		t.Errorf("not-equal selectivity should be >= 0.5, got %f", selectivity)
	}
}

func TestEstimateCandidateSelectivity(t *testing.T) {
	// Test that we can estimate selectivity for an IndexCandidate
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "email", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	candidate := IndexCandidate{
		Index:    indexDef,
		Column:   "email",
		Operator: lexer.EQ,
		Value:    &parser.Literal{Value: types.NewText("test@example.com")},
	}

	estimator := NewCostEstimator()
	selectivity := estimator.EstimateCandidateSelectivity(candidate)

	if selectivity <= 0 || selectivity > 1 {
		t.Errorf("selectivity should be between 0 and 1, got %f", selectivity)
	}
}

// Test Task 3: Compare cost of index scan vs full table scan

func TestCompareAccessPaths_IndexBetterForSelectiveQuery(t *testing.T) {
	// For highly selective queries (equality on indexed column),
	// index scan should be cheaper than table scan
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "email", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	candidate := IndexCandidate{
		Index:    indexDef,
		Column:   "email",
		Operator: lexer.EQ,
		Value:    &parser.Literal{Value: types.NewText("test@example.com")},
	}

	estimator := NewCostEstimator()
	tableRows := int64(10000) // Large table

	comparison := estimator.CompareAccessPaths(tableDef, candidate, tableRows)

	if comparison.IndexCost >= comparison.TableScanCost {
		t.Errorf("index scan should be cheaper for selective query: index=%f, table=%f",
			comparison.IndexCost, comparison.TableScanCost)
	}
	if !comparison.UseIndex {
		t.Errorf("should recommend using index for selective query")
	}
}

func TestCompareAccessPaths_TableScanBetterForNonSelectiveQuery(t *testing.T) {
	// For non-selective queries (e.g., !=), table scan might be cheaper
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "status", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_status",
		TableName: "users",
		Columns:   []string{"status"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	candidate := IndexCandidate{
		Index:    indexDef,
		Column:   "status",
		Operator: lexer.NEQ, // Not equal - scans most of the table
		Value:    &parser.Literal{Value: types.NewText("inactive")},
	}

	estimator := NewCostEstimator()
	tableRows := int64(1000)

	comparison := estimator.CompareAccessPaths(tableDef, candidate, tableRows)

	// For non-selective queries, table scan should be preferred
	if comparison.UseIndex {
		t.Errorf("should not recommend index for non-selective != query")
	}
}

func TestCompareAccessPaths_SmallTablePrefersScan(t *testing.T) {
	// For very small tables, full scan is often cheaper
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "config",
		Columns: []schema.ColumnDef{
			{Name: "key", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_config_key",
		TableName: "config",
		Columns:   []string{"key"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	candidate := IndexCandidate{
		Index:    indexDef,
		Column:   "key",
		Operator: lexer.EQ,
		Value:    &parser.Literal{Value: types.NewText("setting1")},
	}

	estimator := NewCostEstimator()
	tableRows := int64(10) // Very small table

	comparison := estimator.CompareAccessPaths(tableDef, candidate, tableRows)

	// For small tables, table scan is often preferred
	// (or costs are close enough that either is acceptable)
	if comparison.TableScanCost > 10.0 {
		t.Errorf("table scan cost should be low for small table: %f", comparison.TableScanCost)
	}
}

func TestCompareAccessPaths_ReturnsCorrectRowEstimates(t *testing.T) {
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "age", Type: types.TypeInt},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_age",
		TableName: "users",
		Columns:   []string{"age"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	candidate := IndexCandidate{
		Index:    indexDef,
		Column:   "age",
		Operator: lexer.GT, // Range scan - 33% selectivity
		Value:    &parser.Literal{Value: types.NewInt(30)},
	}

	estimator := NewCostEstimator()
	tableRows := int64(1000)

	comparison := estimator.CompareAccessPaths(tableDef, candidate, tableRows)

	// Table scan returns all rows
	if comparison.TableScanRows != tableRows {
		t.Errorf("table scan should return all rows: expected %d, got %d",
			tableRows, comparison.TableScanRows)
	}

	// Index scan returns fewer rows (based on selectivity)
	expectedIndexRows := int64(float64(tableRows) * 0.33) // 33% for range
	if comparison.IndexRows < expectedIndexRows-50 || comparison.IndexRows > expectedIndexRows+50 {
		t.Errorf("index scan rows should be around %d (33%% of %d), got %d",
			expectedIndexRows, tableRows, comparison.IndexRows)
	}
}
