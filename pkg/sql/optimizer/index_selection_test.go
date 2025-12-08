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

// Test Task 4: Choose index with lowest estimated cost

func TestSelectBestAccessPath_ChoosesCheapestIndex(t *testing.T) {
	// When multiple indexes could be used, choose the one with lowest cost
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

	// Index on email (will be used for equality - very selective)
	catalog.CreateIndex(&schema.IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
	})

	// Index on status (will be used for range - less selective)
	catalog.CreateIndex(&schema.IndexDef{
		Name:      "idx_users_status",
		TableName: "users",
		Columns:   []string{"status"},
		Type:      schema.IndexTypeBTree,
	})

	// WHERE email = 'test@example.com' AND status > 'active'
	whereClause := &parser.BinaryExpr{
		Left: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "email"},
			Op:    lexer.EQ,
			Right: &parser.Literal{Value: types.NewText("test@example.com")},
		},
		Op: lexer.AND,
		Right: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "status"},
			Op:    lexer.GT,
			Right: &parser.Literal{Value: types.NewText("active")},
		},
	}

	tableRows := int64(10000)
	result := SelectBestAccessPath(tableDef, whereClause, catalog, tableRows)

	// Should recommend using the email index (equality is more selective)
	if result.RecommendedIndex == nil {
		t.Fatal("should recommend an index for this query")
	}
	if result.RecommendedIndex.Name != "idx_users_email" {
		t.Errorf("should choose email index (most selective), got '%s'",
			result.RecommendedIndex.Name)
	}
}

func TestSelectBestAccessPath_NoIndexReturnsTableScan(t *testing.T) {
	// When no index matches, return table scan recommendation
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "name", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	// No indexes

	// WHERE name = 'John'
	whereClause := &parser.BinaryExpr{
		Left:  &parser.ColumnRef{Name: "name"},
		Op:    lexer.EQ,
		Right: &parser.Literal{Value: types.NewText("John")},
	}

	tableRows := int64(1000)
	result := SelectBestAccessPath(tableDef, whereClause, catalog, tableRows)

	// Should recommend table scan (no index available)
	if result.RecommendedIndex != nil {
		t.Errorf("should not recommend an index when none match, got '%s'",
			result.RecommendedIndex.Name)
	}
	if result.UseTableScan != true {
		t.Error("should recommend table scan when no index matches")
	}
}

func TestSelectBestAccessPath_TableScanBetterThanIndex(t *testing.T) {
	// When table scan is cheaper, recommend table scan even if index exists
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "status", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	catalog.CreateIndex(&schema.IndexDef{
		Name:      "idx_users_status",
		TableName: "users",
		Columns:   []string{"status"},
		Type:      schema.IndexTypeBTree,
	})

	// WHERE status != 'deleted' (non-selective - most rows match)
	whereClause := &parser.BinaryExpr{
		Left:  &parser.ColumnRef{Name: "status"},
		Op:    lexer.NEQ,
		Right: &parser.Literal{Value: types.NewText("deleted")},
	}

	tableRows := int64(1000)
	result := SelectBestAccessPath(tableDef, whereClause, catalog, tableRows)

	// Should prefer table scan for non-selective query
	if !result.UseTableScan {
		t.Error("should prefer table scan for non-selective query")
	}
}

func TestSelectBestAccessPath_NilWhereClauseUsesTableScan(t *testing.T) {
	// No WHERE clause = full table scan
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
		},
	}
	catalog.CreateTable(tableDef)

	catalog.CreateIndex(&schema.IndexDef{
		Name:      "idx_users_id",
		TableName: "users",
		Columns:   []string{"id"},
		Type:      schema.IndexTypeBTree,
	})

	tableRows := int64(1000)
	result := SelectBestAccessPath(tableDef, nil, catalog, tableRows)

	// Should recommend table scan when no WHERE clause
	if result.RecommendedIndex != nil {
		t.Error("should not recommend index without WHERE clause")
	}
	if !result.UseTableScan {
		t.Error("should recommend table scan without WHERE clause")
	}
}

// Test Task 5: Handle multi-column index prefix matching

func TestFindCandidateIndexes_MultiColumnIndexFirstColumn(t *testing.T) {
	// A composite index (a, b, c) should be usable when querying on column 'a' only
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "orders",
		Columns: []schema.ColumnDef{
			{Name: "customer_id", Type: types.TypeInt},
			{Name: "order_date", Type: types.TypeText},
			{Name: "status", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	// Composite index on (customer_id, order_date, status)
	indexDef := &schema.IndexDef{
		Name:      "idx_orders_composite",
		TableName: "orders",
		Columns:   []string{"customer_id", "order_date", "status"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE customer_id = 123 (uses first column of composite index)
	whereClause := &parser.BinaryExpr{
		Left:  &parser.ColumnRef{Name: "customer_id"},
		Op:    lexer.EQ,
		Right: &parser.Literal{Value: types.NewInt(123)},
	}

	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate index, got %d", len(candidates))
	}
	if candidates[0].Index.Name != "idx_orders_composite" {
		t.Errorf("expected composite index, got '%s'", candidates[0].Index.Name)
	}
	if candidates[0].PrefixLength != 1 {
		t.Errorf("expected prefix length 1, got %d", candidates[0].PrefixLength)
	}
}

func TestFindCandidateIndexes_MultiColumnIndexPrefix(t *testing.T) {
	// A composite index should be usable for prefix predicates
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "orders",
		Columns: []schema.ColumnDef{
			{Name: "customer_id", Type: types.TypeInt},
			{Name: "order_date", Type: types.TypeText},
			{Name: "status", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_orders_composite",
		TableName: "orders",
		Columns:   []string{"customer_id", "order_date", "status"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE customer_id = 123 AND order_date = '2024-01-01'
	// Uses first two columns of the composite index
	whereClause := &parser.BinaryExpr{
		Left: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "customer_id"},
			Op:    lexer.EQ,
			Right: &parser.Literal{Value: types.NewInt(123)},
		},
		Op: lexer.AND,
		Right: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "order_date"},
			Op:    lexer.EQ,
			Right: &parser.Literal{Value: types.NewText("2024-01-01")},
		},
	}

	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Should find the composite index with prefix length 2
	found := false
	for _, c := range candidates {
		if c.Index.Name == "idx_orders_composite" && c.PrefixLength == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Error("should find composite index with prefix length 2")
	}
}

func TestFindCandidateIndexes_MultiColumnIndexNotPrefix(t *testing.T) {
	// A composite index (a, b, c) should NOT be usable when querying only 'b'
	// because 'b' is not a prefix of the index
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "orders",
		Columns: []schema.ColumnDef{
			{Name: "customer_id", Type: types.TypeInt},
			{Name: "order_date", Type: types.TypeText},
			{Name: "status", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_orders_composite",
		TableName: "orders",
		Columns:   []string{"customer_id", "order_date", "status"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE order_date = '2024-01-01' (second column only - not a prefix)
	whereClause := &parser.BinaryExpr{
		Left:  &parser.ColumnRef{Name: "order_date"},
		Op:    lexer.EQ,
		Right: &parser.Literal{Value: types.NewText("2024-01-01")},
	}

	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Should NOT find the composite index because order_date is not the first column
	for _, c := range candidates {
		if c.Index.Name == "idx_orders_composite" {
			t.Error("should not match composite index when predicate is not on prefix column")
		}
	}
}

func TestFindCandidateIndexes_FullMultiColumnMatch(t *testing.T) {
	// Using all columns of a composite index should work
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "orders",
		Columns: []schema.ColumnDef{
			{Name: "a", Type: types.TypeInt},
			{Name: "b", Type: types.TypeInt},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_ab",
		TableName: "orders",
		Columns:   []string{"a", "b"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE a = 1 AND b = 2
	whereClause := &parser.BinaryExpr{
		Left: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "a"},
			Op:    lexer.EQ,
			Right: &parser.Literal{Value: types.NewInt(1)},
		},
		Op: lexer.AND,
		Right: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "b"},
			Op:    lexer.EQ,
			Right: &parser.Literal{Value: types.NewInt(2)},
		},
	}

	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Should find index with full prefix length
	found := false
	for _, c := range candidates {
		if c.Index.Name == "idx_ab" && c.PrefixLength == 2 {
			found = true
			break
		}
	}
	if !found {
		t.Error("should find composite index with full prefix length 2")
	}
}
