// pkg/sql/executor/statistics_test.go
package executor

import (
	"fmt"
	"testing"
	"time"

	"tur/pkg/schema"
	"tur/pkg/types"
)

func TestTableSampler_SmallTable(t *testing.T) {
	// Create a small table with 100 rows - should sample all rows
	rows := make([][]types.Value, 100)
	for i := 0; i < 100; i++ {
		rows[i] = []types.Value{types.NewInt(int64(i)), types.NewText("name")}
	}

	sampler := NewTableSampler(1000) // 1000 row sample size
	samples := sampler.Sample(rows)

	// Small table should return all rows
	if len(samples) != 100 {
		t.Errorf("expected 100 samples for small table, got %d", len(samples))
	}
}

func TestTableSampler_LargeTable(t *testing.T) {
	// Create a large table with 10000 rows
	rows := make([][]types.Value, 10000)
	for i := 0; i < 10000; i++ {
		rows[i] = []types.Value{types.NewInt(int64(i))}
	}

	sampler := NewTableSampler(1000) // 1000 row sample size
	samples := sampler.Sample(rows)

	// Should return approximately the sample size
	if len(samples) > 1500 || len(samples) < 500 {
		t.Errorf("expected approximately 1000 samples, got %d", len(samples))
	}
}

func TestTableSampler_EmptyTable(t *testing.T) {
	rows := make([][]types.Value, 0)

	sampler := NewTableSampler(1000)
	samples := sampler.Sample(rows)

	if len(samples) != 0 {
		t.Errorf("expected 0 samples for empty table, got %d", len(samples))
	}
}

func TestCollectColumnStatistics_Integer(t *testing.T) {
	// Create sample data with known distribution
	samples := [][]types.Value{
		{types.NewInt(1)},
		{types.NewInt(2)},
		{types.NewInt(3)},
		{types.NewInt(2)}, // duplicate
		{types.NewInt(4)},
		{types.NewNull()}, // null value
	}

	cols := []schema.ColumnDef{
		{Name: "id", Type: types.TypeInt},
	}

	stats := CollectColumnStatistics(samples, cols, 6)

	if len(stats) != 1 {
		t.Fatalf("expected 1 column stats, got %d", len(stats))
	}

	idStats := stats["id"]
	if idStats == nil {
		t.Fatal("expected stats for 'id' column")
	}

	if idStats.DistinctCount != 4 {
		t.Errorf("expected 4 distinct values, got %d", idStats.DistinctCount)
	}

	if idStats.NullCount != 1 {
		t.Errorf("expected 1 null value, got %d", idStats.NullCount)
	}

	// Check min/max
	if idStats.MinValue.Int() != 1 {
		t.Errorf("expected min value 1, got %d", idStats.MinValue.Int())
	}
	if idStats.MaxValue.Int() != 4 {
		t.Errorf("expected max value 4, got %d", idStats.MaxValue.Int())
	}
}

func TestCollectColumnStatistics_Text(t *testing.T) {
	samples := [][]types.Value{
		{types.NewText("alice")},
		{types.NewText("bob")},
		{types.NewText("charlie")},
		{types.NewText("alice")}, // duplicate
		{types.NewNull()},
	}

	cols := []schema.ColumnDef{
		{Name: "name", Type: types.TypeText},
	}

	stats := CollectColumnStatistics(samples, cols, 5)

	nameStats := stats["name"]
	if nameStats == nil {
		t.Fatal("expected stats for 'name' column")
	}

	if nameStats.DistinctCount != 3 {
		t.Errorf("expected 3 distinct values, got %d", nameStats.DistinctCount)
	}

	if nameStats.NullCount != 1 {
		t.Errorf("expected 1 null value, got %d", nameStats.NullCount)
	}

	// Average width check (alice=5, bob=3, charlie=7, average with alice again)
	if nameStats.AvgWidth < 3 || nameStats.AvgWidth > 7 {
		t.Errorf("expected average width between 3 and 7, got %d", nameStats.AvgWidth)
	}
}

func TestCollectColumnStatistics_MultipleColumns(t *testing.T) {
	samples := [][]types.Value{
		{types.NewInt(1), types.NewText("a")},
		{types.NewInt(2), types.NewText("b")},
		{types.NewInt(3), types.NewText("a")},
	}

	cols := []schema.ColumnDef{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeText},
	}

	stats := CollectColumnStatistics(samples, cols, 3)

	if len(stats) != 2 {
		t.Fatalf("expected 2 column stats, got %d", len(stats))
	}

	if stats["id"] == nil {
		t.Error("expected stats for 'id' column")
	}
	if stats["name"] == nil {
		t.Error("expected stats for 'name' column")
	}

	if stats["id"].DistinctCount != 3 {
		t.Errorf("expected 3 distinct id values, got %d", stats["id"].DistinctCount)
	}
	if stats["name"].DistinctCount != 2 {
		t.Errorf("expected 2 distinct name values, got %d", stats["name"].DistinctCount)
	}
}

func TestBuildHistogram_Integer(t *testing.T) {
	// Create values with known distribution
	values := []types.Value{
		types.NewInt(1),
		types.NewInt(5),
		types.NewInt(10),
		types.NewInt(15),
		types.NewInt(20),
		types.NewInt(25),
		types.NewInt(30),
		types.NewInt(35),
		types.NewInt(40),
		types.NewInt(45),
	}

	histogram := BuildHistogram(values, 4) // 4 buckets

	if len(histogram) != 4 {
		t.Fatalf("expected 4 buckets, got %d", len(histogram))
	}

	// Check that buckets cover the entire range
	totalRows := int64(0)
	for _, bucket := range histogram {
		totalRows += bucket.RowCount
	}
	if totalRows != 10 {
		t.Errorf("expected total of 10 rows across buckets, got %d", totalRows)
	}

	// First bucket should have lower bound equal to min value
	if histogram[0].LowerBound.Int() != 1 {
		t.Errorf("expected first bucket lower bound 1, got %d", histogram[0].LowerBound.Int())
	}

	// Last bucket should have upper bound equal to max value
	if histogram[len(histogram)-1].UpperBound.Int() != 45 {
		t.Errorf("expected last bucket upper bound 45, got %d", histogram[len(histogram)-1].UpperBound.Int())
	}
}

func TestBuildHistogram_Text(t *testing.T) {
	values := []types.Value{
		types.NewText("alice"),
		types.NewText("bob"),
		types.NewText("charlie"),
		types.NewText("david"),
		types.NewText("eve"),
		types.NewText("frank"),
	}

	histogram := BuildHistogram(values, 3) // 3 buckets

	if len(histogram) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(histogram))
	}

	// Check total row count
	totalRows := int64(0)
	for _, bucket := range histogram {
		totalRows += bucket.RowCount
	}
	if totalRows != 6 {
		t.Errorf("expected total of 6 rows across buckets, got %d", totalRows)
	}
}

func TestBuildHistogram_Empty(t *testing.T) {
	values := []types.Value{}

	histogram := BuildHistogram(values, 4)

	if len(histogram) != 0 {
		t.Errorf("expected 0 buckets for empty values, got %d", len(histogram))
	}
}

func TestBuildHistogram_SingleValue(t *testing.T) {
	values := []types.Value{
		types.NewInt(42),
	}

	histogram := BuildHistogram(values, 4)

	if len(histogram) != 1 {
		t.Fatalf("expected 1 bucket for single value, got %d", len(histogram))
	}

	if histogram[0].RowCount != 1 {
		t.Errorf("expected bucket row count 1, got %d", histogram[0].RowCount)
	}
}

func TestBuildHistogram_FewerValuesThanBuckets(t *testing.T) {
	values := []types.Value{
		types.NewInt(10),
		types.NewInt(20),
	}

	histogram := BuildHistogram(values, 10) // More buckets than values

	if len(histogram) != 2 {
		t.Fatalf("expected 2 buckets (one per value), got %d", len(histogram))
	}
}

func TestCollectColumnStatistics_WithHistogram(t *testing.T) {
	// Create sample data
	samples := [][]types.Value{
		{types.NewInt(1)},
		{types.NewInt(10)},
		{types.NewInt(20)},
		{types.NewInt(30)},
		{types.NewInt(40)},
		{types.NewInt(50)},
		{types.NewInt(60)},
		{types.NewInt(70)},
		{types.NewInt(80)},
		{types.NewInt(90)},
	}

	cols := []schema.ColumnDef{
		{Name: "id", Type: types.TypeInt},
	}

	stats := CollectColumnStatisticsWithHistogram(samples, cols, 10, 4)

	if len(stats) != 1 {
		t.Fatalf("expected 1 column stats, got %d", len(stats))
	}

	idStats := stats["id"]
	if idStats == nil {
		t.Fatal("expected stats for 'id' column")
	}

	// Should have histogram buckets
	if len(idStats.Histogram) == 0 {
		t.Error("expected histogram to be populated")
	}

	if len(idStats.Histogram) != 4 {
		t.Errorf("expected 4 histogram buckets, got %d", len(idStats.Histogram))
	}
}

func TestExecutor_Analyze_SpecificTable(t *testing.T) {
	e, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table and insert data
	_, err := e.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert multiple rows
	for i := 0; i < 100; i++ {
		_, err = e.Execute(fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d')", i, i))
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	// Run ANALYZE
	result, err := e.Execute("ANALYZE users")
	if err != nil {
		t.Fatalf("ANALYZE failed: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("expected 1 table analyzed, got %d", result.RowsAffected)
	}

	// Check that statistics were stored
	stats := e.GetCatalog().GetTableStatistics("users")
	if stats == nil {
		t.Fatal("expected statistics to be stored")
	}

	if stats.RowCount != 100 {
		t.Errorf("expected row count 100, got %d", stats.RowCount)
	}

	if stats.ColumnStats == nil {
		t.Fatal("expected column stats to be populated")
	}

	idStats := stats.ColumnStats["id"]
	if idStats == nil {
		t.Fatal("expected stats for 'id' column")
	}

	if idStats.DistinctCount < 1 {
		t.Errorf("expected positive distinct count, got %d", idStats.DistinctCount)
	}
}

func TestExecutor_Analyze_AllTables(t *testing.T) {
	e, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create two tables
	_, err := e.Execute("CREATE TABLE users (id INT)")
	if err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	_, err = e.Execute("CREATE TABLE products (id INT)")
	if err != nil {
		t.Fatalf("failed to create products table: %v", err)
	}

	// Insert data
	for i := 0; i < 10; i++ {
		_, _ = e.Execute(fmt.Sprintf("INSERT INTO users VALUES (%d)", i))
		_, _ = e.Execute(fmt.Sprintf("INSERT INTO products VALUES (%d)", i*10))
	}

	// Run ANALYZE without table name (analyze all)
	result, err := e.Execute("ANALYZE")
	if err != nil {
		t.Fatalf("ANALYZE failed: %v", err)
	}

	if result.RowsAffected != 2 {
		t.Errorf("expected 2 tables analyzed, got %d", result.RowsAffected)
	}

	// Both tables should have statistics
	usersStats := e.GetCatalog().GetTableStatistics("users")
	if usersStats == nil {
		t.Error("expected statistics for users table")
	}

	productsStats := e.GetCatalog().GetTableStatistics("products")
	if productsStats == nil {
		t.Error("expected statistics for products table")
	}
}

func TestExecutor_Analyze_TableNotFound(t *testing.T) {
	e, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := e.Execute("ANALYZE nonexistent")
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

func TestExecutor_Insert_UpdatesRowCount(t *testing.T) {
	e, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table
	_, err := e.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// First run ANALYZE to establish baseline statistics
	_, err = e.Execute("ANALYZE users")
	if err != nil {
		t.Fatalf("failed to analyze table: %v", err)
	}

	stats := e.GetCatalog().GetTableStatistics("users")
	if stats == nil || stats.RowCount != 0 {
		t.Fatalf("expected 0 rows initially, got %v", stats)
	}

	// Insert some rows
	for i := 0; i < 10; i++ {
		_, err = e.Execute(fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d')", i, i))
		if err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	// Check that row count was updated incrementally
	stats = e.GetCatalog().GetTableStatistics("users")
	if stats == nil {
		t.Fatal("expected statistics after inserts")
	}

	if stats.RowCount != 10 {
		t.Errorf("expected row count 10 after inserts, got %d", stats.RowCount)
	}
}

func TestExecutor_BulkInsert_UpdatesRowCount(t *testing.T) {
	e, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table
	_, err := e.Execute("CREATE TABLE products (id INT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Analyze first to create initial statistics
	_, err = e.Execute("ANALYZE products")
	if err != nil {
		t.Fatalf("failed to analyze: %v", err)
	}

	// Insert 100 rows
	for i := 0; i < 100; i++ {
		_, _ = e.Execute(fmt.Sprintf("INSERT INTO products VALUES (%d)", i))
	}

	// Check statistics
	stats := e.GetCatalog().GetTableStatistics("products")
	if stats == nil {
		t.Fatal("expected statistics")
	}

	if stats.RowCount != 100 {
		t.Errorf("expected row count 100, got %d", stats.RowCount)
	}
}

func TestCreateTableStatistics(t *testing.T) {
	samples := [][]types.Value{
		{types.NewInt(1)},
		{types.NewInt(2)},
		{types.NewInt(3)},
	}

	cols := []schema.ColumnDef{
		{Name: "id", Type: types.TypeInt},
	}

	tableStats := CreateTableStatistics("users", samples, cols, 100)

	if tableStats.TableName != "users" {
		t.Errorf("expected table name 'users', got '%s'", tableStats.TableName)
	}

	if tableStats.RowCount != 100 {
		t.Errorf("expected row count 100, got %d", tableStats.RowCount)
	}

	if tableStats.LastAnalyzed.IsZero() {
		t.Error("expected LastAnalyzed to be set")
	}

	// LastAnalyzed should be recent
	if time.Since(tableStats.LastAnalyzed) > time.Second {
		t.Error("LastAnalyzed should be within the last second")
	}

	if tableStats.ColumnStats == nil {
		t.Fatal("expected ColumnStats to be non-nil")
	}

	if tableStats.ColumnStats["id"] == nil {
		t.Error("expected stats for 'id' column")
	}
}
