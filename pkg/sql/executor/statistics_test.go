// pkg/sql/executor/statistics_test.go
package executor

import (
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
