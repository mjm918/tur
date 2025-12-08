// pkg/schema/statistics_test.go
package schema

import (
	"testing"
	"time"

	"tur/pkg/types"
)

func TestTableStatistics_Creation(t *testing.T) {
	stats := &TableStatistics{
		TableName:    "users",
		RowCount:     10000,
		LastAnalyzed: time.Now(),
	}

	if stats.TableName != "users" {
		t.Errorf("expected TableName 'users', got '%s'", stats.TableName)
	}
	if stats.RowCount != 10000 {
		t.Errorf("expected RowCount 10000, got %d", stats.RowCount)
	}
	if stats.LastAnalyzed.IsZero() {
		t.Error("expected LastAnalyzed to be set")
	}
}

func TestColumnStatistics_Creation(t *testing.T) {
	minVal := types.NewInt(1)
	maxVal := types.NewInt(100)

	stats := &ColumnStatistics{
		ColumnName:     "id",
		DistinctCount:  100,
		NullCount:      0,
		AvgWidth:       8,
		MinValue:       minVal,
		MaxValue:       maxVal,
	}

	if stats.ColumnName != "id" {
		t.Errorf("expected ColumnName 'id', got '%s'", stats.ColumnName)
	}
	if stats.DistinctCount != 100 {
		t.Errorf("expected DistinctCount 100, got %d", stats.DistinctCount)
	}
	if stats.NullCount != 0 {
		t.Errorf("expected NullCount 0, got %d", stats.NullCount)
	}
	if stats.AvgWidth != 8 {
		t.Errorf("expected AvgWidth 8, got %d", stats.AvgWidth)
	}
}

func TestTableStatistics_AddColumnStats(t *testing.T) {
	tableStats := &TableStatistics{
		TableName:     "users",
		RowCount:      1000,
		ColumnStats:   make(map[string]*ColumnStatistics),
	}

	colStats := &ColumnStatistics{
		ColumnName:    "email",
		DistinctCount: 950,
		NullCount:     50,
	}

	tableStats.ColumnStats["email"] = colStats

	retrieved := tableStats.ColumnStats["email"]
	if retrieved == nil {
		t.Fatal("expected column stats for 'email', got nil")
	}
	if retrieved.DistinctCount != 950 {
		t.Errorf("expected DistinctCount 950, got %d", retrieved.DistinctCount)
	}
}

func TestColumnStatistics_Selectivity(t *testing.T) {
	// Test selectivity estimation based on distinct count
	stats := &ColumnStatistics{
		ColumnName:    "status",
		DistinctCount: 5,
	}

	// Selectivity for equality should be 1/distinct_count
	selectivity := stats.EqualitySelectivity()
	expected := 1.0 / 5.0 // 0.2
	if selectivity != expected {
		t.Errorf("expected selectivity %f, got %f", expected, selectivity)
	}
}

func TestColumnStatistics_NullFraction(t *testing.T) {
	stats := &ColumnStatistics{
		ColumnName:    "middle_name",
		NullCount:     200,
	}

	// Test null fraction calculation
	nullFrac := stats.NullFraction(1000) // 1000 total rows
	expected := 0.2                       // 200/1000
	if nullFrac != expected {
		t.Errorf("expected null fraction %f, got %f", expected, nullFrac)
	}
}

func TestHistogramBucket_Creation(t *testing.T) {
	bucket := &HistogramBucket{
		LowerBound:    types.NewInt(0),
		UpperBound:    types.NewInt(100),
		RowCount:      500,
		DistinctCount: 100,
	}

	if bucket.RowCount != 500 {
		t.Errorf("expected RowCount 500, got %d", bucket.RowCount)
	}
	if bucket.DistinctCount != 100 {
		t.Errorf("expected DistinctCount 100, got %d", bucket.DistinctCount)
	}
}

func TestColumnStatistics_WithHistogram(t *testing.T) {
	stats := &ColumnStatistics{
		ColumnName:    "age",
		DistinctCount: 80,
		Histogram: []HistogramBucket{
			{LowerBound: types.NewInt(0), UpperBound: types.NewInt(20), RowCount: 100, DistinctCount: 20},
			{LowerBound: types.NewInt(21), UpperBound: types.NewInt(40), RowCount: 300, DistinctCount: 20},
			{LowerBound: types.NewInt(41), UpperBound: types.NewInt(60), RowCount: 400, DistinctCount: 20},
			{LowerBound: types.NewInt(61), UpperBound: types.NewInt(100), RowCount: 200, DistinctCount: 20},
		},
	}

	if len(stats.Histogram) != 4 {
		t.Errorf("expected 4 histogram buckets, got %d", len(stats.Histogram))
	}

	// Verify total row count from buckets
	totalRows := int64(0)
	for _, bucket := range stats.Histogram {
		totalRows += bucket.RowCount
	}
	if totalRows != 1000 {
		t.Errorf("expected total rows 1000 from histogram, got %d", totalRows)
	}
}

func TestCatalog_GetTableStatistics(t *testing.T) {
	catalog := NewCatalog()

	// Create a table first
	table := &TableDef{
		Name: "products",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "name", Type: types.TypeText},
		},
	}
	err := catalog.CreateTable(table)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Initially should have no statistics
	stats := catalog.GetTableStatistics("products")
	if stats != nil {
		t.Error("expected no statistics initially")
	}

	// Set statistics
	tableStats := &TableStatistics{
		TableName:    "products",
		RowCount:     5000,
		LastAnalyzed: time.Now(),
		ColumnStats:  make(map[string]*ColumnStatistics),
	}
	tableStats.ColumnStats["id"] = &ColumnStatistics{
		ColumnName:    "id",
		DistinctCount: 5000,
	}

	err = catalog.UpdateTableStatistics("products", tableStats)
	if err != nil {
		t.Fatalf("failed to update statistics: %v", err)
	}

	// Retrieve statistics
	retrieved := catalog.GetTableStatistics("products")
	if retrieved == nil {
		t.Fatal("expected statistics after update")
	}
	if retrieved.RowCount != 5000 {
		t.Errorf("expected RowCount 5000, got %d", retrieved.RowCount)
	}
	if retrieved.ColumnStats["id"] == nil {
		t.Error("expected column stats for 'id'")
	}
}

func TestCatalog_UpdateTableStatistics_TableNotFound(t *testing.T) {
	catalog := NewCatalog()

	stats := &TableStatistics{
		TableName: "nonexistent",
		RowCount:  100,
	}

	err := catalog.UpdateTableStatistics("nonexistent", stats)
	if err != ErrTableNotFound {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}
}

func TestCatalog_DropTableClearsStatistics(t *testing.T) {
	catalog := NewCatalog()

	// Create table and add statistics
	table := &TableDef{
		Name: "temp_table",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
		},
	}
	_ = catalog.CreateTable(table)
	_ = catalog.UpdateTableStatistics("temp_table", &TableStatistics{
		TableName: "temp_table",
		RowCount:  100,
	})

	// Drop table
	err := catalog.DropTable("temp_table")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	// Statistics should be gone
	stats := catalog.GetTableStatistics("temp_table")
	if stats != nil {
		t.Error("expected statistics to be cleared after table drop")
	}
}
