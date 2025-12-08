// pkg/schema/statistics.go
package schema

import (
	"time"

	"tur/pkg/types"
)

// TableStatistics holds statistics for a table
type TableStatistics struct {
	TableName    string                       // Name of the table
	RowCount     int64                        // Total number of rows in the table
	LastAnalyzed time.Time                    // When statistics were last collected
	ColumnStats  map[string]*ColumnStatistics // Per-column statistics
}

// ColumnStatistics holds statistics for a single column
type ColumnStatistics struct {
	ColumnName    string            // Name of the column
	DistinctCount int64             // Number of distinct values (not including NULL)
	NullCount     int64             // Number of NULL values
	AvgWidth      int               // Average width in bytes (for TEXT/BLOB columns)
	MinValue      types.Value       // Minimum value (for ordered types)
	MaxValue      types.Value       // Maximum value (for ordered types)
	Histogram     []HistogramBucket // Value distribution histogram
}

// HistogramBucket represents a bucket in a column histogram
// Used for estimating selectivity of range predicates
type HistogramBucket struct {
	LowerBound    types.Value // Lower bound of the bucket (inclusive)
	UpperBound    types.Value // Upper bound of the bucket (inclusive)
	RowCount      int64       // Number of rows in this bucket
	DistinctCount int64       // Number of distinct values in this bucket
}

// EqualitySelectivity returns the estimated selectivity for an equality predicate
// Based on the assumption of uniform distribution among distinct values
func (cs *ColumnStatistics) EqualitySelectivity() float64 {
	if cs.DistinctCount <= 0 {
		return 0.01 // Default selectivity if no statistics
	}
	return 1.0 / float64(cs.DistinctCount)
}

// NullFraction returns the fraction of NULL values in the column
func (cs *ColumnStatistics) NullFraction(totalRows int64) float64 {
	if totalRows <= 0 {
		return 0.0
	}
	return float64(cs.NullCount) / float64(totalRows)
}
