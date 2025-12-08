// pkg/sql/executor/statistics.go
package executor

import (
	"math/rand"
	"time"

	"tur/pkg/schema"
	"tur/pkg/types"
)

// TableSampler implements reservoir sampling for large tables
type TableSampler struct {
	sampleSize int
	rng        *rand.Rand
}

// NewTableSampler creates a new table sampler with the given sample size
func NewTableSampler(sampleSize int) *TableSampler {
	return &TableSampler{
		sampleSize: sampleSize,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Sample returns a random sample of rows using reservoir sampling
// If the number of rows is less than or equal to sampleSize, returns all rows
func (s *TableSampler) Sample(rows [][]types.Value) [][]types.Value {
	n := len(rows)
	if n == 0 {
		return rows
	}

	if n <= s.sampleSize {
		// Return all rows if table is small
		return rows
	}

	// Reservoir sampling algorithm
	// Initialize reservoir with first sampleSize elements
	reservoir := make([][]types.Value, s.sampleSize)
	copy(reservoir, rows[:s.sampleSize])

	// Process remaining elements
	for i := s.sampleSize; i < n; i++ {
		// Generate random index j in [0, i]
		j := s.rng.Intn(i + 1)
		// If j is within reservoir size, replace element at j
		if j < s.sampleSize {
			reservoir[j] = rows[i]
		}
	}

	return reservoir
}

// CollectColumnStatistics collects statistics for each column from sample data
func CollectColumnStatistics(samples [][]types.Value, cols []schema.ColumnDef, totalRows int64) map[string]*schema.ColumnStatistics {
	result := make(map[string]*schema.ColumnStatistics)

	for colIdx, col := range cols {
		stats := &schema.ColumnStatistics{
			ColumnName: col.Name,
		}

		// Track distinct values and nulls
		distinctValues := make(map[string]struct{})
		var nullCount int64
		var totalWidth int64
		var nonNullCount int64

		var minVal, maxVal types.Value
		minValSet := false

		for _, row := range samples {
			if colIdx >= len(row) {
				continue
			}

			val := row[colIdx]

			if val.IsNull() {
				nullCount++
				continue
			}

			nonNullCount++

			// Track distinct values using string representation
			key := valueToString(val)
			distinctValues[key] = struct{}{}

			// Track width for text/blob types
			switch val.Type() {
			case types.TypeText:
				totalWidth += int64(len(val.Text()))
			case types.TypeBlob:
				totalWidth += int64(len(val.Blob()))
			}

			// Track min/max for ordered types
			if !minValSet {
				minVal = val
				maxVal = val
				minValSet = true
			} else {
				if compareValues(val, minVal) < 0 {
					minVal = val
				}
				if compareValues(val, maxVal) > 0 {
					maxVal = val
				}
			}
		}

		stats.DistinctCount = int64(len(distinctValues))
		stats.NullCount = nullCount

		if nonNullCount > 0 {
			stats.AvgWidth = int(totalWidth / nonNullCount)
		}

		if minValSet {
			stats.MinValue = minVal
			stats.MaxValue = maxVal
		}

		result[col.Name] = stats
	}

	return result
}

// CreateTableStatistics creates a complete TableStatistics object
func CreateTableStatistics(tableName string, samples [][]types.Value, cols []schema.ColumnDef, totalRows int64) *schema.TableStatistics {
	columnStats := CollectColumnStatistics(samples, cols, totalRows)

	return &schema.TableStatistics{
		TableName:    tableName,
		RowCount:     totalRows,
		LastAnalyzed: time.Now(),
		ColumnStats:  columnStats,
	}
}

// valueToString converts a value to a string for distinct counting
func valueToString(val types.Value) string {
	switch val.Type() {
	case types.TypeInt:
		return string(rune(val.Int())) // Simple encoding for ints
	case types.TypeFloat:
		return string(rune(int64(val.Float() * 1000000))) // Approximate
	case types.TypeText:
		return val.Text()
	case types.TypeBlob:
		return string(val.Blob())
	default:
		return ""
	}
}

// compareValues compares two values for ordering
// Returns -1 if a < b, 0 if a == b, 1 if a > b
func compareValues(a, b types.Value) int {
	if a.Type() != b.Type() {
		return 0 // Cannot compare different types
	}

	switch a.Type() {
	case types.TypeInt:
		if a.Int() < b.Int() {
			return -1
		} else if a.Int() > b.Int() {
			return 1
		}
		return 0
	case types.TypeFloat:
		if a.Float() < b.Float() {
			return -1
		} else if a.Float() > b.Float() {
			return 1
		}
		return 0
	case types.TypeText:
		if a.Text() < b.Text() {
			return -1
		} else if a.Text() > b.Text() {
			return 1
		}
		return 0
	default:
		return 0
	}
}
