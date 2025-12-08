// pkg/vdbe/aggregate_test.go
package vdbe

import (
	"testing"

	"tur/pkg/types"
)

// Test 1: Aggregate interface exists and can be implemented
func TestAggregateFunc_Interface(t *testing.T) {
	// Verify the interface has Init, Step, and Finalize methods
	var agg AggregateFunc

	// A nil interface should be assignable
	if agg != nil {
		t.Error("nil aggregate interface should be nil")
	}
}

// Test 2: COUNT aggregate counts non-null values
func TestCountAggregate_CountsNonNullValues(t *testing.T) {
	count := NewCountAggregate()

	// Initialize
	count.Init()

	// Step with several values including nulls
	count.Step(types.NewInt(1))
	count.Step(types.NewNull())
	count.Step(types.NewInt(2))
	count.Step(types.NewText("hello"))
	count.Step(types.NewNull())

	// Finalize should return count of non-null values
	result := count.Finalize()

	if result.Type() != types.TypeInt {
		t.Errorf("expected integer result, got %v", result.Type())
	}
	if result.Int() != 3 {
		t.Errorf("expected count of 3, got %d", result.Int())
	}
}

// Test 3: COUNT(*) counts all rows including nulls
func TestCountStarAggregate_CountsAllRows(t *testing.T) {
	count := NewCountStarAggregate()

	count.Init()
	count.Step(types.NewInt(1))
	count.Step(types.NewNull())
	count.Step(types.NewInt(2))
	count.Step(types.NewNull())

	result := count.Finalize()

	if result.Int() != 4 {
		t.Errorf("expected count of 4, got %d", result.Int())
	}
}

// Test 4: COUNT with no values returns 0
func TestCountAggregate_EmptyReturnsZero(t *testing.T) {
	count := NewCountAggregate()
	count.Init()
	result := count.Finalize()

	if result.Int() != 0 {
		t.Errorf("expected count of 0, got %d", result.Int())
	}
}

// ============ SUM Aggregate Tests ============

// Test 5: SUM aggregate sums integer values
func TestSumAggregate_SumsIntegers(t *testing.T) {
	sum := NewSumAggregate()
	sum.Init()

	sum.Step(types.NewInt(10))
	sum.Step(types.NewInt(20))
	sum.Step(types.NewInt(30))

	result := sum.Finalize()

	if result.Type() != types.TypeInt {
		t.Errorf("expected integer result, got %v", result.Type())
	}
	if result.Int() != 60 {
		t.Errorf("expected sum of 60, got %d", result.Int())
	}
}

// Test 6: SUM aggregate sums float values
func TestSumAggregate_SumsFloats(t *testing.T) {
	sum := NewSumAggregate()
	sum.Init()

	sum.Step(types.NewFloat(1.5))
	sum.Step(types.NewFloat(2.5))
	sum.Step(types.NewFloat(3.0))

	result := sum.Finalize()

	if result.Type() != types.TypeFloat {
		t.Errorf("expected float result, got %v", result.Type())
	}
	if result.Float() != 7.0 {
		t.Errorf("expected sum of 7.0, got %f", result.Float())
	}
}

// Test 7: SUM with mixed int and float returns float
func TestSumAggregate_MixedIntFloat(t *testing.T) {
	sum := NewSumAggregate()
	sum.Init()

	sum.Step(types.NewInt(10))
	sum.Step(types.NewFloat(2.5))

	result := sum.Finalize()

	if result.Type() != types.TypeFloat {
		t.Errorf("expected float result for mixed sum, got %v", result.Type())
	}
	if result.Float() != 12.5 {
		t.Errorf("expected sum of 12.5, got %f", result.Float())
	}
}

// Test 8: SUM ignores null values
func TestSumAggregate_IgnoresNulls(t *testing.T) {
	sum := NewSumAggregate()
	sum.Init()

	sum.Step(types.NewInt(10))
	sum.Step(types.NewNull())
	sum.Step(types.NewInt(20))

	result := sum.Finalize()

	if result.Int() != 30 {
		t.Errorf("expected sum of 30, got %d", result.Int())
	}
}

// Test 9: SUM with no values returns NULL
func TestSumAggregate_EmptyReturnsNull(t *testing.T) {
	sum := NewSumAggregate()
	sum.Init()
	result := sum.Finalize()

	if !result.IsNull() {
		t.Errorf("expected NULL for empty SUM, got %v", result.Type())
	}
}

// ============ AVG Aggregate Tests ============

// Test 10: AVG aggregate computes average of integers
func TestAvgAggregate_AveragesIntegers(t *testing.T) {
	avg := NewAvgAggregate()
	avg.Init()

	avg.Step(types.NewInt(10))
	avg.Step(types.NewInt(20))
	avg.Step(types.NewInt(30))

	result := avg.Finalize()

	// AVG always returns float
	if result.Type() != types.TypeFloat {
		t.Errorf("expected float result, got %v", result.Type())
	}
	if result.Float() != 20.0 {
		t.Errorf("expected average of 20.0, got %f", result.Float())
	}
}

// Test 11: AVG aggregate computes average of floats
func TestAvgAggregate_AveragesFloats(t *testing.T) {
	avg := NewAvgAggregate()
	avg.Init()

	avg.Step(types.NewFloat(1.0))
	avg.Step(types.NewFloat(2.0))
	avg.Step(types.NewFloat(3.0))
	avg.Step(types.NewFloat(4.0))

	result := avg.Finalize()

	if result.Float() != 2.5 {
		t.Errorf("expected average of 2.5, got %f", result.Float())
	}
}

// Test 12: AVG ignores null values
func TestAvgAggregate_IgnoresNulls(t *testing.T) {
	avg := NewAvgAggregate()
	avg.Init()

	avg.Step(types.NewInt(10))
	avg.Step(types.NewNull())
	avg.Step(types.NewInt(30))

	result := avg.Finalize()

	// Average of 10, 30 is 20 (null ignored)
	if result.Float() != 20.0 {
		t.Errorf("expected average of 20.0 (ignoring null), got %f", result.Float())
	}
}

// Test 13: AVG with no values returns NULL
func TestAvgAggregate_EmptyReturnsNull(t *testing.T) {
	avg := NewAvgAggregate()
	avg.Init()
	result := avg.Finalize()

	if !result.IsNull() {
		t.Errorf("expected NULL for empty AVG, got %v", result.Type())
	}
}

// ============ MIN Aggregate Tests ============

// Test 14: MIN aggregate finds minimum integer
func TestMinAggregate_FindsMinInteger(t *testing.T) {
	min := NewMinAggregate()
	min.Init()

	min.Step(types.NewInt(30))
	min.Step(types.NewInt(10))
	min.Step(types.NewInt(20))

	result := min.Finalize()

	if result.Type() != types.TypeInt {
		t.Errorf("expected integer result, got %v", result.Type())
	}
	if result.Int() != 10 {
		t.Errorf("expected min of 10, got %d", result.Int())
	}
}

// Test 15: MIN aggregate finds minimum float
func TestMinAggregate_FindsMinFloat(t *testing.T) {
	min := NewMinAggregate()
	min.Init()

	min.Step(types.NewFloat(3.5))
	min.Step(types.NewFloat(1.5))
	min.Step(types.NewFloat(2.5))

	result := min.Finalize()

	if result.Float() != 1.5 {
		t.Errorf("expected min of 1.5, got %f", result.Float())
	}
}

// Test 16: MIN aggregate finds minimum text (lexicographically)
func TestMinAggregate_FindsMinText(t *testing.T) {
	min := NewMinAggregate()
	min.Init()

	min.Step(types.NewText("cherry"))
	min.Step(types.NewText("apple"))
	min.Step(types.NewText("banana"))

	result := min.Finalize()

	if result.Text() != "apple" {
		t.Errorf("expected min of 'apple', got %s", result.Text())
	}
}

// Test 17: MIN ignores null values
func TestMinAggregate_IgnoresNulls(t *testing.T) {
	min := NewMinAggregate()
	min.Init()

	min.Step(types.NewInt(30))
	min.Step(types.NewNull())
	min.Step(types.NewInt(10))

	result := min.Finalize()

	if result.Int() != 10 {
		t.Errorf("expected min of 10, got %d", result.Int())
	}
}

// Test 18: MIN with no values returns NULL
func TestMinAggregate_EmptyReturnsNull(t *testing.T) {
	min := NewMinAggregate()
	min.Init()
	result := min.Finalize()

	if !result.IsNull() {
		t.Errorf("expected NULL for empty MIN, got %v", result.Type())
	}
}

// ============ MAX Aggregate Tests ============

// Test 19: MAX aggregate finds maximum integer
func TestMaxAggregate_FindsMaxInteger(t *testing.T) {
	max := NewMaxAggregate()
	max.Init()

	max.Step(types.NewInt(10))
	max.Step(types.NewInt(30))
	max.Step(types.NewInt(20))

	result := max.Finalize()

	if result.Type() != types.TypeInt {
		t.Errorf("expected integer result, got %v", result.Type())
	}
	if result.Int() != 30 {
		t.Errorf("expected max of 30, got %d", result.Int())
	}
}

// Test 20: MAX aggregate finds maximum float
func TestMaxAggregate_FindsMaxFloat(t *testing.T) {
	max := NewMaxAggregate()
	max.Init()

	max.Step(types.NewFloat(1.5))
	max.Step(types.NewFloat(3.5))
	max.Step(types.NewFloat(2.5))

	result := max.Finalize()

	if result.Float() != 3.5 {
		t.Errorf("expected max of 3.5, got %f", result.Float())
	}
}

// Test 21: MAX aggregate finds maximum text (lexicographically)
func TestMaxAggregate_FindsMaxText(t *testing.T) {
	max := NewMaxAggregate()
	max.Init()

	max.Step(types.NewText("apple"))
	max.Step(types.NewText("cherry"))
	max.Step(types.NewText("banana"))

	result := max.Finalize()

	if result.Text() != "cherry" {
		t.Errorf("expected max of 'cherry', got %s", result.Text())
	}
}

// Test 22: MAX ignores null values
func TestMaxAggregate_IgnoresNulls(t *testing.T) {
	max := NewMaxAggregate()
	max.Init()

	max.Step(types.NewInt(10))
	max.Step(types.NewNull())
	max.Step(types.NewInt(30))

	result := max.Finalize()

	if result.Int() != 30 {
		t.Errorf("expected max of 30, got %d", result.Int())
	}
}

// Test 23: MAX with no values returns NULL
func TestMaxAggregate_EmptyReturnsNull(t *testing.T) {
	max := NewMaxAggregate()
	max.Init()
	result := max.Finalize()

	if !result.IsNull() {
		t.Errorf("expected NULL for empty MAX, got %v", result.Type())
	}
}
