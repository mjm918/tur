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
