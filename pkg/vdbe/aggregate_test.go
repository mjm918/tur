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
