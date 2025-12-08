// pkg/vdbe/aggregate.go
// Package vdbe aggregate functions for SQL aggregation operations.
package vdbe

import (
	"tur/pkg/types"
)

// AggregateFunc defines the interface for SQL aggregate functions.
// Aggregates process multiple input values and produce a single result.
type AggregateFunc interface {
	// Init initializes/resets the aggregate state
	Init()

	// Step processes one input value
	Step(value types.Value)

	// Finalize returns the final aggregate result
	Finalize() types.Value
}

// CountAggregate implements COUNT(column) - counts non-null values
type CountAggregate struct {
	count int64
}

// NewCountAggregate creates a new COUNT aggregate
func NewCountAggregate() *CountAggregate {
	return &CountAggregate{}
}

// Init resets the count to zero
func (c *CountAggregate) Init() {
	c.count = 0
}

// Step increments count if value is not null
func (c *CountAggregate) Step(value types.Value) {
	if !value.IsNull() {
		c.count++
	}
}

// Finalize returns the count as an integer value
func (c *CountAggregate) Finalize() types.Value {
	return types.NewInt(c.count)
}

// CountStarAggregate implements COUNT(*) - counts all rows including nulls
type CountStarAggregate struct {
	count int64
}

// NewCountStarAggregate creates a new COUNT(*) aggregate
func NewCountStarAggregate() *CountStarAggregate {
	return &CountStarAggregate{}
}

// Init resets the count to zero
func (c *CountStarAggregate) Init() {
	c.count = 0
}

// Step increments count for every row
func (c *CountStarAggregate) Step(value types.Value) {
	c.count++
}

// Finalize returns the count as an integer value
func (c *CountStarAggregate) Finalize() types.Value {
	return types.NewInt(c.count)
}

// SumAggregate implements SUM(column) - sums numeric values
type SumAggregate struct {
	intSum   int64
	floatSum float64
	hasFloat bool
	hasValue bool
}

// NewSumAggregate creates a new SUM aggregate
func NewSumAggregate() *SumAggregate {
	return &SumAggregate{}
}

// Init resets the sum state
func (s *SumAggregate) Init() {
	s.intSum = 0
	s.floatSum = 0
	s.hasFloat = false
	s.hasValue = false
}

// Step adds a value to the sum, ignoring nulls
func (s *SumAggregate) Step(value types.Value) {
	if value.IsNull() {
		return
	}

	s.hasValue = true

	switch value.Type() {
	case types.TypeInt:
		if s.hasFloat {
			s.floatSum += float64(value.Int())
		} else {
			s.intSum += value.Int()
		}
	case types.TypeFloat:
		if !s.hasFloat {
			// Convert accumulated int sum to float
			s.floatSum = float64(s.intSum)
			s.hasFloat = true
		}
		s.floatSum += value.Float()
	}
}

// Finalize returns the sum as int or float, or NULL if no values
func (s *SumAggregate) Finalize() types.Value {
	if !s.hasValue {
		return types.NewNull()
	}
	if s.hasFloat {
		return types.NewFloat(s.floatSum)
	}
	return types.NewInt(s.intSum)
}

// AvgAggregate implements AVG(column) - computes average of numeric values
type AvgAggregate struct {
	sum   float64
	count int64
}

// NewAvgAggregate creates a new AVG aggregate
func NewAvgAggregate() *AvgAggregate {
	return &AvgAggregate{}
}

// Init resets the avg state
func (a *AvgAggregate) Init() {
	a.sum = 0
	a.count = 0
}

// Step adds a value to the average calculation, ignoring nulls
func (a *AvgAggregate) Step(value types.Value) {
	if value.IsNull() {
		return
	}

	a.count++

	switch value.Type() {
	case types.TypeInt:
		a.sum += float64(value.Int())
	case types.TypeFloat:
		a.sum += value.Float()
	}
}

// Finalize returns the average as float, or NULL if no values
func (a *AvgAggregate) Finalize() types.Value {
	if a.count == 0 {
		return types.NewNull()
	}
	return types.NewFloat(a.sum / float64(a.count))
}
