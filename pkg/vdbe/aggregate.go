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
