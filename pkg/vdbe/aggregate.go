// pkg/vdbe/aggregate.go
// Package vdbe aggregate functions for SQL aggregation operations.
package vdbe

import (
	"strings"

	"tur/pkg/types"
)

// GetAggregate returns a new instance of an aggregate function by name.
// Returns nil if the aggregate name is not recognized.
// Names are case-insensitive.
func GetAggregate(name string) AggregateFunc {
	switch strings.ToUpper(name) {
	case "COUNT":
		return NewCountAggregate()
	case "COUNT*":
		return NewCountStarAggregate()
	case "SUM":
		return NewSumAggregate()
	case "AVG":
		return NewAvgAggregate()
	case "MIN":
		return NewMinAggregate()
	case "MAX":
		return NewMaxAggregate()
	default:
		return nil
	}
}

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

// MinAggregate implements MIN(column) - finds minimum value
type MinAggregate struct {
	min      types.Value
	hasValue bool
}

// NewMinAggregate creates a new MIN aggregate
func NewMinAggregate() *MinAggregate {
	return &MinAggregate{}
}

// Init resets the min state
func (m *MinAggregate) Init() {
	m.min = types.NewNull()
	m.hasValue = false
}

// Step updates min if value is smaller, ignoring nulls
func (m *MinAggregate) Step(value types.Value) {
	if value.IsNull() {
		return
	}

	if !m.hasValue {
		m.min = value
		m.hasValue = true
		return
	}

	if compareValues(value, m.min) < 0 {
		m.min = value
	}
}

// Finalize returns the minimum value, or NULL if no values
func (m *MinAggregate) Finalize() types.Value {
	if !m.hasValue {
		return types.NewNull()
	}
	return m.min
}

// MaxAggregate implements MAX(column) - finds maximum value
type MaxAggregate struct {
	max      types.Value
	hasValue bool
}

// NewMaxAggregate creates a new MAX aggregate
func NewMaxAggregate() *MaxAggregate {
	return &MaxAggregate{}
}

// Init resets the max state
func (m *MaxAggregate) Init() {
	m.max = types.NewNull()
	m.hasValue = false
}

// Step updates max if value is larger, ignoring nulls
func (m *MaxAggregate) Step(value types.Value) {
	if value.IsNull() {
		return
	}

	if !m.hasValue {
		m.max = value
		m.hasValue = true
		return
	}

	if compareValues(value, m.max) > 0 {
		m.max = value
	}
}

// Finalize returns the maximum value, or NULL if no values
func (m *MaxAggregate) Finalize() types.Value {
	if !m.hasValue {
		return types.NewNull()
	}
	return m.max
}

// compareValues compares two values, returns -1, 0, or 1
// Follows SQL comparison rules: NULL < number < text < blob
func compareValues(a, b types.Value) int {
	// Handle NULL
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1
	}
	if b.IsNull() {
		return 1
	}

	// Same type comparisons
	if a.Type() == b.Type() {
		switch a.Type() {
		case types.TypeInt:
			ai, bi := a.Int(), b.Int()
			if ai < bi {
				return -1
			}
			if ai > bi {
				return 1
			}
			return 0
		case types.TypeFloat:
			af, bf := a.Float(), b.Float()
			if af < bf {
				return -1
			}
			if af > bf {
				return 1
			}
			return 0
		case types.TypeText:
			at, bt := a.Text(), b.Text()
			if at < bt {
				return -1
			}
			if at > bt {
				return 1
			}
			return 0
		}
	}

	// Mixed numeric types
	if (a.Type() == types.TypeInt || a.Type() == types.TypeFloat) &&
		(b.Type() == types.TypeInt || b.Type() == types.TypeFloat) {
		var af, bf float64
		if a.Type() == types.TypeInt {
			af = float64(a.Int())
		} else {
			af = a.Float()
		}
		if b.Type() == types.TypeInt {
			bf = float64(b.Int())
		} else {
			bf = b.Float()
		}
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	// Default: compare by type order
	if a.Type() < b.Type() {
		return -1
	}
	return 1
}
