// pkg/sql/executor/result_pool.go
package executor

import (
	"sync"

	"tur/pkg/types"
)

// resultPool provides pooled Result objects to reduce allocations.
// Results are pre-allocated with capacity for common query sizes.
var resultPool = sync.Pool{
	New: func() interface{} {
		return &Result{
			Columns: make([]string, 0, 16),
			Rows:    make([][]types.Value, 0, 100),
		}
	},
}

// rowPool provides pooled row slices to reduce allocations during iteration.
var rowPool = sync.Pool{
	New: func() interface{} {
		// Common case: tables with up to 16 columns
		row := make([]types.Value, 0, 16)
		return &row
	},
}

// acquireResult gets a Result from the pool and resets it.
func acquireResult() *Result {
	r := resultPool.Get().(*Result)
	r.Columns = r.Columns[:0]
	r.Rows = r.Rows[:0]
	r.RowsAffected = 0
	return r
}

// releaseResult returns a Result to the pool for reuse.
// The caller must not use the Result after calling this.
func releaseResult(r *Result) {
	if r == nil {
		return
	}
	// Clear references to allow GC of row data
	for i := range r.Rows {
		r.Rows[i] = nil
	}
	r.Rows = r.Rows[:0]
	r.Columns = r.Columns[:0]
	resultPool.Put(r)
}

// acquireRow gets a row slice from the pool.
func acquireRow(capacity int) []types.Value {
	rowPtr := rowPool.Get().(*[]types.Value)
	row := *rowPtr
	if cap(row) < capacity {
		// Pool row is too small, allocate a new one
		row = make([]types.Value, 0, capacity)
	}
	return row[:0]
}

// releaseRow returns a row slice to the pool for reuse.
func releaseRow(row []types.Value) {
	if row == nil || cap(row) < 16 {
		return
	}
	// Clear references
	for i := range row {
		row[i] = types.Value{}
	}
	row = row[:0]
	rowPool.Put(&row)
}

// PooledResult wraps a Result with automatic pool return on Close.
// Use this for streaming or cursor-based result sets.
type PooledResult struct {
	*Result
	released bool
}

// NewPooledResult creates a new pooled result.
func NewPooledResult() *PooledResult {
	return &PooledResult{
		Result:   acquireResult(),
		released: false,
	}
}

// Release returns the result to the pool.
// After calling Release, the PooledResult should not be used.
func (p *PooledResult) Release() {
	if p.released {
		return
	}
	p.released = true
	releaseResult(p.Result)
	p.Result = nil
}

// columnNamePool provides pooled string slices for column names.
var columnNamePool = sync.Pool{
	New: func() interface{} {
		s := make([]string, 0, 16)
		return &s
	},
}

// acquireColumnNames gets a column name slice from the pool.
func acquireColumnNames(capacity int) []string {
	sPtr := columnNamePool.Get().(*[]string)
	s := *sPtr
	if cap(s) < capacity {
		s = make([]string, 0, capacity)
	}
	return s[:0]
}

// releaseColumnNames returns a column name slice to the pool.
func releaseColumnNames(s []string) {
	if s == nil || cap(s) < 16 {
		return
	}
	s = s[:0]
	columnNamePool.Put(&s)
}
