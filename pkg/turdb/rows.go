// pkg/turdb/rows.go
package turdb

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"tur/pkg/record"
	"tur/pkg/types"
)

var (
	// ErrNoRows is returned when no rows are available
	ErrNoRows = errors.New("no rows in result set")

	// ErrRowsClosed is returned when operations are attempted on a closed result set
	ErrRowsClosed = errors.New("rows are closed")

	// ErrScanBeforeNext is returned when Scan is called before Next
	ErrScanBeforeNext = errors.New("Scan called without calling Next")
)

// rowsPool provides pooled Rows objects to reduce allocations.
var rowsPool = sync.Pool{
	New: func() interface{} {
		return &Rows{}
	},
}

// Rows represents a result set from a query.
// It provides methods to iterate over and access query results.
// Rows is safe for concurrent use.
type Rows struct {
	mu        sync.Mutex
	columns   []string
	rows      [][]types.Value
	singleRow []types.Value // For single-row results (avoids [][]types.Value wrapper)
	index     int           // current row index (-1 means before first row)
	closed    bool          // whether the result set has been closed

	// Direct references for pool cleanup (avoids closure allocation)
	pooledView   *record.RecordView // Pooled RecordView to release on Close
	pooledValues []types.Value      // Pooled values slice to release on Close
}

// NewRows creates a new Rows result set from columns and row data.
// Uses pooling to reduce allocations.
func NewRows(columns []string, rows [][]types.Value) *Rows {
	r := rowsPool.Get().(*Rows)
	r.columns = columns
	r.rows = rows
	r.singleRow = nil
	r.index = -1
	r.closed = false
	r.pooledView = nil
	r.pooledValues = nil
	return r
}

// NewSingleRowRows creates a Rows for a single row without allocating a [][]types.Value wrapper.
// This is an optimization for fast-path queries that return exactly one row.
func NewSingleRowRows(columns []string, row []types.Value) *Rows {
	r := rowsPool.Get().(*Rows)
	r.columns = columns
	r.rows = nil
	r.singleRow = row
	r.index = -1
	r.closed = false
	r.pooledView = nil
	r.pooledValues = nil
	return r
}

// NewSingleRowRowsPooled creates a Rows for a single row with pooled resources.
// The view and values are released back to their pools when Close() is called.
// This avoids closure allocations compared to callback-based cleanup.
func NewSingleRowRowsPooled(columns []string, row []types.Value, view *record.RecordView, values []types.Value) *Rows {
	r := rowsPool.Get().(*Rows)
	r.columns = columns
	r.rows = nil
	r.singleRow = row
	r.index = -1
	r.closed = false
	r.pooledView = view
	r.pooledValues = values
	return r
}

// Columns returns the column names of the result set.
func (r *Rows) Columns() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]string, len(r.columns))
	copy(result, r.columns)
	return result
}

// Next advances the result set to the next row.
// It returns true if there is a next row, false if there are no more rows
// or the result set has been closed.
// Every call to Scan must be preceded by a successful call to Next.
func (r *Rows) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return false
	}

	r.index++

	// Handle single-row optimization
	if r.singleRow != nil {
		return r.index == 0
	}
	return r.index < len(r.rows)
}

// Scan copies the columns in the current row into the values pointed at by dest.
// The number of values in dest must match the number of columns in the result set.
func (r *Rows) Scan(dest ...interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRowsClosed
	}

	if r.index < 0 {
		return ErrScanBeforeNext
	}

	// Get the current row (handle single-row optimization)
	var row []types.Value
	if r.singleRow != nil {
		if r.index != 0 {
			return ErrNoRows
		}
		row = r.singleRow
	} else {
		if r.index >= len(r.rows) {
			return ErrNoRows
		}
		row = r.rows[r.index]
	}

	if len(dest) != len(row) {
		return fmt.Errorf("scan: expected %d destination arguments, got %d", len(row), len(dest))
	}

	for i, val := range row {
		if err := scanValue(val, dest[i]); err != nil {
			return fmt.Errorf("scan column %d: %w", i, err)
		}
	}

	return nil
}

// scanValue copies a types.Value into a destination pointer
func scanValue(src types.Value, dest interface{}) error {
	if dest == nil {
		return errors.New("destination is nil")
	}

	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr {
		return errors.New("destination must be a pointer")
	}

	if rv.IsNil() {
		return errors.New("destination pointer is nil")
	}

	// Handle NULL values
	if src.IsNull() {
		// For pointer types, set to nil
		elem := rv.Elem()
		if elem.Kind() == reflect.Ptr {
			elem.Set(reflect.Zero(elem.Type()))
			return nil
		}
		// For non-pointer types, we could set zero value or return error
		// Following database/sql convention, set zero value
		elem.Set(reflect.Zero(elem.Type()))
		return nil
	}

	elem := rv.Elem()

	switch src.Type() {
	case types.TypeInt32, types.TypeSmallInt, types.TypeBigInt, types.TypeSerial, types.TypeBigSerial:
		return scanInt(src.Int(), elem)
	case types.TypeFloat:
		return scanFloat(src.Float(), elem)
	case types.TypeText:
		return scanText(src.Text(), elem)
	case types.TypeBlob:
		return scanBlob(src.Blob(), elem)
	default:
		return fmt.Errorf("unsupported source type: %v", src.Type())
	}
}

func scanInt(v int64, dest reflect.Value) error {
	switch dest.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dest.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		dest.SetUint(uint64(v))
	case reflect.Float32, reflect.Float64:
		dest.SetFloat(float64(v))
	case reflect.Ptr:
		// Allocate new value and set it
		newVal := reflect.New(dest.Type().Elem())
		if err := scanInt(v, newVal.Elem()); err != nil {
			return err
		}
		dest.Set(newVal)
	default:
		return fmt.Errorf("cannot scan int64 into %v", dest.Kind())
	}
	return nil
}

func scanFloat(v float64, dest reflect.Value) error {
	switch dest.Kind() {
	case reflect.Float32, reflect.Float64:
		dest.SetFloat(v)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dest.SetInt(int64(v))
	case reflect.Ptr:
		newVal := reflect.New(dest.Type().Elem())
		if err := scanFloat(v, newVal.Elem()); err != nil {
			return err
		}
		dest.Set(newVal)
	default:
		return fmt.Errorf("cannot scan float64 into %v", dest.Kind())
	}
	return nil
}

func scanText(v string, dest reflect.Value) error {
	switch dest.Kind() {
	case reflect.String:
		dest.SetString(v)
	case reflect.Ptr:
		newVal := reflect.New(dest.Type().Elem())
		if err := scanText(v, newVal.Elem()); err != nil {
			return err
		}
		dest.Set(newVal)
	default:
		return fmt.Errorf("cannot scan string into %v", dest.Kind())
	}
	return nil
}

func scanBlob(v []byte, dest reflect.Value) error {
	switch dest.Kind() {
	case reflect.Slice:
		if dest.Type().Elem().Kind() == reflect.Uint8 {
			dest.SetBytes(v)
			return nil
		}
		return fmt.Errorf("cannot scan blob into %v", dest.Type())
	case reflect.Ptr:
		newVal := reflect.New(dest.Type().Elem())
		if err := scanBlob(v, newVal.Elem()); err != nil {
			return err
		}
		dest.Set(newVal)
	default:
		return fmt.Errorf("cannot scan blob into %v", dest.Kind())
	}
	return nil
}

// getColumnValue returns the value at the given column index, or nil if out of bounds
func (r *Rows) getColumnValue(i int) (types.Value, bool) {
	// Handle single-row optimization
	var row []types.Value
	if r.singleRow != nil {
		if r.index != 0 {
			return types.Value{}, false
		}
		row = r.singleRow
	} else {
		if r.index < 0 || r.index >= len(r.rows) {
			return types.Value{}, false
		}
		row = r.rows[r.index]
	}

	if i < 0 || i >= len(row) {
		return types.Value{}, false
	}

	return row[i], true
}

// ColumnInt returns the integer value at column index i.
// Returns (value, true) if the column contains an integer, (0, false) otherwise.
func (r *Rows) ColumnInt(i int) (int64, bool) {
	val, ok := r.getColumnValue(i)
	if !ok {
		return 0, false
	}

	if !types.IsIntegerType(val.Type()) {
		return 0, false
	}

	return val.Int(), true
}

// ColumnFloat returns the float value at column index i.
// Returns (value, true) if the column contains a float, (0, false) otherwise.
func (r *Rows) ColumnFloat(i int) (float64, bool) {
	val, ok := r.getColumnValue(i)
	if !ok {
		return 0, false
	}

	if val.Type() != types.TypeFloat {
		return 0, false
	}

	return val.Float(), true
}

// ColumnText returns the text value at column index i.
// Returns (value, true) if the column contains text, ("", false) otherwise.
func (r *Rows) ColumnText(i int) (string, bool) {
	val, ok := r.getColumnValue(i)
	if !ok {
		return "", false
	}

	if val.Type() != types.TypeText {
		return "", false
	}

	return val.Text(), true
}

// ColumnBlob returns the blob value at column index i.
// Returns (value, true) if the column contains a blob, (nil, false) otherwise.
func (r *Rows) ColumnBlob(i int) ([]byte, bool) {
	val, ok := r.getColumnValue(i)
	if !ok {
		return nil, false
	}

	if val.Type() != types.TypeBlob {
		return nil, false
	}

	return val.Blob(), true
}

// ColumnIsNull returns true if the value at column index i is NULL.
// Returns true for out of bounds indices as well.
func (r *Rows) ColumnIsNull(i int) bool {
	val, ok := r.getColumnValue(i)
	if !ok {
		return true
	}

	return val.IsNull()
}

// ColumnValue returns the raw types.Value at column index i.
// Returns a NULL value if the index is out of bounds.
func (r *Rows) ColumnValue(i int) types.Value {
	val, ok := r.getColumnValue(i)
	if !ok {
		return types.NewNull()
	}

	return val
}

// Close closes the result set and releases any associated resources.
// It is safe to call Close multiple times.
// After Close, Next will always return false and Scan will return an error.
func (r *Rows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil // Already closed, no-op
	}

	r.closed = true

	// Release pooled resources first (before clearing references)
	if r.pooledValues != nil {
		record.ReleaseValues(r.pooledValues)
		r.pooledValues = nil
	}
	if r.pooledView != nil {
		record.ReleaseRecordView(r.pooledView)
		r.pooledView = nil
	}

	// Clear references to allow garbage collection
	r.columns = nil
	r.rows = nil
	r.singleRow = nil

	// Return to pool for reuse
	rowsPool.Put(r)
	return nil
}

// Err returns the error, if any, that was encountered during iteration.
// Err may be called after an explicit or implicit Close.
func (r *Rows) Err() error {
	// For in-memory result sets, there are no iteration errors
	// This method exists for API compatibility with database/sql.Rows
	return nil
}
