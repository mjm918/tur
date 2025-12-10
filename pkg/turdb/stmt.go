// pkg/turdb/stmt.go
package turdb

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

var (
	// ErrStmtClosed is returned when attempting operations on a closed statement
	ErrStmtClosed = errors.New("statement is closed")

	// ErrInvalidParamIndex is returned when binding to an invalid parameter index
	ErrInvalidParamIndex = errors.New("invalid parameter index")
)

// Stmt represents a prepared statement.
// Prepared statements are compiled once and can be executed multiple times
// with different parameter values for improved performance.
type Stmt struct {
	mu sync.Mutex

	// db is the parent database connection
	db *DB

	// sql is the original SQL text
	sql string

	// numParams is the number of parameters in the statement
	numParams int

	// params holds the bound parameter values (1-indexed internally stored as 0-indexed)
	params []types.Value

	// closed indicates if the statement has been closed
	closed bool
}

// SQL returns the original SQL text of the prepared statement.
func (s *Stmt) SQL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sql
}

// NumParams returns the number of parameters in the statement.
func (s *Stmt) NumParams() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numParams
}

// Close closes the prepared statement and releases its resources.
// It is an error to call Close more than once.
func (s *Stmt) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStmtClosed
	}

	s.closed = true
	s.params = nil
	return nil
}

// BindInt binds an integer value to the parameter at the given index.
// Parameter indices are 1-based (like SQLite).
func (s *Stmt) BindInt(index int, value int64) error {
	return s.BindValue(index, types.NewInt(value))
}

// BindText binds a text value to the parameter at the given index.
// Parameter indices are 1-based (like SQLite).
func (s *Stmt) BindText(index int, value string) error {
	return s.BindValue(index, types.NewText(value))
}

// BindFloat binds a float value to the parameter at the given index.
// Parameter indices are 1-based (like SQLite).
func (s *Stmt) BindFloat(index int, value float64) error {
	return s.BindValue(index, types.NewFloat(value))
}

// BindNull binds a NULL value to the parameter at the given index.
// Parameter indices are 1-based (like SQLite).
func (s *Stmt) BindNull(index int) error {
	return s.BindValue(index, types.NewNull())
}

// BindValue binds a generic Value to the parameter at the given index.
// Parameter indices are 1-based (like SQLite).
func (s *Stmt) BindValue(index int, value types.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStmtClosed
	}

	// Validate index (1-based)
	if index < 1 || index > s.numParams {
		return ErrInvalidParamIndex
	}

	// Store at 0-based index
	s.params[index-1] = value
	return nil
}

// ClearBindings clears all bound parameter values, setting them to NULL.
func (s *Stmt) ClearBindings() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	for i := range s.params {
		s.params[i] = types.NewNull()
	}
}

// Reset resets the statement for reuse.
// This is called after execution to allow the statement to be executed again.
func (s *Stmt) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStmtClosed
	}

	// Reset is a no-op for now - parameters are kept
	return nil
}

// Exec executes the prepared statement with the current bound parameters.
// It returns a Result containing the number of rows affected.
// This method is used for INSERT, UPDATE, DELETE, and other statements
// that do not return rows.
func (s *Stmt) Exec() (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return Result{}, ErrStmtClosed
	}

	// Build the SQL with bound parameters substituted
	sqlWithParams := s.substituteParams()

	// Lock the database and execute
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	if s.db.closed {
		return Result{}, ErrDatabaseClosed
	}

	// Execute using the database's executor
	result, err := s.db.executor.Execute(sqlWithParams)
	if err != nil {
		return Result{}, err
	}

	return Result{
		rowsAffected: result.RowsAffected,
	}, nil
}

// substituteParams replaces ? placeholders with the bound parameter values.
// This produces a valid SQL string that can be executed by the executor.
func (s *Stmt) substituteParams() string {
	if s.numParams == 0 {
		return s.sql
	}

	result := strings.Builder{}
	paramIdx := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(s.sql); i++ {
		ch := s.sql[i]

		if inString {
			result.WriteByte(ch)
			if ch == stringChar {
				if i+1 < len(s.sql) && s.sql[i+1] == stringChar {
					i++
					result.WriteByte(s.sql[i])
				} else {
					inString = false
				}
			}
		} else {
			switch ch {
			case '\'', '"':
				inString = true
				stringChar = ch
				result.WriteByte(ch)
			case '?':
				if paramIdx < len(s.params) {
					result.WriteString(valueToSQL(s.params[paramIdx]))
					paramIdx++
				} else {
					result.WriteByte(ch)
				}
			default:
				result.WriteByte(ch)
			}
		}
	}

	return result.String()
}

// Query executes the prepared statement with the current bound parameters
// and returns a Rows iterator for the result set.
// This method is used for SELECT statements and other statements that return rows.
func (s *Stmt) Query() (*Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrStmtClosed
	}

	// Build the SQL with bound parameters substituted
	sqlWithParams := s.substituteParams()

	// Lock the database and execute
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	if s.db.closed {
		return nil, ErrDatabaseClosed
	}

	// Execute using the database's executor
	result, err := s.db.executor.Execute(sqlWithParams)
	if err != nil {
		return nil, err
	}

	return &Rows{
		columns: result.Columns,
		rows:    result.Rows,
		pos:     -1, // Before first row
		closed:  false,
	}, nil
}

// Rows represents a result set from a Query.
// It provides methods to iterate over the result rows and scan values.
type Rows struct {
	columns []string
	rows    [][]types.Value
	pos     int
	closed  bool
	mu      sync.Mutex
}

// Close closes the Rows, releasing any resources.
func (r *Rows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil // Closing twice is allowed
	}

	r.closed = true
	r.rows = nil
	return nil
}

// Next advances to the next row, returning true if there is a row available.
// It must be called before calling Scan.
func (r *Rows) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return false
	}

	r.pos++
	return r.pos < len(r.rows)
}

// Columns returns the column names for the result set.
func (r *Rows) Columns() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]string, len(r.columns))
	copy(result, r.columns)
	return result
}

// Scan copies the columns of the current row into the values pointed at by dest.
// The number of values in dest must match the number of columns in the Rows.
func (r *Rows) Scan(dest ...interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("rows are closed")
	}

	if r.pos < 0 || r.pos >= len(r.rows) {
		return errors.New("no row to scan")
	}

	row := r.rows[r.pos]
	if len(dest) != len(row) {
		return fmt.Errorf("scan expects %d columns but %d provided", len(row), len(dest))
	}

	for i, val := range row {
		if err := scanValue(val, dest[i]); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
	}

	return nil
}

// scanValue converts a types.Value and stores it in dest.
func scanValue(v types.Value, dest interface{}) error {
	switch d := dest.(type) {
	case *int:
		*d = int(v.Int())
	case *int64:
		*d = v.Int()
	case *int32:
		*d = int32(v.Int())
	case *float64:
		if v.Type() == types.TypeFloat {
			*d = v.Float()
		} else {
			*d = float64(v.Int())
		}
	case *float32:
		if v.Type() == types.TypeFloat {
			*d = float32(v.Float())
		} else {
			*d = float32(v.Int())
		}
	case *string:
		switch v.Type() {
		case types.TypeText:
			*d = v.Text()
		case types.TypeInt:
			*d = fmt.Sprintf("%d", v.Int())
		case types.TypeFloat:
			*d = fmt.Sprintf("%g", v.Float())
		case types.TypeNull:
			*d = ""
		default:
			*d = ""
		}
	case *[]byte:
		if v.Type() == types.TypeBlob {
			*d = v.Blob()
		} else if v.Type() == types.TypeText {
			*d = []byte(v.Text())
		} else {
			*d = nil
		}
	case *bool:
		*d = v.Int() != 0
	case *types.Value:
		*d = v
	case *interface{}:
		switch v.Type() {
		case types.TypeNull:
			*d = nil
		case types.TypeInt:
			*d = v.Int()
		case types.TypeFloat:
			*d = v.Float()
		case types.TypeText:
			*d = v.Text()
		case types.TypeBlob:
			*d = v.Blob()
		default:
			*d = nil
		}
	default:
		return fmt.Errorf("unsupported scan destination type: %T", dest)
	}
	return nil
}

// valueToSQL converts a types.Value to its SQL literal representation.
func valueToSQL(v types.Value) string {
	switch v.Type() {
	case types.TypeNull:
		return "NULL"
	case types.TypeInt:
		return fmt.Sprintf("%d", v.Int())
	case types.TypeFloat:
		return fmt.Sprintf("%g", v.Float())
	case types.TypeText:
		// Escape single quotes by doubling them
		text := v.Text()
		escaped := strings.ReplaceAll(text, "'", "''")
		return "'" + escaped + "'"
	case types.TypeBlob:
		// Convert blob to hex string
		blob := v.Blob()
		return "x'" + fmt.Sprintf("%x", blob) + "'"
	default:
		return "NULL"
	}
}

// Prepare prepares a SQL statement for execution.
// The statement can contain parameter placeholders (?) which are bound later.
func (db *DB) Prepare(sql string) (*Stmt, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrDatabaseClosed
	}

	// Count parameter placeholders
	numParams := countParams(sql)

	// Validate the SQL by attempting to parse it
	// We use the executor's ability to parse to validate
	// For now, we just do basic validation by checking it's not empty
	// and doesn't have obvious syntax errors
	if err := validateSQL(sql); err != nil {
		return nil, err
	}

	stmt := &Stmt{
		db:        db,
		sql:       sql,
		numParams: numParams,
		params:    make([]types.Value, numParams),
		closed:    false,
	}

	// Initialize all parameters to NULL
	for i := range stmt.params {
		stmt.params[i] = types.NewNull()
	}

	return stmt, nil
}

// countParams counts the number of ? placeholders in the SQL string.
// It properly handles strings and comments.
func countParams(sql string) int {
	count := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if inString {
			if ch == stringChar {
				// Check for escaped quote
				if i+1 < len(sql) && sql[i+1] == stringChar {
					i++ // Skip escaped quote
				} else {
					inString = false
				}
			}
		} else {
			switch ch {
			case '\'', '"':
				inString = true
				stringChar = ch
			case '?':
				count++
			case '-':
				// Check for -- comment
				if i+1 < len(sql) && sql[i+1] == '-' {
					// Skip to end of line
					for i < len(sql) && sql[i] != '\n' {
						i++
					}
				}
			case '/':
				// Check for /* comment
				if i+1 < len(sql) && sql[i+1] == '*' {
					i += 2
					for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
						i++
					}
					i++ // Skip past */
				}
			}
		}
	}

	return count
}

// validateSQL performs validation of the SQL statement by attempting to parse it.
func validateSQL(sql string) error {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return errors.New("empty SQL statement")
	}

	// Use the actual parser to validate the SQL
	// We need to replace ? placeholders with dummy values for parsing
	sqlForParse := replaceParamsForValidation(trimmed)

	p := parser.New(sqlForParse)
	_, err := p.Parse()
	if err != nil {
		return err
	}

	return nil
}

// replaceParamsForValidation replaces ? placeholders with dummy values
// so the parser can validate the SQL structure.
func replaceParamsForValidation(sql string) string {
	result := strings.Builder{}
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if inString {
			result.WriteByte(ch)
			if ch == stringChar {
				if i+1 < len(sql) && sql[i+1] == stringChar {
					i++
					result.WriteByte(sql[i])
				} else {
					inString = false
				}
			}
		} else {
			switch ch {
			case '\'', '"':
				inString = true
				stringChar = ch
				result.WriteByte(ch)
			case '?':
				// Replace ? with a dummy value (NULL works for any type)
				result.WriteString("NULL")
			default:
				result.WriteByte(ch)
			}
		}
	}

	return result.String()
}
