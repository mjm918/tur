// pkg/turdb/stmt.go
package turdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"tur/pkg/sql/lexer"
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

	// cachedAST is the pre-parsed AST for faster execution
	cachedAST parser.Statement

	// Fast path optimization fields (detected at Prepare time)
	isFastPathPK    bool   // True if this is a simple PK lookup query
	fastPathTable   string // Table name for fast path
	fastPathPKParam int    // Which parameter (0-indexed) is the PK value, -1 if literal
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
// It returns an ExecResult containing the number of rows affected.
// This method is used for INSERT, UPDATE, DELETE, and other statements
// that do not return rows.
func (s *Stmt) Exec() (ExecResult, error) {
	return s.ExecContext(context.Background())
}

// ExecContext executes the prepared statement with context support.
// The context can be used for cancellation and timeout control.
// If the context is canceled or times out, the operation returns the context's error.
func (s *Stmt) ExecContext(ctx context.Context) (ExecResult, error) {
	// Check context before acquiring lock
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check context again after acquiring lock
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}

	if s.closed {
		return ExecResult{}, ErrStmtClosed
	}

	// Lock the database and execute
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	// Check context after acquiring database lock
	if err := ctx.Err(); err != nil {
		return ExecResult{}, err
	}

	if s.db.closed {
		return ExecResult{}, ErrDatabaseClosed
	}

	// Use cached AST with parameter substitution (faster path)
	if s.cachedAST != nil {
		result, err := s.db.executor.ExecuteAST(s.cachedAST, s.params)
		if err != nil {
			return ExecResult{}, err
		}
		return ExecResult{
			rowsAffected: result.RowsAffected,
		}, nil
	}

	// Fallback: Build the SQL with bound parameters substituted
	sqlWithParams := s.substituteParams()
	result, err := s.db.executor.Execute(sqlWithParams)
	if err != nil {
		return ExecResult{}, err
	}

	return ExecResult{
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
	return s.QueryContext(context.Background())
}

// QueryContext executes the prepared statement with context support
// and returns a Rows iterator for the result set.
// The context can be used for cancellation and timeout control.
// If the context is canceled or times out, the operation returns the context's error.
func (s *Stmt) QueryContext(ctx context.Context) (*Rows, error) {
	// Check context before acquiring lock
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check context again after acquiring lock
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s.closed {
		return nil, ErrStmtClosed
	}

	// Lock the database and execute
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	// Check context after acquiring database lock
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if s.db.closed {
		return nil, ErrDatabaseClosed
	}

	// ULTRA-FAST PATH: Direct PK lookup bypassing executor
	if s.isFastPathPK && s.fastPathPKParam >= 0 && s.fastPathPKParam < len(s.params) {
		pkValue := s.params[s.fastPathPKParam]
		if isIntegerType(pkValue.Type()) {
			result, err := s.db.executor.DirectPKLookup(s.fastPathTable, pkValue.Int())
			if err != nil {
				// Fall through to regular path on error
			} else {
				return NewRows(result.Columns, result.Rows), nil
			}
		}
	}

	// Use cached AST with parameter substitution (faster path)
	if s.cachedAST != nil {
		result, err := s.db.executor.ExecuteAST(s.cachedAST, s.params)
		if err != nil {
			return nil, err
		}
		return NewRows(result.Columns, result.Rows), nil
	}

	// Fallback: Build the SQL with bound parameters substituted
	sqlWithParams := s.substituteParams()
	result, err := s.db.executor.Execute(sqlWithParams)
	if err != nil {
		return nil, err
	}

	return NewRows(result.Columns, result.Rows), nil
}

// valueToSQL converts a types.Value to its SQL literal representation.
func valueToSQL(v types.Value) string {
	switch v.Type() {
	case types.TypeNull:
		return "NULL"
	case types.TypeSmallInt, types.TypeInt32, types.TypeBigInt, types.TypeSerial, types.TypeBigSerial:
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
// The SQL is parsed once during Prepare and the AST is cached for faster execution.
func (db *DB) Prepare(sql string) (*Stmt, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrDatabaseClosed
	}

	// Parse the SQL once and cache the AST
	p := parser.New(sql)
	ast, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("prepare error: %w", err)
	}

	// Get the placeholder count from the parser
	numParams := p.PlaceholderCount()

	stmt := &Stmt{
		db:            db,
		sql:           sql,
		numParams:     numParams,
		params:        make([]types.Value, numParams),
		closed:        false,
		cachedAST:     ast,
		fastPathPKParam: -1,
	}

	// Detect fast path eligibility at prepare time
	stmt.detectFastPath(db)

	// Initialize all parameters to NULL
	for i := range stmt.params {
		stmt.params[i] = types.NewNull()
	}

	return stmt, nil
}

// detectFastPath checks if this statement is eligible for ultra-fast PK lookup.
// Eligible queries: SELECT * FROM table WHERE pk = ?
func (s *Stmt) detectFastPath(db *DB) {
	selectStmt, ok := s.cachedAST.(*parser.SelectStmt)
	if !ok {
		return
	}

	// Requirements for fast path:
	// 1. Must be SELECT * (star)
	// 2. No CTEs, GROUP BY, HAVING, ORDER BY, LIMIT
	// 3. Single table, no joins
	// 4. WHERE is pk_column = ? (placeholder)

	// Check for SELECT * (must have exactly one column and it must be a star)
	if selectStmt.Columns == nil || len(selectStmt.Columns) != 1 || !selectStmt.Columns[0].Star {
		return
	}

	if selectStmt.With != nil || selectStmt.GroupBy != nil || selectStmt.Having != nil {
		return
	}
	if selectStmt.OrderBy != nil || selectStmt.Limit != nil {
		return
	}
	if selectStmt.Where == nil || selectStmt.From == nil {
		return
	}

	// Check for single table
	table, ok := selectStmt.From.(*parser.Table)
	if !ok {
		return
	}

	// Get table definition from catalog
	tableDef := db.executor.GetCatalog().GetTable(table.Name)
	if tableDef == nil {
		return
	}

	// Find primary key column
	pkColIndex := tableDef.GetPKColumnIndex()
	if pkColIndex == -1 {
		return
	}
	pkCol := &tableDef.Columns[pkColIndex]

	// Only integer PK supported for fast path
	if !isIntegerType(pkCol.Type) {
		return
	}

	// Check WHERE clause: pk_column = ?
	binExpr, ok := selectStmt.Where.(*parser.BinaryExpr)
	if !ok {
		return
	}

	// Must be equality
	if binExpr.Op != lexer.EQ {
		return
	}

	// Left side should be column reference
	colRef, ok := binExpr.Left.(*parser.ColumnRef)
	if !ok {
		return
	}

	// Must reference the primary key column
	if !strings.EqualFold(colRef.Name, pkCol.Name) {
		return
	}

	// Right side should be a placeholder (parameter)
	if _, ok := binExpr.Right.(*parser.Placeholder); ok {
		// Fast path eligible!
		s.isFastPathPK = true
		s.fastPathTable = table.Name
		s.fastPathPKParam = 0 // First (and only) parameter
	}
}

// isIntegerType checks if a value type is an integer type
func isIntegerType(t types.ValueType) bool {
	switch t {
	case types.TypeSmallInt, types.TypeInt32, types.TypeBigInt, types.TypeSerial, types.TypeBigSerial:
		return true
	}
	return false
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
