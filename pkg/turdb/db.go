// pkg/turdb/db.go
package turdb

import (
	"context"
	"errors"
	"os"
	"sync"

	"tur/pkg/btree"
	"tur/pkg/hnsw"
	"tur/pkg/mvcc"
	"tur/pkg/pager"
	"tur/pkg/schema"
	"tur/pkg/sql/executor"
	"tur/pkg/types"
)

var (
	// ErrDatabaseClosed is returned when attempting operations on a closed database
	ErrDatabaseClosed = errors.New("database is closed")

	// ErrDatabaseLocked is returned when the database file is already locked
	ErrDatabaseLocked = errors.New("database is locked by another connection")
)

// DB represents an open database connection.
// It provides the main entry point for database operations.
type DB struct {
	mu sync.RWMutex

	// path is the file path of the database
	path string

	// lockFile holds the lock file to prevent concurrent access
	lockFile *os.File

	// pager manages page-level I/O and caching
	pager *pager.Pager

	// catalog holds schema metadata (tables, indexes, views, triggers)
	catalog *schema.Catalog

	// trees holds B-tree structures for each table
	trees map[string]*btree.BTree

	// rowid tracks next rowid for each table
	rowid map[string]uint64

	// maxRowid tracks max INTEGER PRIMARY KEY value per table (for AUTOINCREMENT)
	maxRowid map[string]int64

	// txManager handles MVCC transactions
	txManager *mvcc.TransactionManager

	// hnswIndexes holds HNSW indexes for vector columns
	hnswIndexes map[string]*hnsw.Index

	// executor handles SQL execution
	executor *executor.Executor

	// stmtCache caches prepared statements by SQL text
	stmtCache map[string]*Stmt

	// closed indicates if the database has been closed
	closed bool
}

// Open opens a database file and returns a new DB handle.
// If the file does not exist, it will be created.
// The caller is responsible for calling Close when done.
func Open(path string) (*DB, error) {
	return OpenWithOptions(path, Options{})
}

// Options configures database opening behavior
type Options struct {
	// PageSize specifies the page size in bytes (default 4096)
	PageSize int

	// CacheSize specifies the number of pages to cache (default 1000)
	CacheSize int

	// ReadOnly opens the database in read-only mode
	ReadOnly bool
}

// OpenWithOptions opens a database file with the specified options.
func OpenWithOptions(path string, opts Options) (*DB, error) {
	// Acquire exclusive lock on lock file to prevent concurrent access
	lockPath := path + ".lock"
	lf, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Try to acquire exclusive lock (non-blocking)
	if err := lockFile(lf); err != nil {
		lf.Close()
		return nil, err
	}

	// Configure pager options
	pagerOpts := pager.Options{
		PageSize:  opts.PageSize,
		CacheSize: opts.CacheSize,
		ReadOnly:  opts.ReadOnly,
	}

	// Open the pager (handles file creation if needed)
	p, err := pager.Open(path, pagerOpts)
	if err != nil {
		// Release lock and close lock file on error
		unlockFile(lf)
		lf.Close()
		return nil, err
	}

	db := &DB{
		path:        path,
		lockFile:    lf,
		pager:       p,
		catalog:     schema.NewCatalog(),
		trees:       make(map[string]*btree.BTree),
		rowid:       make(map[string]uint64),
		maxRowid:    make(map[string]int64),
		txManager:   mvcc.NewTransactionManager(),
		hnswIndexes: make(map[string]*hnsw.Index),
		executor:    executor.New(p),
		stmtCache:   make(map[string]*Stmt),
		closed:      false,
	}

	return db, nil
}

// Path returns the file path of the database.
func (db *DB) Path() string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.path
}

// Close closes the database connection and releases resources.
// It is an error to call Close more than once.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	db.closed = true

	// Close all cached statements
	for _, stmt := range db.stmtCache {
		stmt.closed = true
	}
	db.stmtCache = nil

	var closeErr error

	// Close the pager (syncs and closes the file)
	if db.pager != nil {
		closeErr = db.pager.Close()
	}

	// Release the file lock and close lock file
	if db.lockFile != nil {
		unlockFile(db.lockFile)
		db.lockFile.Close()
		db.lockFile = nil
	}

	return closeErr
}

// IsClosed returns true if the database has been closed.
func (db *DB) IsClosed() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.closed
}

// Pager returns the underlying pager for advanced operations.
// This is primarily for internal use.
func (db *DB) Pager() *pager.Pager {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.pager
}

// Catalog returns the schema catalog for inspecting metadata.
func (db *DB) Catalog() *schema.Catalog {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.catalog
}

// Exec executes a SQL statement and returns the result.
// It is a convenience method that prepares, executes, and closes a statement.
func (db *DB) Exec(sql string) (*QueryResult, error) {
	return db.ExecContext(context.Background(), sql)
}

// ExecContext executes a SQL statement with context support.
// The context can be used for cancellation and timeout control.
// If the context is canceled or times out, the operation returns the context's error.
func (db *DB) ExecContext(ctx context.Context, sql string) (*QueryResult, error) {
	// Check context before acquiring lock
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Check context again after acquiring lock
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if db.closed {
		return nil, ErrDatabaseClosed
	}

	// Use the executor directly for non-parameterized queries
	result, err := db.executor.Execute(sql)
	if err != nil {
		return nil, err
	}

	return convertQueryResult(result), nil
}

// ExecResult represents the result of an Exec operation (for prepared statements)
type ExecResult struct {
	lastInsertID int64
	rowsAffected int64
}

// LastInsertId returns the ID of the last inserted row
func (r ExecResult) LastInsertId() int64 {
	return r.lastInsertID
}

// RowsAffected returns the number of rows affected by the statement
func (r ExecResult) RowsAffected() int64 {
	return r.rowsAffected
}

// PrepareWithCache prepares a SQL statement, using a cached version if available.
// Unlike Prepare, this method caches the statement for reuse with the same SQL.
// This is useful for frequently executed queries.
func (db *DB) PrepareWithCache(sql string) (*Stmt, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrDatabaseClosed
	}

	// Check if statement is already cached
	if stmt, ok := db.stmtCache[sql]; ok && !stmt.closed {
		// Reset the cached statement for reuse
		stmt.ClearBindings()
		return stmt, nil
	}

	// Need to unlock to call Prepare (it acquires its own lock)
	db.mu.Unlock()
	stmt, err := db.Prepare(sql)
	db.mu.Lock()

	if err != nil {
		return nil, err
	}

	// Cache the new statement
	db.stmtCache[sql] = stmt

	return stmt, nil
}

// ClearStmtCache clears the prepared statement cache.
// This closes all cached statements and removes them from the cache.
func (db *DB) ClearStmtCache() {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Close all cached statements
	for _, stmt := range db.stmtCache {
		stmt.closed = true
	}

	// Clear the cache
	db.stmtCache = make(map[string]*Stmt)
}

// QueryResult holds the result of executing a SQL query that returns rows.
// This is used by transaction methods that need full result data.
type QueryResult struct {
	// Columns contains the column names for SELECT queries.
	Columns []string

	// Rows contains the result rows for SELECT queries.
	Rows [][]interface{}

	// RowsAffected is the number of rows affected by INSERT, UPDATE, or DELETE.
	RowsAffected int64
}

// convertQueryResult converts executor.Result to turdb.QueryResult
func convertQueryResult(r *executor.Result) *QueryResult {
	if r == nil {
		return &QueryResult{}
	}

	// Convert types.Value rows to interface{} rows
	rows := make([][]interface{}, len(r.Rows))
	for i, row := range r.Rows {
		rows[i] = make([]interface{}, len(row))
		for j, val := range row {
			rows[i][j] = valueToGo(val)
		}
	}

	return &QueryResult{
		Columns:      r.Columns,
		Rows:         rows,
		RowsAffected: r.RowsAffected,
	}
}

// valueToGo converts a types.Value to a Go native type
func valueToGo(v types.Value) interface{} {
	switch v.Type() {
	case types.TypeNull:
		return nil
	case types.TypeInt:
		return v.Int()
	case types.TypeFloat:
		return v.Float()
	case types.TypeText:
		return v.Text()
	case types.TypeBlob:
		return v.Blob()
	case types.TypeVector:
		return v.Vector()
	default:
		return nil
	}
}
