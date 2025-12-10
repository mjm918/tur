// pkg/turdb/db.go
package turdb

import (
	"errors"
	"os"
	"sync"

	"golang.org/x/sys/unix"

	"tur/pkg/btree"
	"tur/pkg/hnsw"
	"tur/pkg/mvcc"
	"tur/pkg/pager"
	"tur/pkg/schema"
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
	lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Try to acquire exclusive lock (non-blocking)
	err = unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		lockFile.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrDatabaseLocked
		}
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
		unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
		lockFile.Close()
		return nil, err
	}

	db := &DB{
		path:        path,
		lockFile:    lockFile,
		pager:       p,
		catalog:     schema.NewCatalog(),
		trees:       make(map[string]*btree.BTree),
		rowid:       make(map[string]uint64),
		maxRowid:    make(map[string]int64),
		txManager:   mvcc.NewTransactionManager(),
		hnswIndexes: make(map[string]*hnsw.Index),
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

	var closeErr error

	// Close the pager (syncs and closes the file)
	if db.pager != nil {
		closeErr = db.pager.Close()
	}

	// Release the file lock and close lock file
	if db.lockFile != nil {
		unix.Flock(int(db.lockFile.Fd()), unix.LOCK_UN)
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
