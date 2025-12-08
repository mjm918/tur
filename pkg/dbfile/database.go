// pkg/dbfile/database.go
// Database file management including creation, opening, and closing.
package dbfile

import (
	"errors"
	"os"
	"sync"
)

// Database errors.
var (
	ErrDatabaseExists   = errors.New("database file already exists")
	ErrDatabaseNotFound = errors.New("database file not found")
	ErrDatabaseClosed   = errors.New("database is closed")
)

// Options configures database creation and opening.
type Options struct {
	PageSize uint16 // Page size in bytes (default 4096)
	ReadOnly bool   // Open in read-only mode
}

// Database represents an open TurDB database file.
type Database struct {
	mu       sync.RWMutex
	path     string
	file     *os.File
	header   *Header
	closed   bool
	readOnly bool
}

// Create creates a new database file at the given path.
// Returns an error if the file already exists.
func Create(path string, opts *Options) (*Database, error) {
	// Check if file already exists
	if _, err := os.Stat(path); err == nil {
		return nil, ErrDatabaseExists
	}

	// Create the file
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// Initialize header with defaults or options
	header := NewHeader()
	if opts != nil && opts.PageSize > 0 {
		header.PageSize = opts.PageSize
	}

	// Create database instance
	db := &Database{
		path:   path,
		file:   file,
		header: header,
	}

	// Initialize the database file
	if err := db.initialize(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}

	return db, nil
}

// Open opens an existing database file.
// Returns an error if the file doesn't exist or has invalid format.
func Open(path string, opts *Options) (*Database, error) {
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, ErrDatabaseNotFound
	}

	// Determine open mode
	flag := os.O_RDWR
	readOnly := false
	if opts != nil && opts.ReadOnly {
		flag = os.O_RDONLY
		readOnly = true
	}

	// Open the file
	file, err := os.OpenFile(path, flag, 0)
	if err != nil {
		return nil, err
	}

	// Read and validate header
	headerData := make([]byte, HeaderSize)
	if _, err := file.ReadAt(headerData, 0); err != nil {
		file.Close()
		return nil, err
	}

	header, err := DecodeHeader(headerData)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Validate header fields
	if err := ValidateHeader(header); err != nil {
		file.Close()
		return nil, err
	}

	db := &Database{
		path:     path,
		file:     file,
		header:   header,
		readOnly: readOnly,
	}

	return db, nil
}

// initialize writes the initial database structure to a new file.
func (db *Database) initialize() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Allocate space for at least one page (header page)
	pageSize := int64(db.header.PageSize)
	if err := db.file.Truncate(pageSize); err != nil {
		return err
	}

	// Write the header
	headerData := db.header.Encode()
	if _, err := db.file.WriteAt(headerData, 0); err != nil {
		return err
	}

	// Sync to ensure header is written
	return db.file.Sync()
}

// Sync writes the header and syncs the file to disk.
func (db *Database) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	if db.readOnly {
		return nil
	}

	// Write header
	headerData := db.header.Encode()
	if _, err := db.file.WriteAt(headerData, 0); err != nil {
		return err
	}

	return db.file.Sync()
}

// Close closes the database file.
func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	db.closed = true

	// Write header before closing (if not read-only)
	if !db.readOnly && db.file != nil {
		headerData := db.header.Encode()
		db.file.WriteAt(headerData, 0)
		db.file.Sync()
	}

	if db.file != nil {
		return db.file.Close()
	}

	return nil
}

// Header returns the database header.
func (db *Database) Header() *Header {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.header
}

// PageSize returns the page size in bytes.
func (db *Database) PageSize() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return int(db.header.PageSize)
}

// PageCount returns the number of pages in the database.
func (db *Database) PageCount() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.header.PageCount
}

// Path returns the database file path.
func (db *Database) Path() string {
	return db.path
}

// IsReadOnly returns true if the database is open in read-only mode.
func (db *Database) IsReadOnly() bool {
	return db.readOnly
}

// IsClosed returns true if the database has been closed.
func (db *Database) IsClosed() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.closed
}

// SchemaVersion returns the schema format version.
func (db *Database) SchemaVersion() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.header.SchemaVersion
}

// SetSchemaVersion sets the schema format version.
func (db *Database) SetSchemaVersion(version uint32) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.header.SchemaVersion = version
}

// SchemaCookie returns the schema cookie (incremented on schema changes).
func (db *Database) SchemaCookie() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.header.SchemaCookie
}

// IncrementSchemaCookie increments the schema cookie.
// This should be called when the schema changes (CREATE/DROP TABLE, etc.).
func (db *Database) IncrementSchemaCookie() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.header.SchemaCookie++
}

// ChangeCounter returns the file change counter.
func (db *Database) ChangeCounter() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.header.ChangeCounter
}

// IncrementChangeCounter increments the change counter.
// This should be called when the database file is modified.
func (db *Database) IncrementChangeCounter() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.header.ChangeCounter++
}
