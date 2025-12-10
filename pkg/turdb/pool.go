// pkg/turdb/pool.go
package turdb

import (
	"container/list"
	"errors"
	"sync"
)

var (
	// ErrPoolClosed is returned when attempting operations on a closed pool
	ErrPoolClosed = errors.New("connection pool is closed")

	// ErrInvalidMaxConns is returned when maxConns is less than 1
	ErrInvalidMaxConns = errors.New("maxConns must be at least 1")
)

// Pool manages a pool of database connections for concurrent access.
// It maintains a queue of idle connections and creates new ones as needed,
// up to the configured maximum.
type Pool struct {
	mu sync.Mutex

	// path is the database file path
	path string

	// opts holds database options for creating connections
	opts Options

	// maxConns is the maximum number of connections allowed
	maxConns int

	// idle holds available connections ready for checkout
	idle *list.List

	// numOpen is the total number of open connections (idle + in-use)
	numOpen int

	// closed indicates if the pool has been closed
	closed bool
}

// OpenPool creates a new connection pool for the given database path.
// maxConns specifies the maximum number of connections the pool can maintain.
// At least one connection must be allowed (maxConns >= 1).
func OpenPool(path string, maxConns int) (*Pool, error) {
	return OpenPoolWithOptions(path, maxConns, Options{})
}

// OpenPoolWithOptions creates a new connection pool with custom options.
func OpenPoolWithOptions(path string, maxConns int, opts Options) (*Pool, error) {
	if maxConns < 1 {
		return nil, ErrInvalidMaxConns
	}

	pool := &Pool{
		path:     path,
		opts:     opts,
		maxConns: maxConns,
		idle:     list.New(),
		numOpen:  0,
		closed:   false,
	}

	return pool, nil
}

// MaxConns returns the maximum number of connections allowed in the pool.
func (p *Pool) MaxConns() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxConns
}

// Path returns the database file path.
func (p *Pool) Path() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.path
}

// Close closes all connections in the pool and prevents new checkouts.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrPoolClosed
	}

	p.closed = true

	// Close all idle connections
	var closeErr error
	for e := p.idle.Front(); e != nil; e = e.Next() {
		conn := e.Value.(*poolConn)
		if err := conn.db.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	p.idle.Init() // Clear the list
	p.numOpen = 0

	return closeErr
}

// poolConn wraps a DB connection for pool management
type poolConn struct {
	db   *DB
	pool *Pool
}

// Get retrieves a connection from the pool.
// If an idle connection is available, it is returned immediately.
// Otherwise, a new connection is created if under the maxConns limit.
// The returned connection must be returned to the pool using Put when done.
func (p *Pool) Get() (*DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrPoolClosed
	}

	// Try to get an idle connection
	if p.idle.Len() > 0 {
		elem := p.idle.Front()
		p.idle.Remove(elem)
		pc := elem.Value.(*poolConn)
		return pc.db, nil
	}

	// No idle connection, create a new one if under limit
	if p.numOpen < p.maxConns {
		db, err := OpenWithOptions(p.path, p.opts)
		if err != nil {
			return nil, err
		}
		p.numOpen++
		return db, nil
	}

	// At max connections - for now, return an error
	// (blocking wait could be added later)
	return nil, errors.New("connection pool exhausted")
}

// Put returns a connection to the pool.
// If the pool is closed, the connection is closed instead.
// The connection should not be used after calling Put.
func (p *Pool) Put(conn *DB) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// If pool is closed, close the connection
	if p.closed {
		conn.Close()
		return
	}

	// Return connection to idle list
	pc := &poolConn{db: conn, pool: p}
	p.idle.PushBack(pc)
}

// NumOpen returns the number of open connections (idle + in-use).
func (p *Pool) NumOpen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.numOpen
}
