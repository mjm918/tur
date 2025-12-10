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
