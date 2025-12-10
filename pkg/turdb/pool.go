// pkg/turdb/pool.go
package turdb

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

var (
	// ErrPoolClosed is returned when attempting operations on a closed pool
	ErrPoolClosed = errors.New("connection pool is closed")

	// ErrInvalidMaxConns is returned when maxConns is less than 1
	ErrInvalidMaxConns = errors.New("maxConns must be at least 1")
)

// PoolOptions configures pool behavior beyond database options
type PoolOptions struct {
	// DBOptions holds database options for creating connections
	DBOptions Options

	// MaxIdleTime is the maximum time a connection can remain idle before being closed.
	// Zero means no idle timeout (connections stay idle indefinitely).
	MaxIdleTime time.Duration

	// MaxLifetime is the maximum total lifetime of a connection from creation.
	// Zero means no lifetime limit.
	MaxLifetime time.Duration
}

// PoolStats contains statistics about the connection pool
type PoolStats struct {
	// MaxConns is the configured maximum connections
	MaxConns int

	// NumOpen is the total number of open connections (idle + in-use)
	NumOpen int

	// NumIdle is the number of idle connections waiting to be used
	NumIdle int

	// NumInUse is the number of connections currently checked out
	NumInUse int

	// TotalGets is the total number of Get() calls
	TotalGets int64

	// TotalPuts is the total number of Put() calls
	TotalPuts int64

	// TotalCreated is the total number of new connections created
	TotalCreated int64

	// TotalClosed is the total number of connections closed
	TotalClosed int64

	// HitCount is the number of Get() calls that returned an idle connection
	HitCount int64

	// MissCount is the number of Get() calls that required creating a new connection
	MissCount int64
}

// Pool manages a pool of database connections for concurrent access.
// It maintains a queue of idle connections and creates new ones as needed,
// up to the configured maximum.
type Pool struct {
	mu sync.Mutex

	// path is the database file path
	path string

	// opts holds database options for creating connections
	opts Options

	// poolOpts holds pool-specific options
	poolOpts PoolOptions

	// maxConns is the maximum number of connections allowed
	maxConns int

	// idle holds available connections ready for checkout
	idle *list.List

	// connMeta tracks metadata for all open connections by DB pointer
	connMeta map[*DB]*poolConn

	// numOpen is the total number of open connections (idle + in-use)
	numOpen int

	// closed indicates if the pool has been closed
	closed bool

	// Stats counters
	totalGets    int64
	totalPuts    int64
	totalCreated int64
	totalClosed  int64
	hitCount     int64
	missCount    int64
}

// OpenPool creates a new connection pool for the given database path.
// maxConns specifies the maximum number of connections the pool can maintain.
// At least one connection must be allowed (maxConns >= 1).
func OpenPool(path string, maxConns int) (*Pool, error) {
	return OpenPoolWithOptions(path, maxConns, Options{})
}

// OpenPoolWithOptions creates a new connection pool with custom database options.
func OpenPoolWithOptions(path string, maxConns int, opts Options) (*Pool, error) {
	return OpenPoolWithPoolOptions(path, maxConns, PoolOptions{DBOptions: opts})
}

// OpenPoolWithPoolOptions creates a new connection pool with full pool configuration.
func OpenPoolWithPoolOptions(path string, maxConns int, poolOpts PoolOptions) (*Pool, error) {
	if maxConns < 1 {
		return nil, ErrInvalidMaxConns
	}

	pool := &Pool{
		path:     path,
		opts:     poolOpts.DBOptions,
		poolOpts: poolOpts,
		maxConns: maxConns,
		idle:     list.New(),
		connMeta: make(map[*DB]*poolConn),
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
	db        *DB
	pool      *Pool
	createdAt time.Time // when the connection was created
	idleSince time.Time // when the connection became idle (updated on Put)
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

	p.totalGets++

	// Try to get an idle connection
	if p.idle.Len() > 0 {
		elem := p.idle.Front()
		p.idle.Remove(elem)
		pc := elem.Value.(*poolConn)
		p.hitCount++
		return pc.db, nil
	}

	// No idle connection, create a new one if under limit
	if p.numOpen < p.maxConns {
		db, err := OpenWithOptions(p.path, p.opts)
		if err != nil {
			return nil, err
		}
		now := time.Now()
		pc := &poolConn{
			db:        db,
			pool:      p,
			createdAt: now,
		}
		p.connMeta[db] = pc
		p.numOpen++
		p.totalCreated++
		p.missCount++
		return db, nil
	}

	// At max connections - for now, return an error
	// (blocking wait could be added later)
	return nil, errors.New("connection pool exhausted")
}

// Put returns a connection to the pool.
// If the pool is closed or the connection's lifetime has exceeded MaxLifetime,
// the connection is closed instead. The connection should not be used after calling Put.
func (p *Pool) Put(conn *DB) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.totalPuts++

	// If pool is closed, close the connection
	if p.closed {
		conn.Close()
		p.totalClosed++
		return
	}

	// Get connection metadata
	pc, ok := p.connMeta[conn]
	if !ok {
		// Connection not from this pool or was already closed, just close it
		conn.Close()
		p.totalClosed++
		return
	}

	now := time.Now()

	// Check if connection has exceeded its lifetime
	if p.poolOpts.MaxLifetime > 0 && now.Sub(pc.createdAt) >= p.poolOpts.MaxLifetime {
		// Close the connection and remove from tracking
		conn.Close()
		delete(p.connMeta, conn)
		p.numOpen--
		p.totalClosed++
		return
	}

	// Update idle timestamp and return to pool
	pc.idleSince = now
	p.idle.PushBack(pc)
}

// NumOpen returns the number of open connections (idle + in-use).
func (p *Pool) NumOpen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.numOpen
}

// NumIdle returns the number of idle connections in the pool.
func (p *Pool) NumIdle() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.idle.Len()
}

// CleanupExpired closes idle connections that have exceeded MaxIdleTime.
// This method can be called periodically to remove stale connections.
func (p *Pool) CleanupExpired() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed || p.poolOpts.MaxIdleTime <= 0 {
		return
	}

	now := time.Now()
	var toRemove []*list.Element

	// Find expired connections
	for e := p.idle.Front(); e != nil; e = e.Next() {
		pc := e.Value.(*poolConn)
		if now.Sub(pc.idleSince) >= p.poolOpts.MaxIdleTime {
			toRemove = append(toRemove, e)
		}
	}

	// Remove and close expired connections
	for _, e := range toRemove {
		pc := e.Value.(*poolConn)
		p.idle.Remove(e)
		pc.db.Close()
		delete(p.connMeta, pc.db)
		p.numOpen--
		p.totalClosed++
	}
}

// Stats returns a snapshot of pool statistics.
func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	numIdle := p.idle.Len()
	return PoolStats{
		MaxConns:     p.maxConns,
		NumOpen:      p.numOpen,
		NumIdle:      numIdle,
		NumInUse:     p.numOpen - numIdle,
		TotalGets:    p.totalGets,
		TotalPuts:    p.totalPuts,
		TotalCreated: p.totalCreated,
		TotalClosed:  p.totalClosed,
		HitCount:     p.hitCount,
		MissCount:    p.missCount,
	}
}
