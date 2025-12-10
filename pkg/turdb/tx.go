// pkg/turdb/tx.go
package turdb

import (
	"context"
	"errors"
	"sync"

	"tur/pkg/mvcc"
)

var (
	// ErrTxDone is returned when a transaction has already been committed or rolled back.
	ErrTxDone = errors.New("transaction has already been committed or rolled back")
)

// Tx represents a database transaction.
// A Tx must end with a call to Commit or Rollback.
//
// After a call to Commit or Rollback, all operations on the
// transaction will fail with ErrTxDone.
type Tx struct {
	mu   sync.Mutex
	db   *DB
	mvcc *mvcc.Transaction
	done bool
}

// Begin starts a new database transaction.
// The transaction is associated with a single database connection.
//
// The returned Tx must be used for all database operations within the
// transaction, and must end with a call to Commit or Rollback.
//
// Example:
//
//	tx, err := db.Begin()
//	if err != nil {
//	    return err
//	}
//	defer tx.Rollback() // no-op if already committed
//
//	// ... use tx for operations ...
//
//	return tx.Commit()
func (db *DB) Begin() (*Tx, error) {
	return db.BeginContext(context.Background())
}

// BeginContext starts a new database transaction with context support.
// The context can be used for cancellation and timeout control.
// If the context is canceled or times out, the operation returns the context's error.
func (db *DB) BeginContext(ctx context.Context) (*Tx, error) {
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

	// Start a new MVCC transaction
	mvccTx := db.txManager.Begin()

	return &Tx{
		db:   db,
		mvcc: mvccTx,
		done: false,
	}, nil
}

// Commit commits the transaction.
// If the commit is successful, all changes made within the transaction
// are persisted to the database.
//
// After Commit returns, the transaction is no longer valid and
// further operations will fail with ErrTxDone.
func (tx *Tx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.done {
		return ErrTxDone
	}

	// Commit the MVCC transaction
	if err := tx.db.txManager.Commit(tx.mvcc); err != nil {
		return err
	}

	tx.done = true
	return nil
}

// Rollback aborts the transaction.
// All changes made within the transaction are discarded.
//
// After Rollback returns, the transaction is no longer valid and
// further operations will fail with ErrTxDone.
//
// Calling Rollback on an already committed or rolled back transaction
// returns ErrTxDone. This allows the common pattern:
//
//	tx, err := db.Begin()
//	if err != nil {
//	    return err
//	}
//	defer tx.Rollback() // safe to call even after commit
//
//	// ... operations ...
//
//	return tx.Commit()
func (tx *Tx) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.done {
		return ErrTxDone
	}

	// Rollback the MVCC transaction
	if err := tx.db.txManager.Rollback(tx.mvcc); err != nil {
		return err
	}

	tx.done = true
	return nil
}

// Exec executes a SQL statement within the transaction.
// The statement is executed using the transaction's isolation context.
//
// Example:
//
//	tx, err := db.Begin()
//	if err != nil {
//	    return err
//	}
//	defer tx.Rollback()
//
//	_, err = tx.Exec("INSERT INTO users (name) VALUES ('Alice')")
//	if err != nil {
//	    return err
//	}
//
//	return tx.Commit()
func (tx *Tx) Exec(sql string) (*QueryResult, error) {
	return tx.ExecContext(context.Background(), sql)
}

// ExecContext executes a SQL statement within the transaction with context support.
// The context can be used for cancellation and timeout control.
// If the context is canceled or times out, the operation returns the context's error.
func (tx *Tx) ExecContext(ctx context.Context, sql string) (*QueryResult, error) {
	// Check context before acquiring lock
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check context again after acquiring lock
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if tx.done {
		return nil, ErrTxDone
	}

	// Execute using the database's executor with our transaction context
	tx.db.mu.Lock()

	// Check context after acquiring database lock
	if err := ctx.Err(); err != nil {
		tx.db.mu.Unlock()
		return nil, err
	}

	// Save current transaction and set ours
	prevTx := tx.db.executor.GetTransaction()
	tx.db.executor.SetTransaction(tx.mvcc)

	execResult, err := tx.db.executor.Execute(sql)

	// Restore previous transaction
	tx.db.executor.SetTransaction(prevTx)
	tx.db.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return convertQueryResult(execResult), nil
}
