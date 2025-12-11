package tests

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"tur/pkg/turdb"
)

// SQLite best practices for performance:
// 1. Use prepared statements
// 2. Use WAL mode for concurrent reads
// 3. Set synchronous=OFF for benchmarks (not for production)
// 4. Use memory-mapped I/O
// 5. Batch inserts in transactions

func setupSQLiteOptimized(b *testing.B, dbPath string) *sql.DB {
	// Use WAL mode and memory-mapped I/O via connection string
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=OFF&_mmap_size=268435456")
	if err != nil {
		b.Fatalf("Failed to open SQLite: %v", err)
	}
	return db
}

// BenchmarkInsert_TurDB benchmarks INSERT performance for TurDB using prepared statements
func BenchmarkInsert_TurDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := turdb.Open(dbPath)
	if err != nil {
		b.Fatalf("Failed to open TurDB: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Use prepared statement
	stmt, err := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.BindInt(1, int64(i))
		stmt.BindText(2, "name")
		stmt.BindInt(3, int64(i*10))
		_, err := stmt.Exec()
		if err != nil {
			b.Fatalf("INSERT failed at iteration %d: %v", i, err)
		}
	}
}

// BenchmarkInsert_SQLite benchmarks INSERT performance for SQLite with best practices
func BenchmarkInsert_SQLite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db := setupSQLiteOptimized(b, dbPath)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Use prepared statement
	stmt, err := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stmt.Exec(i, "name", i*10)
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
	}
}

// BenchmarkSelect_TurDB benchmarks SELECT performance for TurDB using prepared statements
func BenchmarkSelect_TurDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := turdb.Open(dbPath)
	if err != nil {
		b.Fatalf("Failed to open TurDB: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Pre-populate with 100 rows
	insertStmt, _ := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	for i := 0; i < 100; i++ {
		insertStmt.BindInt(1, int64(i))
		insertStmt.BindText(2, "name")
		insertStmt.BindInt(3, int64(i*10))
		insertStmt.Exec()
	}
	insertStmt.Close()

	// Use prepared statement for SELECT
	stmt, err := db.Prepare("SELECT * FROM bench WHERE id = ?")
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	stmt.BindInt(1, 50)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Query()
		if err != nil {
			b.Fatalf("SELECT failed: %v", err)
		}
		rows.Close()
	}
}

// BenchmarkSelect_SQLite benchmarks SELECT performance for SQLite with best practices
func BenchmarkSelect_SQLite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db := setupSQLiteOptimized(b, dbPath)
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Pre-populate with 100 rows using prepared statement
	insertStmt, _ := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	for i := 0; i < 100; i++ {
		insertStmt.Exec(i, "name", i*10)
	}
	insertStmt.Close()

	// Use prepared statement for SELECT
	stmt, err := db.Prepare("SELECT * FROM bench WHERE id = ?")
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Query(50)
		if err != nil {
			b.Fatalf("SELECT failed: %v", err)
		}
		rows.Close()
	}
}

// BenchmarkUpdate_TurDB benchmarks UPDATE performance for TurDB using prepared statements
func BenchmarkUpdate_TurDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := turdb.Open(dbPath)
	if err != nil {
		b.Fatalf("Failed to open TurDB: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Pre-populate with 100 rows
	insertStmt, _ := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	for i := 0; i < 100; i++ {
		insertStmt.BindInt(1, int64(i))
		insertStmt.BindText(2, "name")
		insertStmt.BindInt(3, int64(i*10))
		insertStmt.Exec()
	}
	insertStmt.Close()

	// Use prepared statement for UPDATE
	stmt, err := db.Prepare("UPDATE bench SET value = ? WHERE id = ?")
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.BindInt(1, int64(i))
		stmt.BindInt(2, 50)
		_, err := stmt.Exec()
		if err != nil {
			b.Fatalf("UPDATE failed: %v", err)
		}
	}
}

// BenchmarkUpdate_SQLite benchmarks UPDATE performance for SQLite with best practices
func BenchmarkUpdate_SQLite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db := setupSQLiteOptimized(b, dbPath)
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Pre-populate with 100 rows
	insertStmt, _ := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	for i := 0; i < 100; i++ {
		insertStmt.Exec(i, "name", i*10)
	}
	insertStmt.Close()

	// Use prepared statement for UPDATE
	stmt, err := db.Prepare("UPDATE bench SET value = ? WHERE id = ?")
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stmt.Exec(i, 50)
		if err != nil {
			b.Fatalf("UPDATE failed: %v", err)
		}
	}
}

// BenchmarkTransaction_TurDB benchmarks transaction with rollback for TurDB
func BenchmarkTransaction_TurDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := turdb.Open(dbPath)
	if err != nil {
		b.Fatalf("Failed to open TurDB: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Use prepared statement
	stmt, err := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec("BEGIN")
		stmt.BindInt(1, int64(i))
		stmt.BindText(2, "name")
		stmt.BindInt(3, int64(i*10))
		stmt.Exec()
		db.Exec("ROLLBACK")
	}
}

// BenchmarkTransaction_SQLite benchmarks transaction with rollback for SQLite
func BenchmarkTransaction_SQLite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db := setupSQLiteOptimized(b, dbPath)
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Use prepared statement
	stmt, err := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin()
		tx.Stmt(stmt).Exec(i, "name", i*10)
		tx.Rollback()
	}
}

// BenchmarkTransactionCommit_TurDB benchmarks transaction with commit for TurDB
func BenchmarkTransactionCommit_TurDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := turdb.Open(dbPath)
	if err != nil {
		b.Fatalf("Failed to open TurDB: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Use prepared statement
	stmt, err := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec("BEGIN")
		stmt.BindInt(1, int64(i))
		stmt.BindText(2, "name")
		stmt.BindInt(3, int64(i*10))
		stmt.Exec()
		db.Exec("COMMIT")
	}
}

// BenchmarkTransactionCommit_SQLite benchmarks transaction with commit for SQLite
func BenchmarkTransactionCommit_SQLite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db := setupSQLiteOptimized(b, dbPath)
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Use prepared statement
	stmt, err := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin()
		tx.Stmt(stmt).Exec(i, "name", i*10)
		tx.Commit()
	}
}

// RunComparison runs the benchmarks and prints a comparison table
func TestPrintBenchmarkComparison(t *testing.T) {
	if os.Getenv("RUN_BENCHMARK_COMPARISON") != "1" {
		t.Skip("Skipping benchmark comparison. Set RUN_BENCHMARK_COMPARISON=1 to run.")
	}

	t.Log("Run benchmarks with: go test -bench=. -benchmem ./tests/")
	t.Log("Compare TurDB vs SQLite results")
}
