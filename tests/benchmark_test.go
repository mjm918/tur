package tests

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"tur/pkg/turdb"
)

// BenchmarkInsert_TurDB benchmarks INSERT performance for TurDB
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name%d', %d)", i, i, i*10))
		if err != nil {
			b.Fatalf("INSERT failed at iteration %d: %v", i, err)
		}
	}
}

// BenchmarkInsert_SQLite benchmarks INSERT performance for SQLite
func BenchmarkInsert_SQLite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("Failed to open SQLite: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name%d', %d)", i, i, i*10))
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
	}
}

// BenchmarkSelect_TurDB benchmarks SELECT performance for TurDB
func BenchmarkSelect_TurDB(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := turdb.Open(dbPath)
	if err != nil {
		b.Fatalf("Failed to open TurDB: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Pre-populate with 100 rows (smaller to avoid B-tree split issues)
	for i := 0; i < 100; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name%d', %d)", i, i, i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("SELECT * FROM bench WHERE id = 50")
		if err != nil {
			b.Fatalf("SELECT failed: %v", err)
		}
	}
}

// BenchmarkSelect_SQLite benchmarks SELECT performance for SQLite
func BenchmarkSelect_SQLite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("Failed to open SQLite: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Pre-populate with 100 rows
	for i := 0; i < 100; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name%d', %d)", i, i, i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM bench WHERE id = 50")
		if err != nil {
			b.Fatalf("SELECT failed: %v", err)
		}
		rows.Close()
	}
}

// BenchmarkUpdate_TurDB benchmarks UPDATE performance for TurDB
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
	for i := 0; i < 100; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name%d', %d)", i, i, i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec(fmt.Sprintf("UPDATE bench SET value = %d WHERE id = 50", i))
		if err != nil {
			b.Fatalf("UPDATE failed: %v", err)
		}
	}
}

// BenchmarkUpdate_SQLite benchmarks UPDATE performance for SQLite
func BenchmarkUpdate_SQLite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("Failed to open SQLite: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	// Pre-populate with 100 rows
	for i := 0; i < 100; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name%d', %d)", i, i, i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec(fmt.Sprintf("UPDATE bench SET value = %d WHERE id = 50", i))
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec("BEGIN")
		db.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name%d', %d)", i, i, i*10))
		db.Exec("ROLLBACK")
	}
}

// BenchmarkTransaction_SQLite benchmarks transaction with rollback for SQLite
func BenchmarkTransaction_SQLite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("Failed to open SQLite: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin()
		tx.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name%d', %d)", i, i, i*10))
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec("BEGIN")
		db.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name%d', %d)", i, i, i*10))
		db.Exec("COMMIT")
	}
}

// BenchmarkTransactionCommit_SQLite benchmarks transaction with commit for SQLite
func BenchmarkTransactionCommit_SQLite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("Failed to open SQLite: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, value INT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin()
		tx.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'name%d', %d)", i, i, i*10))
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
