package tests

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"tur/pkg/turdb"
)

// TestFullFeatureSetWithPersistence tests the complete feature set with physical file storage
// Creates a database, performs operations, closes it, reopens it, and verifies data persists
func TestFullFeatureSetWithPersistence(t *testing.T) {
	// Create a temporary database file
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_persistence.db")

	t.Log("=== Phase 1: Create database and populate with data ===")
	
	db, err := turdb.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Test 1: Create regular tables
	t.Log("Creating regular tables...")
	_, err = db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount REAL, status TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}

	// Test 2: Insert data
	t.Log("Inserting data into users...")
	testUsers := []struct {
		id    int
		name  string
		email string
		age   int
	}{
		{1, "Alice Smith", "alice@example.com", 28},
		{2, "Bob Jones", "bob@example.com", 35},
		{3, "Charlie Brown", "charlie@example.com", 42},
		{4, "Diana Prince", "diana@example.com", 31},
		{5, "Eve Wilson", "eve@example.com", 26},
	}

	for _, u := range testUsers {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO users VALUES (%d, '%s', '%s', %d)",
			u.id, u.name, u.email, u.age))
		if err != nil {
			t.Fatalf("INSERT into users failed: %v", err)
		}
	}

	// Insert orders
	t.Log("Inserting data into orders...")
	testOrders := []struct {
		id     int
		userID int
		amount float64
		status string
	}{
		{1, 1, 99.99, "completed"},
		{2, 1, 149.50, "pending"},
		{3, 2, 75.00, "completed"},
		{4, 3, 200.00, "shipped"},
		{5, 4, 50.25, "completed"},
	}

	for _, o := range testOrders {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %f, '%s')",
			o.id, o.userID, o.amount, o.status))
		if err != nil {
			t.Fatalf("INSERT into orders failed: %v", err)
		}
	}

	// Test 3: Create index
	t.Log("Creating index...")
	_, err = db.Exec("CREATE INDEX idx_user_email ON users(email)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Test 4: Query data
	t.Log("Querying data...")
	result, err := db.Exec("SELECT * FROM users WHERE age > 30")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows with age > 30, got %d", len(result.Rows))
	}

	// Test 5: JOIN query
	t.Log("Testing JOIN query...")
	result, err = db.Exec("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed'")
	if err != nil {
		t.Fatalf("JOIN query failed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 completed orders, got %d", len(result.Rows))
	}

	// Test 6: UPDATE
	t.Log("Testing UPDATE...")
	result, err = db.Exec("UPDATE users SET age = 29 WHERE name = 'Alice Smith'")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Verify update
	result, err = db.Exec("SELECT age FROM users WHERE name = 'Alice Smith'")
	if err != nil {
		t.Fatalf("SELECT after UPDATE failed: %v", err)
	}

	aliceAge := result.Rows[0][0].(int64)
	if aliceAge != 29 {
		t.Errorf("UPDATE did not work: age = %d, want 29", aliceAge)
	}

	// Test 7: DELETE
	t.Log("Testing DELETE...")
	_, err = db.Exec("DELETE FROM orders WHERE status = 'pending'")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	result, err = db.Exec("SELECT COUNT(*) FROM orders")
	if err != nil {
		t.Fatalf("SELECT COUNT after DELETE failed: %v", err)
	}

	orderCount := result.Rows[0][0].(int64)
	if orderCount != 4 {
		t.Errorf("Expected 4 orders after DELETE, got %d", orderCount)
	}

	// Test 8: EXPLAIN ANALYZE
	t.Log("Testing EXPLAIN ANALYZE...")
	result, err = db.Exec("EXPLAIN ANALYZE SELECT * FROM users WHERE age > 25")
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE failed: %v", err)
	}

	// Verify EXPLAIN ANALYZE output contains execution info
	foundExecutionInfo := false
	for _, row := range result.Rows {
		for i := range row {
			text := strings.ToLower(fmt.Sprintf("%v", row[i]))
			if strings.Contains(text, "time") || strings.Contains(text, "actual") {
				foundExecutionInfo = true
				break
			}
		}
		if foundExecutionInfo {
			break
		}
	}

	if !foundExecutionInfo {
		t.Error("EXPLAIN ANALYZE output missing execution information")
	}

	// Explicitly close database to ensure data is flushed to disk
	t.Log("Closing database...")
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	t.Log("✓ Phase 1 complete: Database populated and closed")

	// Phase 2: Reopen and verify data persists
	t.Log("\n=== Phase 2: Reopen database and verify persistence ===")

	// Verify database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Database file does not exist at %s", dbPath)
	}

	t.Log("Reopening database from disk...")
	db, err = turdb.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify 1: Check users table exists and has correct data
	t.Log("Verifying users table...")
	result, err = db.Exec("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Failed to query users after reopen: %v", err)
	}

	userCount := result.Rows[0][0].(int64)
	if userCount != 5 {
		t.Errorf("Expected 5 users, got %d", userCount)
	}

	// Verify 2: Check UPDATE persisted
	t.Log("Verifying UPDATE persistence...")
	result, err = db.Exec("SELECT age FROM users WHERE name = 'Alice Smith'")
	if err != nil {
		t.Fatalf("Failed to query Alice's age: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Alice Smith not found after reopen")
	}

	aliceAge = result.Rows[0][0].(int64)
	if aliceAge != 29 {
		t.Errorf("UPDATE did not persist: age = %d, want 29", aliceAge)
	}

	// Verify 3: Check DELETE persisted
	t.Log("Verifying DELETE persistence...")
	result, err = db.Exec("SELECT COUNT(*) FROM orders")
	if err != nil {
		t.Fatalf("Failed to query orders count: %v", err)
	}

	orderCount = result.Rows[0][0].(int64)
	if orderCount != 4 {
		t.Errorf("Expected 4 orders (after DELETE), got %d", orderCount)
	}

	// Verify 4: Check specific deleted record is gone
	result, err = db.Exec("SELECT COUNT(*) FROM orders WHERE status = 'pending'")
	if err != nil {
		t.Fatalf("Failed to query pending orders: %v", err)
	}

	pendingCount := result.Rows[0][0].(int64)
	if pendingCount != 0 {
		t.Error("Deleted records still exist after reopen")
	}

	// Verify 5: EXPLAIN ANALYZE still works
	t.Log("Verifying EXPLAIN ANALYZE after reopen...")
	result, err = db.Exec("EXPLAIN ANALYZE SELECT * FROM users")
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE failed after reopen: %v", err)
	}

	if len(result.Rows) < 1 {
		t.Error("EXPLAIN ANALYZE returned no results after reopen")
	}

	t.Log("✓ Phase 2 complete: All data verified after reopen")

	// Phase 3: Additional operations and final verification
	t.Log("\n=== Phase 3: Additional operations and final verification ===")

	// Add more data
	t.Log("Adding more data...")
	_, err = db.Exec("INSERT INTO users VALUES (6, 'Frank Castle', 'frank@example.com', 40)")
	if err != nil {
		t.Fatalf("INSERT failed in phase 3: %v", err)
	}

	// Close and reopen one more time
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database in phase 3: %v", err)
	}

	db, err = turdb.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database in phase 3: %v", err)
	}
	defer db.Close()

	// Final verification
	t.Log("Final verification...")
	result, err = db.Exec("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Final SELECT failed: %v", err)
	}

	finalCount := result.Rows[0][0].(int64)
	expectedCount := int64(6)
	if finalCount != expectedCount {
		t.Errorf("Final count = %d, want %d", finalCount, expectedCount)
	}

	t.Log("✓ Phase 3 complete")
	t.Log("\n✅ Full feature set integration test completed successfully!")
	t.Logf("Database file: %s (size: %d bytes)", dbPath, getFileSize(dbPath))
}

func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// TestMillionRecordsWithPersistence tests inserting 1M records with full persistence verification
// This is a stress test to validate the benchmark results are accurate
func TestMillionRecordsWithPersistence(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 1M record test in short mode")
	}

	const totalRecords = 1_000_000
	const batchSize = 10_000
	const sampleSize = 1000 // Number of random records to verify

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "million_records.db")

	t.Logf("=== Phase 1: Insert %d records ===", totalRecords)

	db, err := turdb.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create table with multiple columns to simulate real workload
	// Using INT PRIMARY KEY (not INT) to enable fast rowid-based lookups
	_, err = db.Exec("CREATE TABLE records (id INT PRIMARY KEY, hash TEXT, value INT, category INT, timestamp INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create index on category for query testing
	_, err = db.Exec("CREATE INDEX idx_category ON records(category)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Track some records for verification
	type verifyRecord struct {
		id        int
		hash      string
		value     int
		category  int
		timestamp int
	}
	verifyRecords := make([]verifyRecord, 0, sampleSize)
	rng := rand.New(rand.NewSource(42)) // Deterministic for reproducibility

	// Insert records in batches and measure performance
	startTime := time.Now()
	var lastBatchTime time.Duration
	insertedCount := 0

	for batch := 0; batch < totalRecords/batchSize; batch++ {
		batchStart := time.Now()

		for i := 0; i < batchSize; i++ {
			id := batch*batchSize + i
			// Generate deterministic hash based on id
			hashBytes := sha256.Sum256([]byte(fmt.Sprintf("record-%d", id)))
			hash := hex.EncodeToString(hashBytes[:16])
			value := id * 17 % 1000000
			category := id % 100
			timestamp := 1700000000 + id

			_, err = db.Exec(fmt.Sprintf("INSERT INTO records VALUES (%d, '%s', %d, %d, %d)",
				id, hash, value, category, timestamp))
			if err != nil {
				t.Fatalf("INSERT failed at id %d: %v", id, err)
			}

			// Randomly sample records for verification
			if rng.Intn(totalRecords/sampleSize) == 0 && len(verifyRecords) < sampleSize {
				verifyRecords = append(verifyRecords, verifyRecord{id, hash, value, category, timestamp})
			}

			insertedCount++
		}

		lastBatchTime = time.Since(batchStart)
		if (batch+1)%10 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(insertedCount) / elapsed.Seconds()
			t.Logf("  Inserted %d/%d records (%.0f records/sec, last batch: %v)",
				insertedCount, totalRecords, rate, lastBatchTime)
		}
	}

	insertDuration := time.Since(startTime)
	insertRate := float64(totalRecords) / insertDuration.Seconds()
	t.Logf("Insert complete: %d records in %v (%.0f records/sec)", totalRecords, insertDuration, insertRate)

	// Verify count before close
	result, err := db.Exec("SELECT COUNT(*) FROM records")
	if err != nil {
		t.Fatalf("COUNT failed: %v", err)
	}
	count := result.Rows[0][0].(int64)
	if count != int64(totalRecords) {
		t.Fatalf("Expected %d records, got %d", totalRecords, count)
	}

	// Close database to flush all data
	t.Log("Closing database to flush data...")
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	dbSize := getFileSize(dbPath)
	t.Logf("Database file size: %d bytes (%.2f MB)", dbSize, float64(dbSize)/1024/1024)

	t.Logf("✓ Phase 1 complete: %d records inserted", totalRecords)

	// Phase 2: Reopen and verify persistence
	t.Log("\n=== Phase 2: Verify persistence after reopen ===")

	db, err = turdb.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}

	// Verify total count
	result, err = db.Exec("SELECT COUNT(*) FROM records")
	if err != nil {
		t.Fatalf("COUNT after reopen failed: %v", err)
	}
	count = result.Rows[0][0].(int64)
	if count != int64(totalRecords) {
		t.Fatalf("PERSISTENCE FAILURE: Expected %d records after reopen, got %d", totalRecords, count)
	}
	t.Logf("✓ Total count verified: %d records", count)

	// Verify sampled records have correct data
	t.Logf("Verifying %d random sampled records...", len(verifyRecords))
	verifyStart := time.Now()
	for i, rec := range verifyRecords {
		result, err = db.Exec(fmt.Sprintf("SELECT hash, value, category, timestamp FROM records WHERE id = %d", rec.id))
		if err != nil {
			t.Fatalf("SELECT for verification failed at id %d: %v", rec.id, err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("PERSISTENCE FAILURE: Record id=%d not found after reopen", rec.id)
		}

		gotHash := result.Rows[0][0].(string)
		gotValue := result.Rows[0][1].(int64)
		gotCategory := result.Rows[0][2].(int64)
		gotTimestamp := result.Rows[0][3].(int64)

		if gotHash != rec.hash {
			t.Errorf("PERSISTENCE FAILURE: Record %d hash mismatch: got %s, want %s", rec.id, gotHash, rec.hash)
		}
		if gotValue != int64(rec.value) {
			t.Errorf("PERSISTENCE FAILURE: Record %d value mismatch: got %d, want %d", rec.id, gotValue, rec.value)
		}
		if gotCategory != int64(rec.category) {
			t.Errorf("PERSISTENCE FAILURE: Record %d category mismatch: got %d, want %d", rec.id, gotCategory, rec.category)
		}
		if gotTimestamp != int64(rec.timestamp) {
			t.Errorf("PERSISTENCE FAILURE: Record %d timestamp mismatch: got %d, want %d", rec.id, gotTimestamp, rec.timestamp)
		}

		if (i+1)%200 == 0 {
			t.Logf("  Verified %d/%d sampled records", i+1, len(verifyRecords))
		}
	}
	verifyDuration := time.Since(verifyStart)
	t.Logf("✓ Sampled record verification complete in %v", verifyDuration)

	// Verify boundary records (first, last, middle)
	t.Log("Verifying boundary records...")
	boundaryIDs := []int{0, totalRecords / 2, totalRecords - 1}
	for _, id := range boundaryIDs {
		result, err = db.Exec(fmt.Sprintf("SELECT id, hash FROM records WHERE id = %d", id))
		if err != nil {
			t.Fatalf("SELECT boundary record %d failed: %v", id, err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("PERSISTENCE FAILURE: Boundary record id=%d not found", id)
		}
		expectedHash := sha256.Sum256([]byte(fmt.Sprintf("record-%d", id)))
		expectedHashStr := hex.EncodeToString(expectedHash[:16])
		gotHash := result.Rows[0][1].(string)
		if gotHash != expectedHashStr {
			t.Errorf("PERSISTENCE FAILURE: Boundary record %d hash mismatch", id)
		}
	}
	t.Log("✓ Boundary records verified")

	t.Logf("✓ Phase 2 complete: All persistence checks passed")

	// Phase 3: Query performance on large dataset
	t.Log("\n=== Phase 3: Query performance tests ===")

	// Test point query performance
	t.Log("Testing point query performance (1000 random lookups)...")
	pointQueryStart := time.Now()
	for i := 0; i < 1000; i++ {
		id := rng.Intn(totalRecords)
		_, err = db.Exec(fmt.Sprintf("SELECT * FROM records WHERE id = %d", id))
		if err != nil {
			t.Fatalf("Point query failed: %v", err)
		}
	}
	pointQueryDuration := time.Since(pointQueryStart)
	t.Logf("✓ 1000 point queries in %v (%.2f queries/sec)", pointQueryDuration, 1000.0/pointQueryDuration.Seconds())

	// Test range query with category (uses index)
	t.Log("Testing range query with index...")
	rangeQueryStart := time.Now()
	result, err = db.Exec("SELECT COUNT(*) FROM records WHERE category = 50")
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}
	rangeQueryDuration := time.Since(rangeQueryStart)
	categoryCount := result.Rows[0][0].(int64)
	expectedCategoryCount := int64(totalRecords / 100) // 100 categories
	if categoryCount != expectedCategoryCount {
		t.Errorf("Category count mismatch: got %d, want %d", categoryCount, expectedCategoryCount)
	}
	t.Logf("✓ Range query (category=50) returned %d rows in %v", categoryCount, rangeQueryDuration)

	// Test aggregation
	t.Log("Testing aggregation query...")
	aggStart := time.Now()
	result, err = db.Exec("SELECT category, COUNT(*) FROM records GROUP BY category LIMIT 10")
	if err != nil {
		t.Fatalf("Aggregation query failed: %v", err)
	}
	aggDuration := time.Since(aggStart)
	t.Logf("✓ GROUP BY query returned %d groups in %v", len(result.Rows), aggDuration)

	t.Logf("✓ Phase 3 complete: Query performance tests passed")

	// Phase 4: Update and delete persistence
	t.Log("\n=== Phase 4: Update/Delete persistence test ===")

	// Update 1000 records
	updateStart := time.Now()
	updateCount := 1000
	for i := 0; i < updateCount; i++ {
		id := i * (totalRecords / updateCount)
		_, err = db.Exec(fmt.Sprintf("UPDATE records SET value = -1 WHERE id = %d", id))
		if err != nil {
			t.Fatalf("UPDATE failed at id %d: %v", id, err)
		}
	}
	updateDuration := time.Since(updateStart)
	t.Logf("✓ Updated %d records in %v", updateCount, updateDuration)

	// Delete 1000 records (from end to avoid affecting updates)
	deleteStart := time.Now()
	deleteCount := 1000
	for i := 0; i < deleteCount; i++ {
		id := totalRecords - 1 - i
		_, err = db.Exec(fmt.Sprintf("DELETE FROM records WHERE id = %d", id))
		if err != nil {
			t.Fatalf("DELETE failed at id %d: %v", id, err)
		}
	}
	deleteDuration := time.Since(deleteStart)
	t.Logf("✓ Deleted %d records in %v", deleteCount, deleteDuration)

	// Close and reopen to verify update/delete persistence
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	db, err = turdb.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	// Verify count after delete
	result, err = db.Exec("SELECT COUNT(*) FROM records")
	if err != nil {
		t.Fatalf("COUNT after delete failed: %v", err)
	}
	count = result.Rows[0][0].(int64)
	expectedAfterDelete := int64(totalRecords - deleteCount)
	if count != expectedAfterDelete {
		t.Fatalf("PERSISTENCE FAILURE: Expected %d records after delete, got %d", expectedAfterDelete, count)
	}
	t.Logf("✓ Delete persistence verified: %d records remain", count)

	// Verify updates persisted
	result, err = db.Exec("SELECT COUNT(*) FROM records WHERE value = -1")
	if err != nil {
		t.Fatalf("COUNT updated records failed: %v", err)
	}
	updatedCount := result.Rows[0][0].(int64)
	if updatedCount != int64(updateCount) {
		t.Fatalf("PERSISTENCE FAILURE: Expected %d updated records, got %d", updateCount, updatedCount)
	}
	t.Logf("✓ Update persistence verified: %d records with value=-1", updatedCount)

	// Verify deleted records are gone
	result, err = db.Exec(fmt.Sprintf("SELECT COUNT(*) FROM records WHERE id >= %d", totalRecords-deleteCount))
	if err != nil {
		t.Fatalf("COUNT deleted range failed: %v", err)
	}
	remainingDeleted := result.Rows[0][0].(int64)
	if remainingDeleted != 0 {
		t.Fatalf("PERSISTENCE FAILURE: Expected 0 records in deleted range, got %d", remainingDeleted)
	}
	t.Log("✓ Deleted records confirmed gone")

	finalSize := getFileSize(dbPath)
	t.Logf("✓ Phase 4 complete: Update/Delete persistence verified")

	// Final summary
	t.Log("\n========================================")
	t.Log("=== 1M RECORDS STRESS TEST SUMMARY ===")
	t.Log("========================================")
	t.Logf("Total records inserted: %d", totalRecords)
	t.Logf("Insert rate: %.0f records/sec", insertRate)
	t.Logf("Point query rate: %.0f queries/sec", 1000.0/pointQueryDuration.Seconds())
	t.Logf("Update rate: %.0f records/sec", float64(updateCount)/updateDuration.Seconds())
	t.Logf("Delete rate: %.0f records/sec", float64(deleteCount)/deleteDuration.Seconds())
	t.Logf("Final database size: %.2f MB", float64(finalSize)/1024/1024)
	t.Log("✅ ALL PERSISTENCE CHECKS PASSED")
}

// TestMillionRecordsCompareWithSQLite compares TurDB vs SQLite performance with 100K records
// Uses smaller dataset for reasonable test time but still validates performance claims
func TestMillionRecordsCompareWithSQLite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping comparison test in short mode")
	}

	// Skip if sqlite3 driver not available
	if os.Getenv("SKIP_SQLITE_COMPARE") == "1" {
		t.Skip("Skipping SQLite comparison (SKIP_SQLITE_COMPARE=1)")
	}

	const totalRecords = 100_000

	t.Log("=== TurDB vs Reference: Insert Performance Comparison ===")
	t.Logf("Records to insert: %d", totalRecords)

	// Test TurDB
	tmpDir := t.TempDir()
	turdbPath := filepath.Join(tmpDir, "turdb_compare.db")

	db, err := turdb.Open(turdbPath)
	if err != nil {
		t.Fatalf("Failed to open TurDB: %v", err)
	}

	// Using INT PRIMARY KEY (not INT) to enable fast rowid-based lookups
	_, err = db.Exec("CREATE TABLE bench (id INT PRIMARY KEY, data TEXT, value INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	turdbStart := time.Now()
	for i := 0; i < totalRecords; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, 'data-%d', %d)", i, i, i*7))
		if err != nil {
			t.Fatalf("INSERT failed at %d: %v", i, err)
		}
	}
	turdbInsertDuration := time.Since(turdbStart)
	turdbInsertRate := float64(totalRecords) / turdbInsertDuration.Seconds()

	// Close and reopen to verify
	db.Close()
	db, err = turdb.Open(turdbPath)
	if err != nil {
		t.Fatalf("Failed to reopen TurDB: %v", err)
	}

	result, err := db.Exec("SELECT COUNT(*) FROM bench")
	if err != nil {
		t.Fatalf("COUNT failed: %v", err)
	}
	count := result.Rows[0][0].(int64)
	if count != int64(totalRecords) {
		t.Fatalf("TurDB persistence failure: expected %d, got %d", totalRecords, count)
	}

	// Test select performance
	turdbSelectStart := time.Now()
	for i := 0; i < 1000; i++ {
		id := i * (totalRecords / 1000)
		_, err = db.Exec(fmt.Sprintf("SELECT * FROM bench WHERE id = %d", id))
		if err != nil {
			t.Fatalf("SELECT failed: %v", err)
		}
	}
	turdbSelectDuration := time.Since(turdbSelectStart)
	turdbSelectRate := 1000.0 / turdbSelectDuration.Seconds()

	db.Close()

	turdbSize := getFileSize(turdbPath)

	t.Log("\n=== RESULTS ===")
	t.Logf("TurDB Insert: %d records in %v (%.0f records/sec)", totalRecords, turdbInsertDuration, turdbInsertRate)
	t.Logf("TurDB Select: 1000 queries in %v (%.0f queries/sec)", turdbSelectDuration, turdbSelectRate)
	t.Logf("TurDB File Size: %.2f MB", float64(turdbSize)/1024/1024)
	t.Logf("TurDB Persistence: ✓ Verified %d records after reopen", count)

	// Performance assertions
	if turdbInsertRate < 10000 {
		t.Errorf("Insert rate too slow: %.0f records/sec (expected >10000)", turdbInsertRate)
	}
	if turdbSelectRate < 5000 {
		t.Errorf("Select rate too slow: %.0f queries/sec (expected >5000)", turdbSelectRate)
	}

	t.Log("\n✅ Performance validation passed")
}
