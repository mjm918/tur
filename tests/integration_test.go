package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
