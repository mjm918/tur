package tests

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"tur/pkg/turdb"
)

// TestFullFeatureSet tests the complete feature set in a single session
func TestFullFeatureSet(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := turdb.Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	t.Log("=== Testing Full Feature Set ===\n")

	// Test 1: CREATE TABLE
	t.Log("1. Creating tables...")
	_, err = db.Exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = db.Exec("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount REAL, status TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE orders failed: %v", err)
	}
	t.Log("✓ Tables created")

	// Test 2: INSERT
	t.Log("\n2. Inserting data...")
	users := []struct {
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

	for _, u := range users {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO users VALUES (%d, '%s', '%s', %d)",
			u.id, u.name, u.email, u.age))
		if err != nil {
			t.Fatalf("INSERT users failed: %v", err)
		}
	}

	orders := []struct {
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

	for _, o := range orders {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %f, '%s')",
			o.id, o.userID, o.amount, o.status))
		if err != nil {
			t.Fatalf("INSERT orders failed: %v", err)
		}
	}
	t.Log("✓ Data inserted")

	// Test 3: SELECT with WHERE
	t.Log("\n3. Testing SELECT with WHERE...")
	result, err := db.Exec("SELECT name, age FROM users WHERE age > 30")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	t.Logf("✓ Found %d users with age > 30", len(result.Rows))

	// Test 4: JOIN
	t.Log("\n4. Testing JOIN...")
	result, err = db.Exec("SELECT u.name, o.amount, o.status FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'completed'")
	if err != nil {
		t.Fatalf("JOIN failed: %v", err)
	}
	t.Logf("✓ JOIN returned %d rows", len(result.Rows))

	// Test 5: CREATE INDEX
	t.Log("\n5. Creating index...")
	_, err = db.Exec("CREATE INDEX idx_user_age ON users(age)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}
	t.Log("✓ Index created")

	// Test 6: UPDATE
	t.Log("\n6. Testing UPDATE...")
	_, err = db.Exec("UPDATE users SET age = 29 WHERE name = 'Alice Smith'")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	result, err = db.Exec("SELECT age FROM users WHERE name = 'Alice Smith'")
	if err != nil {
		t.Fatalf("SELECT after UPDATE failed: %v", err)
	}

	if len(result.Rows) > 0 {
		age := result.Rows[0][0].(int64)
		if age == 29 {
			t.Log("✓ UPDATE successful, age is now 29")
		} else {
			t.Errorf("UPDATE failed: age = %d, want 29", age)
		}
	}

	// Test 7: DELETE
	t.Log("\n7. Testing DELETE...")
	_, err = db.Exec("DELETE FROM orders WHERE status = 'pending'")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	result, err = db.Exec("SELECT COUNT(*) FROM orders WHERE status = 'pending'")
	if err != nil {
		t.Fatalf("SELECT after DELETE failed: %v", err)
	}

	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count == 0 {
			t.Log("✓ DELETE successful, pending orders removed")
		} else {
			t.Errorf("DELETE failed: still have %d pending orders", count)
		}
	}

	// Test 8: EXPLAIN (bytecode)
	t.Log("\n8. Testing EXPLAIN (bytecode)...")
	result, err = db.Exec("EXPLAIN SELECT * FROM users WHERE age > 25")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}

	if len(result.Rows) > 0 {
		t.Logf("✓ EXPLAIN returned %d opcodes", len(result.Rows))
		if len(result.Columns) >= 2 {
			t.Logf("  First opcode: %v", result.Rows[0][1])
		}
	}

	// Test 9: EXPLAIN QUERY PLAN
	t.Log("\n9. Testing EXPLAIN QUERY PLAN...")
	result, err = db.Exec("EXPLAIN QUERY PLAN SELECT * FROM users WHERE age > 25")
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}

	if len(result.Rows) > 0 {
		t.Logf("✓ EXPLAIN QUERY PLAN returned %d plan nodes", len(result.Rows))
	}

	// Test 10: EXPLAIN ANALYZE (NEW FEATURE!)
	t.Log("\n10. Testing EXPLAIN ANALYZE (NEW FEATURE!)...")
	result, err = db.Exec("EXPLAIN ANALYZE SELECT * FROM users WHERE age > 25")
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE failed: %v", err)
	}

	if len(result.Rows) < 1 {
		t.Fatal("EXPLAIN ANALYZE returned no results")
	}

	// Verify execution information
	foundTime := false
	foundActual := false
	foundMemory := false

	for _, row := range result.Rows {
		for _, col := range row {
			text := strings.ToLower(fmt.Sprintf("%v", col))
			if strings.Contains(text, "time") {
				foundTime = true
			}
			if strings.Contains(text, "actual") {
				foundActual = true
			}
			if strings.Contains(text, "memory") || strings.Contains(text, "bytes") {
				foundMemory = true
			}
		}
	}

	t.Logf("✓ EXPLAIN ANALYZE returned %d rows with statistics:", len(result.Rows))
	if foundTime {
		t.Log("  ✓ Contains timing information")
	}
	if foundActual {
		t.Log("  ✓ Contains actual row counts")
	}
	if foundMemory {
		t.Log("  ✓ Contains memory usage")
	}

	if !foundTime && !foundActual {
		t.Error("EXPLAIN ANALYZE missing runtime statistics")
	}

	// Test 11: Transactions
	t.Log("\n11. Testing transactions...")
	_, err = db.Exec("BEGIN")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO users VALUES (999, 'Test User', 'test@example.com', 99)")
	if err != nil {
		t.Fatalf("INSERT in transaction failed: %v", err)
	}

	_, err = db.Exec("ROLLBACK")
	if err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	result, err = db.Exec("SELECT COUNT(*) FROM users WHERE id = 999")
	if err != nil {
		t.Fatalf("SELECT after ROLLBACK failed: %v", err)
	}

	if len(result.Rows) > 0 {
		count := result.Rows[0][0].(int64)
		if count == 0 {
			t.Log("✓ ROLLBACK successful, test user not found")
		} else {
			t.Error("ROLLBACK failed, test user still exists")
		}
	}

	t.Log("\n✅ All feature tests passed!")
	t.Logf("\nDatabase file: %s", dbPath)
}
