package executor

import (
	"strings"
	"testing"
)

// ============================================================================
// UNIQUE Constraint Tests
// ============================================================================

func TestExecutor_UniqueConstraint_Insert_RejectsDuplicates(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with UNIQUE constraint on email column
	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// First insert should succeed
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")
	if err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}

	// Second insert with same email should fail
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (2, 'alice@example.com')")
	if err == nil {
		t.Fatal("Expected UNIQUE constraint violation, but INSERT succeeded")
	}

	if !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Errorf("Error should mention UNIQUE violation, got: %v", err)
	}
}

func TestExecutor_UniqueConstraint_Insert_AllowsDifferentValues(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// First insert
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")
	if err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}

	// Second insert with different email should succeed
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
	if err != nil {
		t.Errorf("INSERT with different email should succeed, got: %v", err)
	}
}

func TestExecutor_UniqueConstraint_Insert_AllowsMultipleNulls(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Multiple NULL values should be allowed (SQL standard: NULLs are distinct)
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("First NULL INSERT failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (2, NULL)")
	if err != nil {
		t.Errorf("Second NULL INSERT should succeed (NULLs are distinct), got: %v", err)
	}
}

func TestExecutor_UniqueConstraint_Update_RejectsDuplicates(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert two users with different emails
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")
	if err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
	if err != nil {
		t.Fatalf("Second INSERT failed: %v", err)
	}

	// Update bob's email to alice's email should fail
	_, err = exec.Execute("UPDATE users SET email = 'alice@example.com' WHERE id = 2")
	if err == nil {
		t.Fatal("Expected UNIQUE constraint violation on UPDATE, but it succeeded")
	}

	if !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Errorf("Error should mention UNIQUE violation, got: %v", err)
	}
}

func TestExecutor_UniqueConstraint_Update_AllowsSameValue(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Updating to the same value should succeed (not a duplicate of another row)
	_, err = exec.Execute("UPDATE users SET email = 'alice@example.com' WHERE id = 1")
	if err != nil {
		t.Errorf("UPDATE to same value should succeed, got: %v", err)
	}
}

// ============================================================================
// NOT NULL Constraint Tests
// ============================================================================

func TestExecutor_NotNullConstraint_Insert_RejectsNull(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT NOT NULL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with NULL in NOT NULL column should fail
	_, err = exec.Execute("INSERT INTO users (id, name) VALUES (1, NULL)")
	if err == nil {
		t.Fatal("Expected NOT NULL constraint violation, but INSERT succeeded")
	}

	if !strings.Contains(strings.ToLower(err.Error()), "null") {
		t.Errorf("Error should mention NULL violation, got: %v", err)
	}
}

func TestExecutor_NotNullConstraint_Insert_AcceptsValue(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT NOT NULL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with non-NULL value should succeed
	_, err = exec.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Errorf("INSERT with non-NULL value should succeed, got: %v", err)
	}
}

func TestExecutor_NotNullConstraint_Update_RejectsNull(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Update to NULL should fail
	_, err = exec.Execute("UPDATE users SET name = NULL WHERE id = 1")
	if err == nil {
		t.Fatal("Expected NOT NULL constraint violation on UPDATE, but it succeeded")
	}

	if !strings.Contains(strings.ToLower(err.Error()), "null") {
		t.Errorf("Error should mention NULL violation, got: %v", err)
	}
}

func TestExecutor_NotNullConstraint_Insert_MissingColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT, name TEXT NOT NULL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert without specifying NOT NULL column should fail (implicit NULL)
	_, err = exec.Execute("INSERT INTO users (id) VALUES (1)")
	if err == nil {
		t.Fatal("Expected NOT NULL constraint violation for missing column, but INSERT succeeded")
	}

	if !strings.Contains(strings.ToLower(err.Error()), "null") {
		t.Errorf("Error should mention NULL violation, got: %v", err)
	}
}

// ============================================================================
// CHECK Constraint Tests
// ============================================================================

func TestExecutor_CheckConstraint_Insert_RejectsInvalid(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE products (id INT, price REAL CHECK(price > 0))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with invalid value (price <= 0) should fail
	_, err = exec.Execute("INSERT INTO products (id, price) VALUES (1, -10.0)")
	if err == nil {
		t.Fatal("Expected CHECK constraint violation, but INSERT succeeded")
	}

	if !strings.Contains(strings.ToLower(err.Error()), "check") {
		t.Errorf("Error should mention CHECK violation, got: %v", err)
	}
}

func TestExecutor_CheckConstraint_Insert_AcceptsValid(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE products (id INT, price REAL CHECK(price > 0))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with valid value should succeed
	_, err = exec.Execute("INSERT INTO products (id, price) VALUES (1, 19.99)")
	if err != nil {
		t.Errorf("INSERT with valid value should succeed, got: %v", err)
	}
}

func TestExecutor_CheckConstraint_Insert_AllowsNull(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE products (id INT, price REAL CHECK(price > 0))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert NULL should be allowed (CHECK is skipped for NULL per SQL standard)
	_, err = exec.Execute("INSERT INTO products (id, price) VALUES (1, NULL)")
	if err != nil {
		t.Errorf("INSERT with NULL should succeed (CHECK skipped for NULL), got: %v", err)
	}
}

func TestExecutor_CheckConstraint_Update_RejectsInvalid(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, price REAL CHECK(price > 0))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO products (id, price) VALUES (1, 19.99)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Update to invalid value should fail
	_, err = exec.Execute("UPDATE products SET price = -5.0 WHERE id = 1")
	if err == nil {
		t.Fatal("Expected CHECK constraint violation on UPDATE, but it succeeded")
	}

	if !strings.Contains(strings.ToLower(err.Error()), "check") {
		t.Errorf("Error should mention CHECK violation, got: %v", err)
	}
}

func TestExecutor_CheckConstraint_MultiColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Table-level CHECK constraint involving multiple columns
	_, err := exec.Execute("CREATE TABLE orders (id INT, min_qty INT, max_qty INT, CHECK(max_qty >= min_qty))")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Valid: max_qty > min_qty
	_, err = exec.Execute("INSERT INTO orders (id, min_qty, max_qty) VALUES (1, 5, 10)")
	if err != nil {
		t.Errorf("INSERT with valid values should succeed, got: %v", err)
	}

	// Invalid: max_qty < min_qty
	_, err = exec.Execute("INSERT INTO orders (id, min_qty, max_qty) VALUES (2, 10, 5)")
	if err == nil {
		t.Fatal("Expected CHECK constraint violation for max_qty < min_qty, but INSERT succeeded")
	}
}

// ============================================================================
// DEFAULT Constraint Tests
// ============================================================================

func TestExecutor_DefaultConstraint_Insert_AppliesDefault(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT, status TEXT DEFAULT 'active')")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert without specifying status column - should use default
	_, err = exec.Execute("INSERT INTO users (id) VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify default was applied
	result, err := exec.Execute("SELECT status FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	status := result.Rows[0][0]
	if status.Text() != "active" {
		t.Errorf("Expected default status 'active', got '%s'", status.Text())
	}
}

func TestExecutor_DefaultConstraint_Insert_ExplicitValueOverridesDefault(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT, status TEXT DEFAULT 'active')")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with explicit value - should override default
	_, err = exec.Execute("INSERT INTO users (id, status) VALUES (1, 'inactive')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify explicit value was used
	result, err := exec.Execute("SELECT status FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	status := result.Rows[0][0]
	if status.Text() != "inactive" {
		t.Errorf("Expected explicit status 'inactive', got '%s'", status.Text())
	}
}

func TestExecutor_DefaultConstraint_Insert_ExplicitNullOverridesDefault(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT, status TEXT DEFAULT 'active')")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert with explicit NULL - should be NULL, not default
	_, err = exec.Execute("INSERT INTO users (id, status) VALUES (1, NULL)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Verify NULL was stored
	result, err := exec.Execute("SELECT status FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	status := result.Rows[0][0]
	if !status.IsNull() {
		t.Errorf("Expected NULL status, got '%v'", status)
	}
}

func TestExecutor_DefaultConstraint_IntegerDefault(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE products (id INT, quantity INT DEFAULT 0)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("INSERT INTO products (id) VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	result, err := exec.Execute("SELECT quantity FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	qty := result.Rows[0][0]
	if qty.Int() != 0 {
		t.Errorf("Expected default quantity 0, got %d", qty.Int())
	}
}

// ============================================================================
// Combined Constraint Tests
// ============================================================================

func TestExecutor_Constraints_NotNullWithDefault(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Column with both NOT NULL and DEFAULT - missing column should use default
	_, err := exec.Execute("CREATE TABLE users (id INT, status TEXT NOT NULL DEFAULT 'active')")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert without specifying status - should use default and pass NOT NULL
	_, err = exec.Execute("INSERT INTO users (id) VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT should succeed with NOT NULL + DEFAULT, got: %v", err)
	}

	result, err := exec.Execute("SELECT status FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if result.Rows[0][0].Text() != "active" {
		t.Errorf("Expected 'active', got '%s'", result.Rows[0][0].Text())
	}
}

func TestExecutor_Constraints_UniqueNotNull(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute("CREATE TABLE users (id INT, email TEXT UNIQUE NOT NULL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// NULL should fail due to NOT NULL
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (1, NULL)")
	if err == nil {
		t.Fatal("Expected NOT NULL violation, but INSERT succeeded")
	}

	// First valid insert should succeed
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")
	if err != nil {
		t.Fatalf("First INSERT failed: %v", err)
	}

	// Duplicate should fail due to UNIQUE
	_, err = exec.Execute("INSERT INTO users (id, email) VALUES (2, 'alice@example.com')")
	if err == nil {
		t.Fatal("Expected UNIQUE violation, but INSERT succeeded")
	}
}
