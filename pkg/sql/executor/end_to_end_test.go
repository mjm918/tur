package executor

import (
	"os"
	"testing"
	"tur/pkg/pager"
	// "tur/pkg/types"
)

func TestExecutor_Join_EndToEnd(t *testing.T) {
	// Setup
	tmpFile := "test_join.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	// p.Close() is called by exec.Close()

	exec := New(p)
	defer exec.Close()

	// 1. Create tables
	sql := `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`
	if _, err := exec.Execute(sql); err != nil {
		t.Fatalf("create users failed: %v", err)
	}

	sql = `CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT);`
	if _, err := exec.Execute(sql); err != nil {
		t.Fatalf("create orders failed: %v", err)
	}

	// 2. Insert data
	// Users: (1, Alice), (2, Bob), (3, Charlie)
	mustExec(t, exec, "INSERT INTO users VALUES (1, 'Alice');")
	mustExec(t, exec, "INSERT INTO users VALUES (2, 'Bob');")
	mustExec(t, exec, "INSERT INTO users VALUES (3, 'Charlie');") // No orders

	// Orders: (101, 1, 50), (102, 1, 20), (103, 2, 100)
	mustExec(t, exec, "INSERT INTO orders VALUES (101, 1, 50);")
	mustExec(t, exec, "INSERT INTO orders VALUES (102, 1, 20);")
	mustExec(t, exec, "INSERT INTO orders VALUES (103, 2, 100);")

	// 3. Select Join
	// SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id
	// Expected: (Alice, 50), (Alice, 20), (Bob, 100)

	sql = `SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id`
	res, err := exec.Execute(sql)
	if err != nil {
		t.Fatalf("select join failed: %v", err)
	}

	if len(res.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(res.Rows))
	}

	foundAlice50 := false
	foundAlice20 := false
	foundBob100 := false

	for _, row := range res.Rows {
		if len(row) < 2 {
			t.Errorf("expected 2 columns, got %d", len(row))
			continue
		}
		name := row[0].Text()
		amount := row[1].Int()

		if name == "Alice" && amount == 50 {
			foundAlice50 = true
		}
		if name == "Alice" && amount == 20 {
			foundAlice20 = true
		}
		if name == "Bob" && amount == 100 {
			foundBob100 = true
		}
	}

	if !foundAlice50 {
		t.Error("missing Alice 50")
	}
	if !foundAlice20 {
		t.Error("missing Alice 20")
	}
	if !foundBob100 {
		t.Error("missing Bob 100")
	}
}

func mustExec(t *testing.T, exec *Executor, sql string) {
	if _, err := exec.Execute(sql); err != nil {
		t.Fatalf("exec failed (%s): %v", sql, err)
	}
}

func TestExecutor_BacktickEscapedKeywords(t *testing.T) {
	// Test that reserved keywords can be used as identifiers when backtick-escaped
	tmpFile := "test_backtick.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create table with reserved keyword names (using backticks)
	sql := "CREATE TABLE `index` (`order` INT PRIMARY KEY, `select` TEXT, `from` INT)"
	if _, err := exec.Execute(sql); err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	// Insert data using backtick-escaped column names
	sql = "INSERT INTO `index` (`order`, `select`, `from`) VALUES (1, 'hello', 100)"
	if _, err := exec.Execute(sql); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	sql = "INSERT INTO `index` (`order`, `select`, `from`) VALUES (2, 'world', 200)"
	if _, err := exec.Execute(sql); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Select using backtick-escaped identifiers
	sql = "SELECT `order`, `select`, `from` FROM `index`"
	res, err := exec.Execute(sql)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}

	if len(res.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(res.Rows))
	}

	// Verify first row
	if res.Rows[0][0].Int() != 1 {
		t.Errorf("row[0][0] = %d, want 1", res.Rows[0][0].Int())
	}
	if res.Rows[0][1].Text() != "hello" {
		t.Errorf("row[0][1] = %q, want 'hello'", res.Rows[0][1].Text())
	}
	if res.Rows[0][2].Int() != 100 {
		t.Errorf("row[0][2] = %d, want 100", res.Rows[0][2].Int())
	}

	// Update using backtick-escaped identifiers
	sql = "UPDATE `index` SET `select` = 'updated' WHERE `order` = 1"
	if _, err := exec.Execute(sql); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// Verify update
	sql = "SELECT `select` FROM `index` WHERE `order` = 1"
	res, err = exec.Execute(sql)
	if err != nil {
		t.Fatalf("select after update failed: %v", err)
	}

	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if res.Rows[0][0].Text() != "updated" {
		t.Errorf("after update: got %q, want 'updated'", res.Rows[0][0].Text())
	}

	// Delete using backtick-escaped identifiers
	sql = "DELETE FROM `index` WHERE `order` = 2"
	if _, err := exec.Execute(sql); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Verify delete
	sql = "SELECT * FROM `index`"
	res, err = exec.Execute(sql)
	if err != nil {
		t.Fatalf("select after delete failed: %v", err)
	}

	if len(res.Rows) != 1 {
		t.Errorf("expected 1 row after delete, got %d", len(res.Rows))
	}
}
