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
