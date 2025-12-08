package executor

import (
	"os"
	"testing"
	"tur/pkg/pager"
)

func TestExecutor_LeftJoin(t *testing.T) {
	// Setup
	tmpFile := "test_left_join.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create tables
	mustExec(t, exec, `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`)
	mustExec(t, exec, `CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT);`)

	// Insert data
	// Users: (1, Alice), (2, Bob), (3, Charlie) - Charlie has no orders
	mustExec(t, exec, "INSERT INTO users VALUES (1, 'Alice');")
	mustExec(t, exec, "INSERT INTO users VALUES (2, 'Bob');")
	mustExec(t, exec, "INSERT INTO users VALUES (3, 'Charlie');") // No orders

	// Orders: (101, 1, 50), (102, 1, 20), (103, 2, 100)
	mustExec(t, exec, "INSERT INTO orders VALUES (101, 1, 50);")
	mustExec(t, exec, "INSERT INTO orders VALUES (102, 1, 20);")
	mustExec(t, exec, "INSERT INTO orders VALUES (103, 2, 100);")

	// LEFT JOIN - all users, orders may be NULL
	sql := `SELECT users.name, orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id`
	res, err := exec.Execute(sql)
	if err != nil {
		t.Fatalf("select left join failed: %v", err)
	}

	// Expected: Alice(50), Alice(20), Bob(100), Charlie(NULL) = 4 rows
	if len(res.Rows) != 4 {
		t.Errorf("expected 4 rows, got %d", len(res.Rows))
		for i, row := range res.Rows {
			t.Logf("Row %d: %v", i, row)
		}
	}

	foundAlice50 := false
	foundAlice20 := false
	foundBob100 := false
	foundCharlieNull := false

	for _, row := range res.Rows {
		if len(row) < 2 {
			t.Errorf("expected 2 columns, got %d", len(row))
			continue
		}
		name := row[0].Text()
		isNull := row[1].IsNull()

		if name == "Alice" && !isNull && row[1].Int() == 50 {
			foundAlice50 = true
		}
		if name == "Alice" && !isNull && row[1].Int() == 20 {
			foundAlice20 = true
		}
		if name == "Bob" && !isNull && row[1].Int() == 100 {
			foundBob100 = true
		}
		if name == "Charlie" && isNull {
			foundCharlieNull = true
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
	if !foundCharlieNull {
		t.Error("missing Charlie with NULL order - LEFT JOIN not working")
	}
}

func TestExecutor_RightJoin(t *testing.T) {
	// Setup
	tmpFile := "test_right_join.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create tables
	mustExec(t, exec, `CREATE TABLE products (id INT PRIMARY KEY, name TEXT);`)
	mustExec(t, exec, `CREATE TABLE sales (id INT PRIMARY KEY, product_id INT, qty INT);`)

	// Products: (1, Widget), (2, Gadget)
	mustExec(t, exec, "INSERT INTO products VALUES (1, 'Widget');")
	mustExec(t, exec, "INSERT INTO products VALUES (2, 'Gadget');")

	// Sales: (101, 1, 10), (102, 1, 5), (103, 99, 3) - product 99 doesn't exist
	mustExec(t, exec, "INSERT INTO sales VALUES (101, 1, 10);")
	mustExec(t, exec, "INSERT INTO sales VALUES (102, 1, 5);")
	mustExec(t, exec, "INSERT INTO sales VALUES (103, 99, 3);") // Orphan sale

	// RIGHT JOIN - all sales, products may be NULL
	sql := `SELECT products.name, sales.qty FROM products RIGHT JOIN sales ON products.id = sales.product_id`
	res, err := exec.Execute(sql)
	if err != nil {
		t.Fatalf("select right join failed: %v", err)
	}

	// Expected: Widget(10), Widget(5), NULL(3) = 3 rows
	if len(res.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(res.Rows))
		for i, row := range res.Rows {
			t.Logf("Row %d: %v", i, row)
		}
	}

	foundWidget10 := false
	foundWidget5 := false
	foundNullQty3 := false

	for _, row := range res.Rows {
		if len(row) < 2 {
			t.Errorf("expected 2 columns, got %d", len(row))
			continue
		}
		isNameNull := row[0].IsNull()
		qty := row[1].Int()

		if !isNameNull && row[0].Text() == "Widget" && qty == 10 {
			foundWidget10 = true
		}
		if !isNameNull && row[0].Text() == "Widget" && qty == 5 {
			foundWidget5 = true
		}
		if isNameNull && qty == 3 {
			foundNullQty3 = true
		}
	}

	if !foundWidget10 {
		t.Error("missing Widget 10")
	}
	if !foundWidget5 {
		t.Error("missing Widget 5")
	}
	if !foundNullQty3 {
		t.Error("missing NULL product with qty 3 - RIGHT JOIN not working")
	}
}

func TestExecutor_FullOuterJoin(t *testing.T) {
	// Setup
	tmpFile := "test_full_join.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create tables
	mustExec(t, exec, `CREATE TABLE left_t (id INT PRIMARY KEY, val TEXT);`)
	mustExec(t, exec, `CREATE TABLE right_t (id INT PRIMARY KEY, data TEXT);`)

	// Left: (1, A), (2, B), (3, C)
	mustExec(t, exec, "INSERT INTO left_t VALUES (1, 'A');")
	mustExec(t, exec, "INSERT INTO left_t VALUES (2, 'B');")
	mustExec(t, exec, "INSERT INTO left_t VALUES (3, 'C');") // No match in right

	// Right: (1, X), (2, Y), (4, Z)
	mustExec(t, exec, "INSERT INTO right_t VALUES (1, 'X');")
	mustExec(t, exec, "INSERT INTO right_t VALUES (2, 'Y');")
	mustExec(t, exec, "INSERT INTO right_t VALUES (4, 'Z');") // No match in left

	// FULL OUTER JOIN
	sql := `SELECT left_t.val, right_t.data FROM left_t FULL OUTER JOIN right_t ON left_t.id = right_t.id`
	res, err := exec.Execute(sql)
	if err != nil {
		t.Fatalf("select full outer join failed: %v", err)
	}

	// Expected: (A, X), (B, Y), (C, NULL), (NULL, Z) = 4 rows
	if len(res.Rows) != 4 {
		t.Errorf("expected 4 rows, got %d", len(res.Rows))
		for i, row := range res.Rows {
			t.Logf("Row %d: %v", i, row)
		}
	}

	foundAX := false
	foundBY := false
	foundCNull := false
	foundNullZ := false

	for _, row := range res.Rows {
		if len(row) < 2 {
			t.Errorf("expected 2 columns, got %d", len(row))
			continue
		}
		leftNull := row[0].IsNull()
		rightNull := row[1].IsNull()

		if !leftNull && !rightNull && row[0].Text() == "A" && row[1].Text() == "X" {
			foundAX = true
		}
		if !leftNull && !rightNull && row[0].Text() == "B" && row[1].Text() == "Y" {
			foundBY = true
		}
		if !leftNull && rightNull && row[0].Text() == "C" {
			foundCNull = true
		}
		if leftNull && !rightNull && row[1].Text() == "Z" {
			foundNullZ = true
		}
	}

	if !foundAX {
		t.Error("missing (A, X)")
	}
	if !foundBY {
		t.Error("missing (B, Y)")
	}
	if !foundCNull {
		t.Error("missing (C, NULL) - LEFT part of FULL JOIN not working")
	}
	if !foundNullZ {
		t.Error("missing (NULL, Z) - RIGHT part of FULL JOIN not working")
	}
}
