package executor

import (
	"testing"
	"tur/pkg/record"
	"tur/pkg/types"
)

func TestExecutor_Insert_UpdatesIndexes(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	exec.Execute("CREATE INDEX idx_name ON users (name)")

	// Insert row
	_, err := exec.Execute("INSERT INTO users VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Verify index entry exists
	// We need to inspect the index B-tree directly or via future plan to usage index
	// For now, we inspect e.trees["index:idx_name"]

	tree := exec.trees["index:idx_name"]
	if tree == nil {
		t.Fatal("Index tree not found in memory")
	}

	// Construct key for 'Alice'
	// Key = Encode('Alice') + RowID(1) since it's non-unique
	nameVal := types.NewText("Alice")
	rowIDVal := types.NewInt(1)
	key := record.Encode([]types.Value{nameVal, rowIDVal})

	// Check existence
	_, err = tree.Get(key)
	if err != nil {
		t.Errorf("Index entry not found: %v", err)
	}
}

func TestExecutor_Insert_UpdatesUniqueIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE users (email TEXT)")
	exec.Execute("CREATE UNIQUE INDEX idx_email ON users (email)")

	// Insert row
	_, err := exec.Execute("INSERT INTO users VALUES ('alice@example.com')")
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Verify index entry
	tree := exec.trees["index:idx_email"]
	if tree == nil {
		t.Fatal("Index tree not found")
	}

	// Construct key
	key := record.Encode([]types.Value{types.NewText("alice@example.com")})

	// Check existence
	val, err := tree.Get(key)
	if err != nil {
		t.Errorf("Index entry not found: %v", err)
	}
	if len(val) != 8 { // Value is RowID
		t.Errorf("Expected 8 bytes value (rowid), got %d", len(val))
	}

	// Insert duplicate
	_, err = exec.Execute("INSERT INTO users VALUES ('alice@example.com')")
	if err == nil {
		t.Error("Expected error for duplicate unique index key")
	}
}
