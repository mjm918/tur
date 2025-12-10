// pkg/cli/repl_test.go
package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestREPL_ExecuteStatement(t *testing.T) {
	// Create a temporary database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	output := &bytes.Buffer{}
	errOutput := &bytes.Buffer{}

	repl, err := NewREPL(dbPath, output, errOutput)
	if err != nil {
		t.Fatalf("NewREPL failed: %v", err)
	}
	defer repl.Close()

	// Execute CREATE TABLE
	err = repl.ExecuteStatement("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Execute INSERT
	err = repl.ExecuteStatement("INSERT INTO test (id, name) VALUES (1, 'Alice');")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Execute SELECT
	output.Reset()
	err = repl.ExecuteStatement("SELECT * FROM test;")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	// Check output contains expected data
	result := output.String()
	if !strings.Contains(result, "id") || !strings.Contains(result, "name") {
		t.Errorf("output should contain column headers, got: %s", result)
	}
	if !strings.Contains(result, "1") || !strings.Contains(result, "Alice") {
		t.Errorf("output should contain row data, got: %s", result)
	}
}

func TestREPL_ExecuteStatement_Error(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	output := &bytes.Buffer{}
	errOutput := &bytes.Buffer{}

	repl, err := NewREPL(dbPath, output, errOutput)
	if err != nil {
		t.Fatalf("NewREPL failed: %v", err)
	}
	defer repl.Close()

	// Execute invalid SQL
	err = repl.ExecuteStatement("SELECT * FROM nonexistent;")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestREPL_DisplayResult(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	output := &bytes.Buffer{}
	errOutput := &bytes.Buffer{}

	repl, err := NewREPL(dbPath, output, errOutput)
	if err != nil {
		t.Fatalf("NewREPL failed: %v", err)
	}
	defer repl.Close()

	// Create a table with data
	repl.ExecuteStatement("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);")
	repl.ExecuteStatement("INSERT INTO users VALUES (1, 'Alice', 30);")
	repl.ExecuteStatement("INSERT INTO users VALUES (2, 'Bob', 25);")

	// Select and display
	output.Reset()
	repl.ExecuteStatement("SELECT * FROM users;")

	result := output.String()

	// Check for table formatting
	if !strings.Contains(result, "id") {
		t.Error("output should contain 'id' column")
	}
	if !strings.Contains(result, "name") {
		t.Error("output should contain 'name' column")
	}
	if !strings.Contains(result, "age") {
		t.Error("output should contain 'age' column")
	}
	if !strings.Contains(result, "Alice") {
		t.Error("output should contain 'Alice'")
	}
	if !strings.Contains(result, "Bob") {
		t.Error("output should contain 'Bob'")
	}
}

func TestREPL_Run(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Simulate user input
	input := strings.NewReader("CREATE TABLE t (x INTEGER);\nINSERT INTO t VALUES (1);\nSELECT * FROM t;\n.exit\n")
	output := &bytes.Buffer{}
	errOutput := &bytes.Buffer{}

	repl, err := NewREPLWithInput(dbPath, input, output, errOutput)
	if err != nil {
		t.Fatalf("NewREPLWithInput failed: %v", err)
	}

	// Run the REPL - it should exit on .exit command
	repl.Run()

	result := output.String()

	// Check that SELECT output is present
	if !strings.Contains(result, "1") {
		t.Errorf("output should contain SELECT result, got: %s", result)
	}
}

func TestREPL_DotExit(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	input := strings.NewReader(".exit\n")
	output := &bytes.Buffer{}
	errOutput := &bytes.Buffer{}

	repl, err := NewREPLWithInput(dbPath, input, output, errOutput)
	if err != nil {
		t.Fatalf("NewREPLWithInput failed: %v", err)
	}

	// Run should exit cleanly on .exit
	repl.Run()

	// Should not produce an error
	if errOutput.Len() > 0 {
		t.Errorf("unexpected error output: %s", errOutput.String())
	}
}

func TestREPL_OpenWithBadPath(t *testing.T) {
	// Try to open a database in a non-existent directory
	output := &bytes.Buffer{}
	errOutput := &bytes.Buffer{}

	_, err := NewREPL("/nonexistent/path/test.db", output, errOutput)
	if err == nil {
		t.Error("expected error for invalid path")
	}
}

func TestREPL_MemoryDatabase(t *testing.T) {
	output := &bytes.Buffer{}
	errOutput := &bytes.Buffer{}

	// Use :memory: for in-memory database
	repl, err := NewREPL(":memory:", output, errOutput)
	if err != nil {
		t.Fatalf("NewREPL with :memory: failed: %v", err)
	}
	defer repl.Close()

	// Should work just like file-based
	err = repl.ExecuteStatement("CREATE TABLE test (id INTEGER);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
}

func TestMain(m *testing.M) {
	// Run tests
	os.Exit(m.Run())
}
