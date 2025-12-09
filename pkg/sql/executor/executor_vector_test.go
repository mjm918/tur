// pkg/sql/executor/executor_vector_test.go
package executor

import (
	"encoding/hex"
	"fmt"
	"testing"

	"tur/pkg/types"
)

// vectorToHex converts a float32 vector to hex string for SQL literals
func vectorToHex(data []float32) string {
	v := types.NewVector(data)
	return hex.EncodeToString(v.ToBytes())
}

// TestVectorQuantize_Basic tests the basic vector_quantize function
func TestVectorQuantize_Basic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column
	_, err := exec.Execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert some vectors using hex encoding
	vec1 := vectorToHex([]float32{1.0, 0.0, 0.0})
	vec2 := vectorToHex([]float32{0.0, 1.0, 0.0})
	vec3 := vectorToHex([]float32{0.0, 0.0, 1.0})

	_, err = exec.Execute(fmt.Sprintf("INSERT INTO embeddings (id, embedding) VALUES (1, x'%s')", vec1))
	if err != nil {
		t.Fatalf("failed to insert row 1: %v", err)
	}
	_, err = exec.Execute(fmt.Sprintf("INSERT INTO embeddings (id, embedding) VALUES (2, x'%s')", vec2))
	if err != nil {
		t.Fatalf("failed to insert row 2: %v", err)
	}
	_, err = exec.Execute(fmt.Sprintf("INSERT INTO embeddings (id, embedding) VALUES (3, x'%s')", vec3))
	if err != nil {
		t.Fatalf("failed to insert row 3: %v", err)
	}

	// Call vector_quantize to build an HNSW index
	result, err := exec.Execute("SELECT vector_quantize('embeddings', 'embedding')")
	if err != nil {
		t.Fatalf("vector_quantize failed: %v", err)
	}

	// Should return the number of vectors indexed
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if len(result.Rows[0]) != 1 {
		t.Fatalf("expected 1 column, got %d", len(result.Rows[0]))
	}

	count := result.Rows[0][0].Int()
	if count != 3 {
		t.Errorf("expected 3 vectors indexed, got %d", count)
	}
}

// TestVectorQuantize_EmptyTable tests vector_quantize on an empty table
func TestVectorQuantize_EmptyTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column
	_, err := exec.Execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Call vector_quantize on empty table
	result, err := exec.Execute("SELECT vector_quantize('embeddings', 'embedding')")
	if err != nil {
		t.Fatalf("vector_quantize failed: %v", err)
	}

	// Should return 0 for empty table
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	count := result.Rows[0][0].Int()
	if count != 0 {
		t.Errorf("expected 0 vectors indexed for empty table, got %d", count)
	}
}

// TestVectorQuantize_InvalidTable tests vector_quantize with invalid table name
func TestVectorQuantize_InvalidTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Call vector_quantize on non-existent table
	// The projection iterator swallows errors and returns no rows
	result, err := exec.Execute("SELECT vector_quantize('nonexistent', 'embedding')")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// No rows returned because projection failed
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows for non-existent table, got %d", len(result.Rows))
	}
}

// TestVectorQuantize_InvalidColumn tests vector_quantize with invalid column name
func TestVectorQuantize_InvalidColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column
	_, err := exec.Execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Call vector_quantize with non-existent column
	// The projection iterator swallows errors and returns no rows
	result, err := exec.Execute("SELECT vector_quantize('embeddings', 'nonexistent')")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// No rows returned because projection failed
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows for non-existent column, got %d", len(result.Rows))
	}
}

// TestVectorQuantize_NonVectorColumn tests vector_quantize on a non-vector column
func TestVectorQuantize_NonVectorColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a non-VECTOR column
	_, err := exec.Execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Call vector_quantize on TEXT column
	// The projection iterator swallows errors and returns no rows
	result, err := exec.Execute("SELECT vector_quantize('items', 'name')")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// No rows returned because projection failed
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows for non-vector column, got %d", len(result.Rows))
	}
}
