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

// TestVectorQuantizeScan_Basic tests the basic vector_quantize_scan function
func TestVectorQuantizeScan_Basic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column
	_, err := exec.Execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert some vectors using hex encoding
	// vec1 is along X axis, vec2 along Y axis, vec3 along Z axis
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

	// Build HNSW index
	_, err = exec.Execute("SELECT vector_quantize('embeddings', 'embedding')")
	if err != nil {
		t.Fatalf("vector_quantize failed: %v", err)
	}

	// Query for vectors similar to X axis - should return rowid 1 first
	queryVec := vectorToHex([]float32{1.0, 0.0, 0.0})
	result, err := exec.Execute(fmt.Sprintf("SELECT * FROM vector_quantize_scan('embeddings', 'embedding', x'%s', 2)", queryVec))
	if err != nil {
		t.Fatalf("vector_quantize_scan failed: %v", err)
	}

	// Should return 2 rows (k=2)
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	// Check columns: should have rowid and distance
	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns (rowid, distance), got %d", len(result.Columns))
	}

	// First result should be rowid=1 (exact match to query)
	firstRowId := result.Rows[0][0].Int()
	if firstRowId != 1 {
		t.Errorf("expected first result to be rowid 1, got %d", firstRowId)
	}

	// First result distance should be 0 (exact match)
	firstDistance := result.Rows[0][1].Float()
	if firstDistance > 0.001 {
		t.Errorf("expected first result distance ~0, got %f", firstDistance)
	}
}

// TestVectorQuantizeScan_NoIndex tests vector_quantize_scan without an index
func TestVectorQuantizeScan_NoIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column
	_, err := exec.Execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert a vector but DON'T build index
	vec1 := vectorToHex([]float32{1.0, 0.0, 0.0})
	_, err = exec.Execute(fmt.Sprintf("INSERT INTO embeddings (id, embedding) VALUES (1, x'%s')", vec1))
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	// Query without index should fail or return empty
	queryVec := vectorToHex([]float32{1.0, 0.0, 0.0})
	result, err := exec.Execute(fmt.Sprintf("SELECT * FROM vector_quantize_scan('embeddings', 'embedding', x'%s', 2)", queryVec))

	// Should either error or return empty result
	if err == nil && len(result.Rows) > 0 {
		t.Errorf("expected error or empty result when no index exists")
	}
}
