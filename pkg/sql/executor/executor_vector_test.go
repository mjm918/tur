// pkg/sql/executor/executor_vector_test.go
package executor

import (
	"encoding/hex"
	"fmt"
	"strings"
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
	_, err := exec.Execute("CREATE TABLE embeddings (id INT PRIMARY KEY, embedding VECTOR(3))")
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
	_, err := exec.Execute("CREATE TABLE embeddings (id INT PRIMARY KEY, embedding VECTOR(3))")
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

	// Call vector_quantize on non-existent table - should return an error
	_, err := exec.Execute("SELECT vector_quantize('nonexistent', 'embedding')")
	if err == nil {
		t.Fatal("expected error for non-existent table, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' error, got: %v", err)
	}
}

// TestVectorQuantize_InvalidColumn tests vector_quantize with invalid column name
func TestVectorQuantize_InvalidColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column
	_, err := exec.Execute("CREATE TABLE embeddings (id INT PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Call vector_quantize with non-existent column - should return an error
	_, err = exec.Execute("SELECT vector_quantize('embeddings', 'nonexistent')")
	if err == nil {
		t.Fatal("expected error for non-existent column, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' error, got: %v", err)
	}
}

// TestVectorQuantize_NonVectorColumn tests vector_quantize on a non-vector column
func TestVectorQuantize_NonVectorColumn(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a non-VECTOR column
	_, err := exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Call vector_quantize on TEXT column - should return an error
	_, err = exec.Execute("SELECT vector_quantize('items', 'name')")
	if err == nil {
		t.Fatal("expected error for non-vector column, got nil")
	}
	if !strings.Contains(err.Error(), "not a VECTOR") {
		t.Errorf("expected 'not a VECTOR' error, got: %v", err)
	}
}

// TestVectorQuantizeScan_Basic tests the basic vector_quantize_scan function
func TestVectorQuantizeScan_Basic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column
	_, err := exec.Execute("CREATE TABLE embeddings (id INT PRIMARY KEY, embedding VECTOR(3))")
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

// TestVectorQuantize_WithDistanceMetric tests vector_quantize with distance metric parameter
func TestVectorQuantize_WithDistanceMetric(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column
	_, err := exec.Execute("CREATE TABLE embeddings (id INT PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert normalized vectors to test different metrics work
	// Normalized [1,0,0] remains [1,0,0]
	// Normalized [0.9,0.1,0] becomes [0.994, 0.110, 0]
	vec1 := vectorToHex([]float32{1.0, 0.0, 0.0})
	vec2 := vectorToHex([]float32{0.9, 0.1, 0.0})
	vec3 := vectorToHex([]float32{0.0, 1.0, 0.0})

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

	// Build HNSW index with Euclidean distance metric (default is Cosine)
	result, err := exec.Execute("SELECT vector_quantize('embeddings', 'embedding', 'euclidean')")
	if err != nil {
		t.Fatalf("vector_quantize with euclidean failed: %v", err)
	}

	// Should return the number of vectors indexed
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	count := result.Rows[0][0].Int()
	if count != 3 {
		t.Errorf("expected 3 vectors indexed, got %d", count)
	}

	// Query for vectors similar to [1,0,0] using Euclidean
	queryVec := vectorToHex([]float32{1.0, 0.0, 0.0})
	result, err = exec.Execute(fmt.Sprintf("SELECT * FROM vector_quantize_scan('embeddings', 'embedding', x'%s', 3)", queryVec))
	if err != nil {
		t.Fatalf("vector_quantize_scan failed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	// Log the raw results for debugging
	for i, row := range result.Rows {
		t.Logf("Row %d: rowid=%d, distance=%f", i, row[0].Int(), row[1].Float())
	}

	// First result should be rowid=1 (exact match, distance=0)
	if result.Rows[0][0].Int() != 1 {
		t.Errorf("expected first result to be rowid=1, got %d", result.Rows[0][0].Int())
	}

	// Euclidean distance from [1,0,0] to [1,0,0] = 0
	// Euclidean distance from [1,0,0] to [0.9,0.1,0] = sqrt(0.01+0.01) = 0.1414
	// Euclidean distance from [1,0,0] to [0,1,0] = sqrt(1+1) = 1.414

	dist1 := result.Rows[0][1].Float()
	dist2 := result.Rows[1][1].Float()
	dist3 := result.Rows[2][1].Float()

	if dist1 > 0.01 {
		t.Errorf("expected first distance ~0, got %f", dist1)
	}

	// Verify results are sorted by distance
	if dist2 < dist1 || dist3 < dist2 {
		t.Errorf("results not sorted by distance: %f, %f, %f", dist1, dist2, dist3)
	}
}

// TestVectorQuantize_InvalidMetric tests vector_quantize with invalid distance metric
func TestVectorQuantize_InvalidMetric(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column
	_, err := exec.Execute("CREATE TABLE embeddings (id INT PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Try invalid metric
	_, err = exec.Execute("SELECT vector_quantize('embeddings', 'embedding', 'invalid')")
	if err == nil {
		t.Fatal("expected error for invalid distance metric, got nil")
	}
	if !strings.Contains(err.Error(), "unknown distance metric") {
		t.Errorf("expected 'unknown distance metric' error, got: %v", err)
	}
}

// TestVectorQuantizeScan_NoIndex tests vector_quantize_scan without an index
func TestVectorQuantizeScan_NoIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column
	_, err := exec.Execute("CREATE TABLE embeddings (id INT PRIMARY KEY, embedding VECTOR(3))")
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

// TestVectorInsert_NoNormalize tests that NONORMALIZE option skips normalization
func TestVectorInsert_NoNormalize(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column with NONORMALIZE option
	_, err := exec.Execute("CREATE TABLE embeddings (id INT PRIMARY KEY, embedding VECTOR(3) NONORMALIZE)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert an unnormalized vector [3, 4, 0] - L2 norm is 5
	vec := vectorToHex([]float32{3.0, 4.0, 0.0})
	_, err = exec.Execute(fmt.Sprintf("INSERT INTO embeddings (id, embedding) VALUES (1, x'%s')", vec))
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	// Retrieve the vector and check it was NOT normalized
	result, err := exec.Execute("SELECT embedding FROM embeddings WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to select: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	// Parse the stored vector
	blob := result.Rows[0][0].Blob()
	storedVec, err := types.VectorFromBytes(blob)
	if err != nil {
		t.Fatalf("failed to parse stored vector: %v", err)
	}

	// With NONORMALIZE, values should be the original [3, 4, 0]
	data := storedVec.Data()
	if len(data) != 3 {
		t.Fatalf("expected 3 dimensions, got %d", len(data))
	}

	// Check values are NOT normalized (should be [3, 4, 0], not [0.6, 0.8, 0])
	if data[0] < 2.9 || data[0] > 3.1 {
		t.Errorf("expected x ~3.0 (not normalized), got %f", data[0])
	}
	if data[1] < 3.9 || data[1] > 4.1 {
		t.Errorf("expected y ~4.0 (not normalized), got %f", data[1])
	}
}

// TestVectorInsert_NormalizeByDefault tests that vectors are normalized by default
func TestVectorInsert_NormalizeByDefault(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a VECTOR column WITHOUT NONORMALIZE
	_, err := exec.Execute("CREATE TABLE embeddings (id INT PRIMARY KEY, embedding VECTOR(3))")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert an unnormalized vector [3, 4, 0] - L2 norm is 5
	vec := vectorToHex([]float32{3.0, 4.0, 0.0})
	_, err = exec.Execute(fmt.Sprintf("INSERT INTO embeddings (id, embedding) VALUES (1, x'%s')", vec))
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	// Retrieve the vector and check it WAS normalized
	result, err := exec.Execute("SELECT embedding FROM embeddings WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to select: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	// Parse the stored vector
	blob := result.Rows[0][0].Blob()
	storedVec, err := types.VectorFromBytes(blob)
	if err != nil {
		t.Fatalf("failed to parse stored vector: %v", err)
	}

	// With default normalization, values should be [0.6, 0.8, 0]
	data := storedVec.Data()
	if len(data) != 3 {
		t.Fatalf("expected 3 dimensions, got %d", len(data))
	}

	// Check values ARE normalized
	if data[0] < 0.55 || data[0] > 0.65 {
		t.Errorf("expected x ~0.6 (normalized), got %f", data[0])
	}
	if data[1] < 0.75 || data[1] > 0.85 {
		t.Errorf("expected y ~0.8 (normalized), got %f", data[1])
	}
}
