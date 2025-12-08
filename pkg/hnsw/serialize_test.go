// pkg/hnsw/serialize_test.go
package hnsw

import (
	"bytes"
	"math"
	"testing"

	"tur/pkg/types"
)

func TestSerializeEmpty(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Serialize
	var buf bytes.Buffer
	err := idx.Serialize(&buf)
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}

	// Deserialize
	restored, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	// Verify
	if restored.Len() != 0 {
		t.Errorf("expected empty index, got %d nodes", restored.Len())
	}
	if restored.config.Dimension != config.Dimension {
		t.Errorf("dimension mismatch: %d vs %d", restored.config.Dimension, config.Dimension)
	}
}

func TestSerializeSingleNode(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	vec := types.NewVector([]float32{1.0, 0.0, 0.0})
	vec.Normalize()
	idx.Insert(42, vec)

	// Serialize
	var buf bytes.Buffer
	err := idx.Serialize(&buf)
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}

	// Deserialize
	restored, err := Deserialize(&buf)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	// Verify
	if restored.Len() != 1 {
		t.Errorf("expected 1 node, got %d", restored.Len())
	}

	// Search should find the vector
	results, err := restored.SearchKNN(vec, 1)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].RowID != 42 {
		t.Errorf("expected rowID 42, got %d", results[0].RowID)
	}
	if results[0].Distance > 0.01 {
		t.Errorf("expected distance ~0, got %f", results[0].Distance)
	}
}

func TestSerializeMultipleNodes(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert multiple vectors
	vectors := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
		{1.0, 1.0, 0.0},
		{1.0, 0.0, 1.0},
	}

	for i, v := range vectors {
		vec := types.NewVector(v)
		vec.Normalize()
		idx.Insert(int64(i+1), vec)
	}

	// Serialize
	data, err := idx.SerializeToBytes()
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}

	// Deserialize
	restored, err := DeserializeFromBytes(data)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	// Verify count
	if restored.Len() != len(vectors) {
		t.Errorf("expected %d nodes, got %d", len(vectors), restored.Len())
	}

	// Verify config
	if restored.config.M != config.M {
		t.Errorf("M mismatch: %d vs %d", restored.config.M, config.M)
	}
	if math.Abs(restored.config.ML-config.ML) > 0.0001 {
		t.Errorf("ML mismatch: %f vs %f", restored.config.ML, config.ML)
	}

	// Verify search works
	query := types.NewVector([]float32{1.0, 0.0, 0.0})
	query.Normalize()

	results, err := restored.SearchKNN(query, 3)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	// First result should be rowID 1 (exact match)
	if results[0].RowID != 1 {
		t.Errorf("expected rowID 1 as first result, got %d", results[0].RowID)
	}
}

func TestSerializeLargeIndex(t *testing.T) {
	config := DefaultConfig(128)
	idx := NewIndex(config)

	// Insert 500 vectors
	for i := 0; i < 500; i++ {
		v := make([]float32, 128)
		for j := range v {
			v[j] = float32(math.Sin(float64(i*128 + j)))
		}
		vec := types.NewVector(v)
		vec.Normalize()
		idx.Insert(int64(i), vec)
	}

	// Serialize
	data, err := idx.SerializeToBytes()
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}

	t.Logf("Serialized %d nodes (%d dimensions) to %d bytes", idx.Len(), config.Dimension, len(data))

	// Deserialize
	restored, err := DeserializeFromBytes(data)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	// Verify
	if restored.Len() != idx.Len() {
		t.Errorf("expected %d nodes, got %d", idx.Len(), restored.Len())
	}

	// Verify search works
	query := make([]float32, 128)
	for i := range query {
		query[i] = float32(math.Sin(float64(i)))
	}
	queryVec := types.NewVector(query)
	queryVec.Normalize()

	results, err := restored.SearchKNN(queryVec, 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}
}

func TestSerializePreservesNeighbors(t *testing.T) {
	config := DefaultConfig(3)
	idx := NewIndex(config)

	// Insert enough vectors to create a real graph
	for i := 0; i < 20; i++ {
		vec := types.NewVector([]float32{
			float32(math.Sin(float64(i))),
			float32(math.Cos(float64(i))),
			float32(math.Sin(float64(i) * 2)),
		})
		vec.Normalize()
		idx.Insert(int64(i), vec)
	}

	// Serialize and deserialize
	data, err := idx.SerializeToBytes()
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}

	restored, err := DeserializeFromBytes(data)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	// Verify both indices give same search results
	query := types.NewVector([]float32{0.5, 0.5, 0.5})
	query.Normalize()

	origResults, _ := idx.SearchKNN(query, 5)
	restoredResults, _ := restored.SearchKNN(query, 5)

	if len(origResults) != len(restoredResults) {
		t.Errorf("result count mismatch: %d vs %d", len(origResults), len(restoredResults))
	}

	for i := range origResults {
		if origResults[i].RowID != restoredResults[i].RowID {
			t.Errorf("result %d: rowID mismatch: %d vs %d", i, origResults[i].RowID, restoredResults[i].RowID)
		}
		if math.Abs(float64(origResults[i].Distance-restoredResults[i].Distance)) > 0.0001 {
			t.Errorf("result %d: distance mismatch: %f vs %f", i, origResults[i].Distance, restoredResults[i].Distance)
		}
	}
}

func TestDeserializeInvalidMagic(t *testing.T) {
	data := make([]byte, 100)
	data[0] = 0xFF // Invalid magic

	_, err := DeserializeFromBytes(data)
	if err != ErrInvalidMagic {
		t.Errorf("expected ErrInvalidMagic, got %v", err)
	}
}

func TestDeserializeInvalidVersion(t *testing.T) {
	data := make([]byte, 100)
	// Set valid magic
	data[0] = 0x48 // H
	data[1] = 0x57 // W
	data[2] = 0x53 // S
	data[3] = 0x48 // H
	// Set invalid version
	data[4] = 0xFF
	data[5] = 0xFF
	data[6] = 0xFF
	data[7] = 0xFF

	_, err := DeserializeFromBytes(data)
	if err != ErrInvalidVersion {
		t.Errorf("expected ErrInvalidVersion, got %v", err)
	}
}
