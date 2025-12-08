// pkg/hnsw/persistent_test.go
package hnsw

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"tur/pkg/pager"
	"tur/pkg/types"
)

func TestPersistentCreate(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	p, err := pager.Open(dbPath, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	config := DefaultConfig(3)
	idx, err := CreatePersistent(p, config)
	if err != nil {
		t.Fatalf("failed to create persistent index: %v", err)
	}

	if idx.Len() != 0 {
		t.Errorf("expected empty index, got %d nodes", idx.Len())
	}
	if idx.Dimension() != 3 {
		t.Errorf("expected dimension 3, got %d", idx.Dimension())
	}
}

func TestPersistentInsertAndSearch(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	p, err := pager.Open(dbPath, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	config := DefaultConfig(3)
	idx, err := CreatePersistent(p, config)
	if err != nil {
		t.Fatalf("failed to create persistent index: %v", err)
	}

	// Insert vectors
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
		if err := idx.Insert(int64(i+1), vec); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	if idx.Len() != 5 {
		t.Errorf("expected 5 nodes, got %d", idx.Len())
	}

	// Search
	query := types.NewVector([]float32{1.0, 0.0, 0.0})
	query.Normalize()

	results, err := idx.SearchKNN(query, 3)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// First result should be rowID 1 (exact match)
	if results[0].RowID != 1 {
		t.Errorf("expected rowID 1 as first result, got %d", results[0].RowID)
	}
}

func TestPersistentReopen(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	var metaPage uint32

	// Create and populate index
	{
		p, err := pager.Open(dbPath, pager.Options{})
		if err != nil {
			t.Fatalf("failed to open pager: %v", err)
		}

		config := DefaultConfig(3)
		idx, err := CreatePersistent(p, config)
		if err != nil {
			t.Fatalf("failed to create persistent index: %v", err)
		}

		metaPage = idx.MetaPage()

		// Insert vectors
		for i := 0; i < 10; i++ {
			vec := types.NewVector([]float32{
				float32(math.Sin(float64(i))),
				float32(math.Cos(float64(i))),
				float32(math.Sin(float64(i) * 2)),
			})
			vec.Normalize()
			if err := idx.Insert(int64(i+1), vec); err != nil {
				t.Fatalf("insert failed: %v", err)
			}
		}

		if err := idx.Sync(); err != nil {
			t.Fatalf("sync failed: %v", err)
		}
		p.Close()
	}

	// Reopen and verify
	{
		p, err := pager.Open(dbPath, pager.Options{})
		if err != nil {
			t.Fatalf("failed to reopen pager: %v", err)
		}
		defer p.Close()

		idx, err := OpenPersistent(p, metaPage)
		if err != nil {
			t.Fatalf("failed to open persistent index: %v", err)
		}

		if idx.Len() != 10 {
			t.Errorf("expected 10 nodes after reopen, got %d", idx.Len())
		}

		// Search should work
		query := types.NewVector([]float32{0.5, 0.5, 0.5})
		query.Normalize()

		results, err := idx.SearchKNN(query, 5)
		if err != nil {
			t.Fatalf("search after reopen failed: %v", err)
		}

		if len(results) != 5 {
			t.Errorf("expected 5 results, got %d", len(results))
		}
	}
}

func TestPersistentDelete(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	p, err := pager.Open(dbPath, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	config := DefaultConfig(3)
	idx, err := CreatePersistent(p, config)
	if err != nil {
		t.Fatalf("failed to create persistent index: %v", err)
	}

	// Insert vectors
	for i := 0; i < 10; i++ {
		vec := types.NewVector([]float32{float32(i), float32(i + 1), float32(i + 2)})
		vec.Normalize()
		idx.Insert(int64(i+1), vec)
	}

	if idx.Len() != 10 {
		t.Fatalf("expected 10 nodes, got %d", idx.Len())
	}

	// Delete a node
	deleted := idx.Delete(5)
	if !deleted {
		t.Error("expected delete to return true")
	}

	if idx.Len() != 9 {
		t.Errorf("expected 9 nodes after delete, got %d", idx.Len())
	}

	// Delete non-existent
	deleted = idx.Delete(999)
	if deleted {
		t.Error("expected delete of non-existent to return false")
	}
}

func TestPersistentLargeIndex(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Use larger page size
	p, err := pager.Open(dbPath, pager.Options{PageSize: 8192})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	config := DefaultConfig(32) // Smaller dimension for testing
	idx, err := CreatePersistent(p, config)
	if err != nil {
		t.Fatalf("failed to create persistent index: %v", err)
	}

	// Insert 50 vectors
	for i := 0; i < 50; i++ {
		v := make([]float32, 32)
		for j := range v {
			v[j] = float32(math.Sin(float64(i*32 + j)))
		}
		vec := types.NewVector(v)
		vec.Normalize()
		if err := idx.Insert(int64(i), vec); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	if idx.Len() != 50 {
		t.Errorf("expected 50 nodes, got %d", idx.Len())
	}

	// Search
	query := make([]float32, 32)
	for i := range query {
		query[i] = float32(math.Cos(float64(i)))
	}
	queryVec := types.NewVector(query)
	queryVec.Normalize()

	results, err := idx.SearchKNN(queryVec, 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}

	// Check file was created
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("database file not found: %v", err)
	}
	t.Logf("Database file size: %d bytes", info.Size())
}
