// pkg/cowbtree/cowbtree_test.go
package cowbtree

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCowBTreeBasicOperations(t *testing.T) {
	tree := NewCowBTree()
	defer tree.Close()

	// Test insert and get
	key := []byte("test-key")
	value := []byte("test-value")

	err := tree.Insert(key, value)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := tree.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Errorf("Got %q, want %q", got, value)
	}

	// Test key not found
	_, err = tree.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// Test delete
	err = tree.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = tree.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Expected key to be deleted, got %v", err)
	}
}

func TestCowBTreeMultipleInserts(t *testing.T) {
	tree := NewCowBTree()
	defer tree.Close()

	// Insert many keys
	n := 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d", i))
		if err := tree.Insert(key, value); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Verify all keys
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		expectedValue := []byte(fmt.Sprintf("value-%05d", i))

		got, err := tree.Get(key)
		if err != nil {
			t.Fatalf("Get %d failed: %v", i, err)
		}
		if !bytes.Equal(got, expectedValue) {
			t.Errorf("Key %d: got %q, want %q", i, got, expectedValue)
		}
	}

	// Check stats
	stats := tree.Stats()
	if stats.KeyCount != int64(n) {
		t.Errorf("KeyCount: got %d, want %d", stats.KeyCount, n)
	}
}

func TestCowBTreeUpdate(t *testing.T) {
	tree := NewCowBTree()
	defer tree.Close()

	key := []byte("update-key")
	value1 := []byte("value-1")
	value2 := []byte("value-2")

	// Insert initial value
	tree.Insert(key, value1)

	// Update
	tree.Insert(key, value2)

	// Verify update
	got, err := tree.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, value2) {
		t.Errorf("Got %q, want %q", got, value2)
	}

	// Key count should still be 1
	if tree.KeyCount() != 1 {
		t.Errorf("KeyCount: got %d, want 1", tree.KeyCount())
	}
}

func TestCowBTreeRangeScan(t *testing.T) {
	tree := NewCowBTree()
	defer tree.Close()

	// Insert keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%03d", i))
		tree.Insert(key, value)
	}

	// Range scan from key-020 to key-030
	var results []string
	err := tree.Range([]byte("key-020"), []byte("key-030"), func(key, value []byte) bool {
		results = append(results, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("Range failed: %v", err)
	}

	if len(results) != 11 { // 020 through 030 inclusive
		t.Errorf("Range count: got %d, want 11", len(results))
	}

	// Verify order
	for i, key := range results {
		expected := fmt.Sprintf("key-%03d", i+20)
		if key != expected {
			t.Errorf("Range[%d]: got %s, want %s", i, key, expected)
		}
	}
}

func TestCowBTreeSnapshot(t *testing.T) {
	tree := NewCowBTree()
	defer tree.Close()

	// Insert some keys
	tree.Insert([]byte("key1"), []byte("value1"))
	tree.Insert([]byte("key2"), []byte("value2"))

	// Create snapshot
	snapshot := tree.Snapshot()
	defer snapshot.Release()

	// Modify tree after snapshot
	tree.Insert([]byte("key3"), []byte("value3"))
	tree.Insert([]byte("key1"), []byte("value1-updated"))

	// Snapshot should see old values
	val1, err := snapshot.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Snapshot get key1 failed: %v", err)
	}
	if !bytes.Equal(val1, []byte("value1")) {
		t.Errorf("Snapshot key1: got %q, want %q", val1, "value1")
	}

	// Snapshot should not see key3
	_, err = snapshot.Get([]byte("key3"))
	if err != ErrKeyNotFound {
		t.Errorf("Snapshot should not see key3, got %v", err)
	}

	// Current tree should see updated values
	val1Current, _ := tree.Get([]byte("key1"))
	if !bytes.Equal(val1Current, []byte("value1-updated")) {
		t.Errorf("Current tree key1: got %q, want %q", val1Current, "value1-updated")
	}

	val3, _ := tree.Get([]byte("key3"))
	if !bytes.Equal(val3, []byte("value3")) {
		t.Errorf("Current tree key3: got %q, want %q", val3, "value3")
	}
}

func TestCowBTreeConcurrentReads(t *testing.T) {
	tree := NewCowBTree()
	defer tree.Close()

	// Insert keys
	n := 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d", i))
		tree.Insert(key, value)
	}

	// Concurrent reads
	var wg sync.WaitGroup
	readers := 10
	readsPerReader := 1000
	errCount := int32(0)

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(readerID)))
			for i := 0; i < readsPerReader; i++ {
				idx := rng.Intn(n)
				key := []byte(fmt.Sprintf("key-%05d", idx))
				expectedValue := []byte(fmt.Sprintf("value-%05d", idx))

				got, err := tree.Get(key)
				if err != nil {
					atomic.AddInt32(&errCount, 1)
					continue
				}
				if !bytes.Equal(got, expectedValue) {
					atomic.AddInt32(&errCount, 1)
				}
			}
		}(r)
	}

	wg.Wait()

	if errCount > 0 {
		t.Errorf("Concurrent reads had %d errors", errCount)
	}
}

func TestCowBTreeConcurrentReadsAndWrites(t *testing.T) {
	tree := NewCowBTree()
	defer tree.Close()

	// Pre-populate
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%03d", i))
		tree.Insert(key, value)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Start readers
	readErrors := int32(0)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(readerID)))

			for {
				select {
				case <-done:
					return
				default:
					idx := rng.Intn(100)
					key := []byte(fmt.Sprintf("key-%03d", idx))
					_, err := tree.Get(key)
					if err != nil && err != ErrKeyNotFound {
						atomic.AddInt32(&readErrors, 1)
					}
				}
			}
		}(i)
	}

	// Start writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			key := []byte(fmt.Sprintf("key-%03d", i%100))
			value := []byte(fmt.Sprintf("value-%03d-v%d", i%100, i))
			tree.Insert(key, value)
			time.Sleep(time.Microsecond * 100)
		}
	}()

	// Let it run for a bit
	time.Sleep(time.Millisecond * 500)
	close(done)
	wg.Wait()

	if readErrors > 0 {
		t.Errorf("Had %d read errors during concurrent access", readErrors)
	}
}

func TestCowBTreeLargeValues(t *testing.T) {
	tree := NewCowBTree()
	defer tree.Close()

	// Insert large values
	largeValue := make([]byte, 10000)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("large-key-%03d", i))
		tree.Insert(key, largeValue)
	}

	// Verify
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("large-key-%03d", i))
		got, err := tree.Get(key)
		if err != nil {
			t.Fatalf("Get large value %d failed: %v", i, err)
		}
		if !bytes.Equal(got, largeValue) {
			t.Errorf("Large value %d mismatch", i)
		}
	}
}

func TestCowBTreeNodeSplitting(t *testing.T) {
	// Use small max keys to force splits
	config := NodeConfig{MaxKeys: 4}
	tree := NewCowBTreeWithConfig(config)
	defer tree.Close()

	// Insert enough to cause multiple splits
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("split-key-%02d", i))
		value := []byte(fmt.Sprintf("split-value-%02d", i))
		tree.Insert(key, value)
	}

	// Verify all keys still accessible
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("split-key-%02d", i))
		expectedValue := []byte(fmt.Sprintf("split-value-%02d", i))

		got, err := tree.Get(key)
		if err != nil {
			t.Fatalf("Get after split %d failed: %v", i, err)
		}
		if !bytes.Equal(got, expectedValue) {
			t.Errorf("After split key %d: got %q, want %q", i, got, expectedValue)
		}
	}

	stats := tree.Stats()
	if stats.SplitCount == 0 {
		t.Error("Expected some splits to occur")
	}
	if stats.Height <= 1 {
		t.Error("Expected tree height > 1 after splits")
	}
}

func TestCowBTreeDeleteAllKeys(t *testing.T) {
	tree := NewCowBTree()
	defer tree.Close()

	n := 100
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("del-key-%03d", i))
		value := []byte(fmt.Sprintf("del-value-%03d", i))
		tree.Insert(key, value)
	}

	// Delete all keys
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("del-key-%03d", i))
		err := tree.Delete(key)
		if err != nil {
			t.Fatalf("Delete %d failed: %v", i, err)
		}
	}

	// Verify all deleted
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("del-key-%03d", i))
		_, err := tree.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("Key %d should be deleted", i)
		}
	}

	if tree.KeyCount() != 0 {
		t.Errorf("KeyCount should be 0, got %d", tree.KeyCount())
	}
}

func TestCowBTreeEpochReclamation(t *testing.T) {
	tree := NewCowBTree()
	defer tree.Close()

	// Insert and update many times to generate old versions
	for round := 0; round < 10; round++ {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("epoch-key-%02d", i))
			value := []byte(fmt.Sprintf("epoch-value-%02d-round-%02d", i, round))
			tree.Insert(key, value)
		}
	}

	// Force reclamation
	tree.epoch.Advance()
	reclaimed := tree.epoch.TryReclaim()

	// Should have reclaimed some old nodes
	t.Logf("Reclaimed %d nodes", reclaimed)
}

func BenchmarkCowBTreeInsert(b *testing.B) {
	tree := NewCowBTree()
	defer tree.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("bench-key-%010d", i))
		value := []byte(fmt.Sprintf("bench-value-%010d", i))
		tree.Insert(key, value)
	}
}

func BenchmarkCowBTreeGet(b *testing.B) {
	tree := NewCowBTree()
	defer tree.Close()

	// Pre-populate
	n := 10000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("bench-key-%05d", i))
		value := []byte(fmt.Sprintf("bench-value-%05d", i))
		tree.Insert(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("bench-key-%05d", i%n))
		tree.Get(key)
	}
}

func BenchmarkCowBTreeConcurrentReads(b *testing.B) {
	tree := NewCowBTree()
	defer tree.Close()

	// Pre-populate
	n := 10000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("bench-key-%05d", i))
		value := []byte(fmt.Sprintf("bench-value-%05d", i))
		tree.Insert(key, value)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("bench-key-%05d", i%n))
			tree.Get(key)
			i++
		}
	})
}

func BenchmarkCowBTreeMixedWorkload(b *testing.B) {
	tree := NewCowBTree()
	defer tree.Close()

	// Pre-populate
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("bench-key-%05d", i))
		value := []byte(fmt.Sprintf("bench-value-%05d", i))
		tree.Insert(key, value)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				// 10% writes
				key := []byte(fmt.Sprintf("bench-key-%05d", i%1000))
				value := []byte(fmt.Sprintf("bench-value-%05d-v%d", i%1000, i))
				tree.Insert(key, value)
			} else {
				// 90% reads
				key := []byte(fmt.Sprintf("bench-key-%05d", i%1000))
				tree.Get(key)
			}
			i++
		}
	})
}
