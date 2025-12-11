// pkg/cowbtree/versioned_store_test.go
package cowbtree

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestVersionedStoreBasicOperations(t *testing.T) {
	store := NewCowVersionedStore()
	defer store.Close()

	// Begin transaction
	tx := store.Begin()

	// Put key
	key := []byte("test-key")
	value := []byte("test-value")

	err := store.Put(tx, key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get key (should see own write)
	got, err := store.Get(tx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Errorf("Got %q, want %q", got, value)
	}

	// Commit
	err = store.Commit(tx)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Start new transaction and verify
	tx2 := store.Begin()
	got2, err := store.Get(tx2, key)
	if err != nil {
		t.Fatalf("Get after commit failed: %v", err)
	}
	if !bytes.Equal(got2, value) {
		t.Errorf("After commit: got %q, want %q", got2, value)
	}
	store.Commit(tx2)
}

func TestVersionedStoreSnapshotIsolation(t *testing.T) {
	store := NewCowVersionedStore()
	defer store.Close()

	// TX1: Insert initial value
	tx1 := store.Begin()
	store.Put(tx1, []byte("key1"), []byte("value1-initial"))
	store.Commit(tx1)

	// TX2: Start reading transaction (takes snapshot)
	tx2 := store.Begin()

	// TX3: Update the value and commit
	tx3 := store.Begin()
	store.Put(tx3, []byte("key1"), []byte("value1-updated"))
	store.Commit(tx3)

	// TX2 should still see the old value (snapshot isolation)
	got, err := store.Get(tx2, []byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, []byte("value1-initial")) {
		t.Errorf("Snapshot isolation violated: got %q, want %q", got, "value1-initial")
	}

	store.Commit(tx2)

	// New transaction should see updated value
	tx4 := store.Begin()
	got4, _ := store.Get(tx4, []byte("key1"))
	if !bytes.Equal(got4, []byte("value1-updated")) {
		t.Errorf("New tx should see update: got %q, want %q", got4, "value1-updated")
	}
	store.Commit(tx4)
}

func TestVersionedStoreRollback(t *testing.T) {
	store := NewCowVersionedStore()
	defer store.Close()

	// TX1: Insert and commit
	tx1 := store.Begin()
	store.Put(tx1, []byte("key1"), []byte("committed-value"))
	store.Commit(tx1)

	// TX2: Update and rollback
	tx2 := store.Begin()
	store.Put(tx2, []byte("key1"), []byte("rolled-back-value"))

	// Should see own uncommitted write
	got, _ := store.Get(tx2, []byte("key1"))
	if !bytes.Equal(got, []byte("rolled-back-value")) {
		t.Errorf("Should see own write: got %q", got)
	}

	// Rollback
	store.Rollback(tx2)

	// New transaction should see original value
	tx3 := store.Begin()
	got3, _ := store.Get(tx3, []byte("key1"))
	if !bytes.Equal(got3, []byte("committed-value")) {
		t.Errorf("After rollback should see original: got %q, want %q", got3, "committed-value")
	}
	store.Commit(tx3)
}

func TestVersionedStoreWriteConflict(t *testing.T) {
	store := NewCowVersionedStore()
	defer store.Close()

	// TX1: Start and write
	tx1 := store.Begin()
	store.Put(tx1, []byte("conflict-key"), []byte("tx1-value"))
	// Don't commit yet

	// TX2: Try to write to same key
	tx2 := store.Begin()
	err := store.Put(tx2, []byte("conflict-key"), []byte("tx2-value"))

	// Should get write conflict
	if err != ErrWriteConflict {
		t.Errorf("Expected ErrWriteConflict, got %v", err)
	}

	// TX1 commits
	store.Commit(tx1)

	// TX3 should be able to write now
	tx3 := store.Begin()
	err = store.Put(tx3, []byte("conflict-key"), []byte("tx3-value"))
	if err != nil {
		t.Errorf("TX3 should succeed after TX1 commits: %v", err)
	}
	store.Commit(tx3)
}

func TestVersionedStoreDelete(t *testing.T) {
	store := NewCowVersionedStore()
	defer store.Close()

	// Insert
	tx1 := store.Begin()
	store.Put(tx1, []byte("del-key"), []byte("del-value"))
	store.Commit(tx1)

	// Delete
	tx2 := store.Begin()
	store.Delete(tx2, []byte("del-key"))
	store.Commit(tx2)

	// Should not find
	tx3 := store.Begin()
	_, err := store.Get(tx3, []byte("del-key"))
	if err != ErrStoreNotFound {
		t.Errorf("Expected ErrStoreNotFound after delete, got %v", err)
	}
	store.Commit(tx3)
}

func TestVersionedStoreRange(t *testing.T) {
	store := NewCowVersionedStore()
	defer store.Close()

	// Insert keys
	tx1 := store.Begin()
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("range-key-%02d", i))
		value := []byte(fmt.Sprintf("range-value-%02d", i))
		store.Put(tx1, key, value)
	}
	store.Commit(tx1)

	// Range scan
	tx2 := store.Begin()
	var results []string
	err := store.Range(tx2, []byte("range-key-10"), []byte("range-key-20"), func(key, value []byte) bool {
		results = append(results, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("Range failed: %v", err)
	}
	store.Commit(tx2)

	if len(results) != 11 { // 10 through 20 inclusive
		t.Errorf("Range count: got %d, want 11", len(results))
	}
}

func TestVersionedStoreConcurrentTransactions(t *testing.T) {
	store := NewCowVersionedStore()
	defer store.Close()

	// Pre-populate
	tx := store.Begin()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("conc-key-%03d", i))
		value := []byte(fmt.Sprintf("conc-value-%03d", i))
		store.Put(tx, key, value)
	}
	store.Commit(tx)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Multiple readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				tx := store.Begin()
				key := []byte(fmt.Sprintf("conc-key-%03d", j%100))
				_, err := store.Get(tx, key)
				if err != nil && err != ErrStoreNotFound {
					errors <- fmt.Errorf("reader %d get error: %v", readerID, err)
				}
				store.Commit(tx)
			}
		}(i)
	}

	// Single writer updating different keys
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			tx := store.Begin()
			key := []byte(fmt.Sprintf("conc-key-%03d", j))
			value := []byte(fmt.Sprintf("conc-value-%03d-updated", j))
			err := store.Put(tx, key, value)
			if err != nil {
				store.Rollback(tx)
				continue
			}
			store.Commit(tx)
			time.Sleep(time.Microsecond * 100)
		}
	}()

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

func TestVersionedStoreSnapshot(t *testing.T) {
	store := NewCowVersionedStore()
	defer store.Close()

	// Insert data
	tx1 := store.Begin()
	store.Put(tx1, []byte("snap-key1"), []byte("snap-value1"))
	store.Put(tx1, []byte("snap-key2"), []byte("snap-value2"))
	store.Commit(tx1)

	// Create snapshot
	tx2 := store.Begin()
	snapshot, err := store.CreateSnapshot(tx2)
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	defer snapshot.Release()

	// Modify after snapshot
	tx3 := store.Begin()
	store.Put(tx3, []byte("snap-key1"), []byte("snap-value1-updated"))
	store.Put(tx3, []byte("snap-key3"), []byte("snap-value3"))
	store.Commit(tx3)

	// Snapshot should see old values
	val1, err := snapshot.Get([]byte("snap-key1"))
	if err != nil {
		t.Fatalf("Snapshot get failed: %v", err)
	}
	if !bytes.Equal(val1, []byte("snap-value1")) {
		t.Errorf("Snapshot should see old value: got %q", val1)
	}

	store.Commit(tx2)
}

func TestVersionedStoreGarbageCollection(t *testing.T) {
	store := NewCowVersionedStore()
	defer store.Close()

	// Create many versions
	for round := 0; round < 10; round++ {
		tx := store.Begin()
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("gc-key-%02d", i))
			value := []byte(fmt.Sprintf("gc-value-%02d-round-%02d", i, round))
			store.Put(tx, key, value)
		}
		store.Commit(tx)
	}

	// Get stats before GC
	statsBefore := store.Stats()
	t.Logf("Before GC: %d version chains, %d total versions",
		statsBefore.TotalVersionChains, statsBefore.TotalVersions)

	// Run GC
	store.GarbageCollect()

	// Get stats after GC
	statsAfter := store.Stats()
	t.Logf("After GC: %d version chains, %d total versions",
		statsAfter.TotalVersionChains, statsAfter.TotalVersions)
}

func BenchmarkVersionedStorePut(b *testing.B) {
	store := NewCowVersionedStore()
	defer store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := store.Begin()
		key := []byte(fmt.Sprintf("bench-key-%010d", i))
		value := []byte(fmt.Sprintf("bench-value-%010d", i))
		store.Put(tx, key, value)
		store.Commit(tx)
	}
}

func BenchmarkVersionedStoreGet(b *testing.B) {
	store := NewCowVersionedStore()
	defer store.Close()

	// Pre-populate
	n := 10000
	tx := store.Begin()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("bench-key-%05d", i))
		value := []byte(fmt.Sprintf("bench-value-%05d", i))
		store.Put(tx, key, value)
	}
	store.Commit(tx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := store.Begin()
		key := []byte(fmt.Sprintf("bench-key-%05d", i%n))
		store.Get(tx, key)
		store.Commit(tx)
	}
}

func BenchmarkVersionedStoreConcurrentReads(b *testing.B) {
	store := NewCowVersionedStore()
	defer store.Close()

	// Pre-populate
	n := 10000
	tx := store.Begin()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("bench-key-%05d", i))
		value := []byte(fmt.Sprintf("bench-value-%05d", i))
		store.Put(tx, key, value)
	}
	store.Commit(tx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			tx := store.Begin()
			key := []byte(fmt.Sprintf("bench-key-%05d", i%n))
			store.Get(tx, key)
			store.Commit(tx)
			i++
		}
	})
}
