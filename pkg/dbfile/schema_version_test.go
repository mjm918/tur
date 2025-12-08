// pkg/dbfile/schema_version_test.go
package dbfile

import (
	"path/filepath"
	"testing"
)

func TestSchemaVersion_InitialValue(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	// Initial schema version should be 0
	if sv := db.SchemaVersion(); sv != 0 {
		t.Errorf("SchemaVersion() = %d, want 0", sv)
	}
}

func TestSchemaVersion_SetAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Set schema version
	db.SetSchemaVersion(5)

	if sv := db.SchemaVersion(); sv != 5 {
		t.Errorf("SchemaVersion() = %d, want 5", sv)
	}

	db.Sync()
	db.Close()

	// Reopen and verify persistence
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db2.Close()

	if sv := db2.SchemaVersion(); sv != 5 {
		t.Errorf("SchemaVersion() after reopen = %d, want 5", sv)
	}
}

func TestChangeCounter_InitialValue(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	// Initial change counter should be 0
	if cc := db.ChangeCounter(); cc != 0 {
		t.Errorf("ChangeCounter() = %d, want 0", cc)
	}
}

func TestChangeCounter_Increment(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	// Increment change counter
	db.IncrementChangeCounter()

	if cc := db.ChangeCounter(); cc != 1 {
		t.Errorf("ChangeCounter() = %d, want 1", cc)
	}

	// Increment again
	db.IncrementChangeCounter()

	if cc := db.ChangeCounter(); cc != 2 {
		t.Errorf("ChangeCounter() = %d, want 2", cc)
	}
}

func TestChangeCounter_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	db.IncrementChangeCounter()
	db.IncrementChangeCounter()
	db.IncrementChangeCounter()

	db.Sync()
	db.Close()

	// Reopen and verify persistence
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db2.Close()

	if cc := db2.ChangeCounter(); cc != 3 {
		t.Errorf("ChangeCounter() after reopen = %d, want 3", cc)
	}
}

func TestSchemaCookie_InitialValue(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	// Initial schema cookie should be 0
	if sc := db.SchemaCookie(); sc != 0 {
		t.Errorf("SchemaCookie() = %d, want 0", sc)
	}
}

func TestSchemaCookie_Increment(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	// Increment schema cookie (used when schema changes)
	db.IncrementSchemaCookie()

	if sc := db.SchemaCookie(); sc != 1 {
		t.Errorf("SchemaCookie() = %d, want 1", sc)
	}
}

func TestSchemaCookie_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	db.IncrementSchemaCookie()
	db.IncrementSchemaCookie()

	db.Sync()
	db.Close()

	// Reopen and verify persistence
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db2.Close()

	if sc := db2.SchemaCookie(); sc != 2 {
		t.Errorf("SchemaCookie() after reopen = %d, want 2", sc)
	}
}

func TestVersionValidFor_Update(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := Create(dbPath, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer db.Close()

	// Increment change counter
	db.IncrementChangeCounter()
	db.IncrementChangeCounter()

	// VersionValidFor should be updatable
	// It records the change counter value at which the version was set
	db.Header().VersionValidFor = db.ChangeCounter()

	if db.Header().VersionValidFor != 2 {
		t.Errorf("VersionValidFor = %d, want 2", db.Header().VersionValidFor)
	}
}
