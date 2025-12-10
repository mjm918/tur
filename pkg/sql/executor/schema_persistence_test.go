package executor

import (
	"path/filepath"
	"strings"
	"testing"

	"tur/pkg/dbfile"
	"tur/pkg/pager"
)

func TestNewExecutor_InitializesSchemaBTree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_schema_init.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	exec := New(p)
	if exec.schemaBTree == nil {
		t.Fatal("Expected schema B-tree to be initialized")
	}

	// Verify schema B-tree root is page 1
	if exec.schemaBTree.RootPage() != 1 {
		t.Errorf("Expected schema B-tree root page 1, got %d", exec.schemaBTree.RootPage())
	}
}

func TestPersistSchemaEntry_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_persist_entry.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	exec := New(p)

	// Create a schema entry
	entry := &dbfile.SchemaEntry{
		Type:      dbfile.SchemaEntryTable,
		Name:      "users",
		TableName: "users",
		RootPage:  10,
		SQL:       "CREATE TABLE users (id INT)",
	}

	// Persist it
	err = exec.persistSchemaEntry(entry)
	if err != nil {
		t.Fatalf("persistSchemaEntry failed: %v", err)
	}

	// Read it back
	retrieved, err := exec.getSchemaEntry("users")
	if err != nil {
		t.Fatalf("getSchemaEntry failed: %v", err)
	}

	if retrieved.Name != entry.Name {
		t.Errorf("Name mismatch: got %s, want %s", retrieved.Name, entry.Name)
	}

	if retrieved.RootPage != entry.RootPage {
		t.Errorf("RootPage mismatch: got %d, want %d", retrieved.RootPage, entry.RootPage)
	}

	if retrieved.SQL != entry.SQL {
		t.Errorf("SQL mismatch: got %s, want %s", retrieved.SQL, entry.SQL)
	}
}

func TestCreateTable_PersistsSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_create_table_persist.db")

	// Phase 1: Create table and close
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	p.Close()

	// Phase 2: Reopen and verify schema persisted
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	table := exec2.catalog.GetTable("users")
	if table == nil {
		t.Fatal("Table 'users' not found after reopen - schema not persisted")
	}

	if len(table.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(table.Columns))
	}

	if table.Columns[0].Name != "id" {
		t.Errorf("First column should be 'id', got %s", table.Columns[0].Name)
	}
}

func TestCreateIndex_PersistsSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_create_index_persist.db")

	// Create table and index
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_email ON users(email)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	p.Close()

	// Reopen and verify
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	idx := exec2.catalog.GetIndex("idx_email")
	if idx == nil {
		t.Fatal("Index not found after reopen")
	}

	if idx.TableName != "users" {
		t.Errorf("Wrong table name: %s", idx.TableName)
	}

	if len(idx.Columns) != 1 || idx.Columns[0] != "email" {
		t.Errorf("Wrong columns: %v", idx.Columns)
	}
}

func TestDropTable_RemovesSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_drop_table.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE temp (id INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("DROP TABLE temp")
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}

	p.Close()

	// Reopen and verify table is gone
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	if exec2.catalog.GetTable("temp") != nil {
		t.Error("Dropped table still exists after reopen")
	}
}

func TestDropIndex_RemovesSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_drop_index.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE users (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE INDEX idx_email ON users(email)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	_, err = exec.Execute("DROP INDEX idx_email")
	if err != nil {
		t.Fatalf("DROP INDEX failed: %v", err)
	}

	p.Close()

	// Reopen and verify index is gone
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	if exec2.catalog.GetIndex("idx_email") != nil {
		t.Error("Dropped index still exists after reopen")
	}

	// Table should still exist
	if exec2.catalog.GetTable("users") == nil {
		t.Error("Table was incorrectly removed when dropping index")
	}
}

func TestCreateView_PersistsSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_create_view_persist.db")

	// Phase 1: Create table and view, then close
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute("CREATE VIEW adult_users AS SELECT id, name FROM users WHERE age >= 18")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	p.Close()

	// Phase 2: Reopen and verify view persisted
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	view := exec2.catalog.GetView("adult_users")
	if view == nil {
		t.Fatal("View 'adult_users' not found after reopen - schema not persisted")
	}

	if view.Name != "adult_users" {
		t.Errorf("Expected view name 'adult_users', got %s", view.Name)
	}
}

func TestCreateTrigger_PersistsSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_create_trigger_persist.db")

	// Phase 1: Create table and trigger, then close
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)
	_, err = exec.Execute("CREATE TABLE audit_log (id INT, event_type TEXT, created_at TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE audit_log failed: %v", err)
	}

	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	_, err = exec.Execute(`CREATE TRIGGER log_insert AFTER INSERT ON users
BEGIN
	INSERT INTO audit_log (event_type) VALUES ('insert');
END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	p.Close()

	// Phase 2: Reopen and verify trigger persisted
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	trigger := exec2.catalog.GetTrigger("log_insert")
	if trigger == nil {
		t.Fatal("Trigger 'log_insert' not found after reopen - schema not persisted")
	}

	if trigger.Name != "log_insert" {
		t.Errorf("Expected trigger name 'log_insert', got %s", trigger.Name)
	}

	if trigger.TableName != "users" {
		t.Errorf("Expected trigger table 'users', got %s", trigger.TableName)
	}
}

func TestDropTable_BlockedByDependentView(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_drop_blocked_view.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	exec := New(p)

	// Create table
	_, err = exec.Execute("CREATE TABLE users (id INT, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create view that depends on table
	_, err = exec.Execute("CREATE VIEW adult_users AS SELECT id, name FROM users WHERE age >= 18")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// Try to drop table - should fail because view depends on it
	_, err = exec.Execute("DROP TABLE users")
	if err == nil {
		t.Fatal("DROP TABLE should fail when a view depends on it")
	}

	// Error message should mention the dependent view
	if !contains(err.Error(), "view") && !contains(err.Error(), "adult_users") {
		t.Errorf("Error should mention dependent view, got: %v", err)
	}
}

func TestDropTable_BlockedByDependentTrigger(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_drop_blocked_trigger.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	exec := New(p)

	// Create audit log table
	_, err = exec.Execute("CREATE TABLE audit_log (id INT, event_type TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE audit_log failed: %v", err)
	}

	// Create users table
	_, err = exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Create trigger on users table
	_, err = exec.Execute(`CREATE TRIGGER log_insert AFTER INSERT ON users
BEGIN
	INSERT INTO audit_log (event_type) VALUES ('insert');
END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Try to drop users table - should fail because trigger depends on it
	_, err = exec.Execute("DROP TABLE users")
	if err == nil {
		t.Fatal("DROP TABLE should fail when a trigger depends on it")
	}

	// Error message should mention the dependent trigger
	if !contains(err.Error(), "trigger") && !contains(err.Error(), "log_insert") {
		t.Errorf("Error should mention dependent trigger, got: %v", err)
	}
}

func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

func TestDropTable_CascadeDropsIndexesTriggersViews(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_drop_cascade.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	exec := New(p)

	// Create audit log table
	_, err = exec.Execute("CREATE TABLE audit_log (id INT, event_type TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE audit_log failed: %v", err)
	}

	// Create users table
	_, err = exec.Execute("CREATE TABLE users (id INT, name TEXT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE users failed: %v", err)
	}

	// Create index on users
	_, err = exec.Execute("CREATE INDEX idx_email ON users(email)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	// Create trigger on users
	_, err = exec.Execute(`CREATE TRIGGER log_insert AFTER INSERT ON users
BEGIN
	INSERT INTO audit_log (event_type) VALUES ('insert');
END`)
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Create view on users
	_, err = exec.Execute("CREATE VIEW user_emails AS SELECT id, email FROM users")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// Drop table with CASCADE - should succeed and drop dependencies
	_, err = exec.Execute("DROP TABLE users CASCADE")
	if err != nil {
		t.Fatalf("DROP TABLE CASCADE failed: %v", err)
	}

	// Verify table is gone
	if exec.catalog.GetTable("users") != nil {
		t.Error("Table 'users' should be dropped")
	}

	// Verify index is gone
	if exec.catalog.GetIndex("idx_email") != nil {
		t.Error("Index 'idx_email' should be dropped by CASCADE")
	}

	// Verify trigger is gone
	if exec.catalog.GetTrigger("log_insert") != nil {
		t.Error("Trigger 'log_insert' should be dropped by CASCADE")
	}

	// Verify view is gone
	if exec.catalog.GetView("user_emails") != nil {
		t.Error("View 'user_emails' should be dropped by CASCADE")
	}

	// Verify audit_log still exists
	if exec.catalog.GetTable("audit_log") == nil {
		t.Error("Table 'audit_log' should still exist")
	}
}

func TestDropTable_FreesPages(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_drop_frees_pages.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)

	// Record initial page count (should be 2 - page 0 for header, page 1 for schema tree)
	initialPageCount := p.PageCount()

	// Create a table (allocates at least 1 page)
	_, err = exec.Execute("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Page count should have increased
	afterCreatePageCount := p.PageCount()
	if afterCreatePageCount <= initialPageCount {
		t.Fatalf("Expected page count to increase after CREATE TABLE")
	}

	// Record free pages before drop
	freePagesBefore := p.FreePageCount()

	// Drop the table
	_, err = exec.Execute("DROP TABLE users")
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}

	// Free pages should have increased
	freePagesAfter := p.FreePageCount()
	if freePagesAfter <= freePagesBefore {
		t.Errorf("Expected free pages to increase after DROP TABLE: before=%d, after=%d", freePagesBefore, freePagesAfter)
	}

	p.Close()
}

func TestCreateProcedure_PersistsSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_create_procedure_persist.db")

	// Phase 1: Create table and procedure, then close
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)

	// Create a table that the procedure will use
	_, err = exec.Execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Create a simple stored procedure
	_, err = exec.Execute(`CREATE PROCEDURE add_user(IN user_id INT, IN user_name TEXT)
BEGIN
	INSERT INTO users (id, name) VALUES (user_id, user_name);
END`)
	if err != nil {
		t.Fatalf("CREATE PROCEDURE failed: %v", err)
	}

	// Verify procedure exists before close
	proc := exec.catalog.GetProcedure("add_user")
	if proc == nil {
		t.Fatal("Procedure 'add_user' not found immediately after creation")
	}

	p.Close()

	// Phase 2: Reopen and verify procedure persisted
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	proc2 := exec2.catalog.GetProcedure("add_user")
	if proc2 == nil {
		t.Fatal("Procedure 'add_user' not found after reopen - schema not persisted")
	}

	if proc2.Name != "add_user" {
		t.Errorf("Expected procedure name 'add_user', got %s", proc2.Name)
	}

	// Verify parameters were restored
	if len(proc2.Parameters) != 2 {
		t.Fatalf("Expected 2 parameters, got %d", len(proc2.Parameters))
	}

	if proc2.Parameters[0].Name != "user_id" {
		t.Errorf("Expected first param name 'user_id', got %s", proc2.Parameters[0].Name)
	}

	if proc2.Parameters[1].Name != "user_name" {
		t.Errorf("Expected second param name 'user_name', got %s", proc2.Parameters[1].Name)
	}
}

func TestDropProcedure_RemovesSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_drop_procedure.db")

	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)

	// Create procedure
	_, err = exec.Execute(`CREATE PROCEDURE greet()
BEGIN
	SELECT 'Hello';
END`)
	if err != nil {
		t.Fatalf("CREATE PROCEDURE failed: %v", err)
	}

	// Drop procedure
	_, err = exec.Execute("DROP PROCEDURE greet")
	if err != nil {
		t.Fatalf("DROP PROCEDURE failed: %v", err)
	}

	p.Close()

	// Reopen and verify procedure is gone
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	if exec2.catalog.GetProcedure("greet") != nil {
		t.Error("Dropped procedure still exists after reopen")
	}
}

func TestProcedure_WithParams_PersistsSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_procedure_params_persist.db")

	// Phase 1: Create procedure with all param types
	p, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	exec := New(p)

	// Create procedure with IN, OUT, and INOUT parameters
	_, err = exec.Execute(`CREATE PROCEDURE calculate(IN x INT, OUT result INT, INOUT counter INT)
BEGIN
	SET result = x * 2;
	SET counter = counter + 1;
END`)
	if err != nil {
		t.Fatalf("CREATE PROCEDURE failed: %v", err)
	}

	p.Close()

	// Phase 2: Reopen and verify all param modes persisted
	p2, err := pager.Open(path, pager.Options{})
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer p2.Close()

	exec2 := New(p2)
	proc := exec2.catalog.GetProcedure("calculate")
	if proc == nil {
		t.Fatal("Procedure 'calculate' not found after reopen")
	}

	if len(proc.Parameters) != 3 {
		t.Fatalf("Expected 3 parameters, got %d", len(proc.Parameters))
	}

	// Check IN param
	if proc.Parameters[0].Name != "x" || proc.Parameters[0].Mode != 0 { // 0 = ParamModeIn
		t.Errorf("First param should be IN x, got %s mode %d", proc.Parameters[0].Name, proc.Parameters[0].Mode)
	}

	// Check OUT param
	if proc.Parameters[1].Name != "result" || proc.Parameters[1].Mode != 1 { // 1 = ParamModeOut
		t.Errorf("Second param should be OUT result, got %s mode %d", proc.Parameters[1].Name, proc.Parameters[1].Mode)
	}

	// Check INOUT param
	if proc.Parameters[2].Name != "counter" || proc.Parameters[2].Mode != 2 { // 2 = ParamModeInOut
		t.Errorf("Third param should be INOUT counter, got %s mode %d", proc.Parameters[2].Name, proc.Parameters[2].Mode)
	}
}
