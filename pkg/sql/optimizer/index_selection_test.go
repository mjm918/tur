// pkg/sql/optimizer/index_selection_test.go
package optimizer

import (
	"testing"

	"tur/pkg/schema"
	"tur/pkg/sql/lexer"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

// Test Task 1: Identify candidate indexes for WHERE clause predicates

func TestFindCandidateIndexes_SingleColumnEquality(t *testing.T) {
	// Setup: Table with index on "email" column
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "email", Type: types.TypeText},
			{Name: "name", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE email = 'test@example.com'
	whereClause := &parser.BinaryExpr{
		Left:  &parser.ColumnRef{Name: "email"},
		Op:    lexer.EQ,
		Right: &parser.Literal{Value: types.NewText("test@example.com")},
	}

	// Act
	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Assert
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate index, got %d", len(candidates))
	}
	if candidates[0].Index.Name != "idx_users_email" {
		t.Errorf("expected index 'idx_users_email', got '%s'", candidates[0].Index.Name)
	}
	if candidates[0].Column != "email" {
		t.Errorf("expected column 'email', got '%s'", candidates[0].Column)
	}
}

func TestFindCandidateIndexes_NoMatchingIndex(t *testing.T) {
	// Setup: Table with index on "email", but WHERE on "name"
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "email", Type: types.TypeText},
			{Name: "name", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE name = 'John' (no index on name)
	whereClause := &parser.BinaryExpr{
		Left:  &parser.ColumnRef{Name: "name"},
		Op:    lexer.EQ,
		Right: &parser.Literal{Value: types.NewText("John")},
	}

	// Act
	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Assert
	if len(candidates) != 0 {
		t.Fatalf("expected 0 candidate indexes, got %d", len(candidates))
	}
}

func TestFindCandidateIndexes_MultipleIndexes(t *testing.T) {
	// Setup: Table with multiple indexes
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "email", Type: types.TypeText},
			{Name: "status", Type: types.TypeText},
		},
	}
	catalog.CreateTable(tableDef)

	catalog.CreateIndex(&schema.IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      schema.IndexTypeBTree,
	})
	catalog.CreateIndex(&schema.IndexDef{
		Name:      "idx_users_status",
		TableName: "users",
		Columns:   []string{"status"},
		Type:      schema.IndexTypeBTree,
	})

	// WHERE email = 'test@example.com' AND status = 'active'
	whereClause := &parser.BinaryExpr{
		Left: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "email"},
			Op:    lexer.EQ,
			Right: &parser.Literal{Value: types.NewText("test@example.com")},
		},
		Op: lexer.AND,
		Right: &parser.BinaryExpr{
			Left:  &parser.ColumnRef{Name: "status"},
			Op:    lexer.EQ,
			Right: &parser.Literal{Value: types.NewText("active")},
		},
	}

	// Act
	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Assert: Should find both indexes
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidate indexes, got %d", len(candidates))
	}
}

func TestFindCandidateIndexes_RangePredicates(t *testing.T) {
	// Setup: Table with index on "age" column
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "age", Type: types.TypeInt},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_age",
		TableName: "users",
		Columns:   []string{"age"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE age > 30
	whereClause := &parser.BinaryExpr{
		Left:  &parser.ColumnRef{Name: "age"},
		Op:    lexer.GT,
		Right: &parser.Literal{Value: types.NewInt(30)},
	}

	// Act
	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Assert
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate index, got %d", len(candidates))
	}
	if candidates[0].Index.Name != "idx_users_age" {
		t.Errorf("expected index 'idx_users_age', got '%s'", candidates[0].Index.Name)
	}
}

func TestFindCandidateIndexes_ColumnOnRightSide(t *testing.T) {
	// Setup: Table with index on "id" column
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
		},
	}
	catalog.CreateTable(tableDef)

	indexDef := &schema.IndexDef{
		Name:      "idx_users_id",
		TableName: "users",
		Columns:   []string{"id"},
		Type:      schema.IndexTypeBTree,
	}
	catalog.CreateIndex(indexDef)

	// WHERE 100 = id (column on right side)
	whereClause := &parser.BinaryExpr{
		Left:  &parser.Literal{Value: types.NewInt(100)},
		Op:    lexer.EQ,
		Right: &parser.ColumnRef{Name: "id"},
	}

	// Act
	candidates := FindCandidateIndexes(tableDef, whereClause, catalog)

	// Assert: Should still find the index
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate index, got %d", len(candidates))
	}
	if candidates[0].Index.Name != "idx_users_id" {
		t.Errorf("expected index 'idx_users_id', got '%s'", candidates[0].Index.Name)
	}
}

func TestFindCandidateIndexes_NilWhereClause(t *testing.T) {
	catalog := schema.NewCatalog()
	tableDef := &schema.TableDef{
		Name: "users",
		Columns: []schema.ColumnDef{
			{Name: "id", Type: types.TypeInt},
		},
	}
	catalog.CreateTable(tableDef)

	// No WHERE clause
	candidates := FindCandidateIndexes(tableDef, nil, catalog)

	// Assert: No candidates when there's no WHERE
	if len(candidates) != 0 {
		t.Fatalf("expected 0 candidate indexes for nil WHERE, got %d", len(candidates))
	}
}
