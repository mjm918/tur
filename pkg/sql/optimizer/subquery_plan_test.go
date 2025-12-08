package optimizer

import (
	"testing"
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
)

func TestBuildPlan_DerivedTable(t *testing.T) {
	// SELECT * FROM (SELECT id FROM users) AS u
	catalog := schema.NewCatalog()
	catalog.CreateTable(&schema.TableDef{Name: "users"})

	subquery := &parser.SelectStmt{
		Columns: []parser.SelectColumn{{Expr: &parser.ColumnRef{Name: "id"}}},
		From:    &parser.Table{Name: "users"},
	}

	stmt := &parser.SelectStmt{
		Columns: []parser.SelectColumn{{Star: true}},
		From: &parser.DerivedTable{
			Subquery: subquery,
			Alias:    "u",
		},
	}

	plan, err := BuildPlan(stmt, catalog)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}

	// Expected: DerivedTable -> SubqueryScanNode
	// Since Select * from derived table, it might be wrapped in ProjectionNode or just return the Node depending on logic.
	// BuildPlan logic: "if !isStar ... node = &ProjectionNode".
	// Here isStar is true. So it returns node directly.
	// The node matches DerivedTable -> SubqueryScanNode directly.

	subScan, ok := plan.(*SubqueryScanNode)
	if !ok {
		t.Fatalf("Expected SubqueryScanNode, got %T", plan)
	}

	if subScan.Alias != "u" {
		t.Errorf("Expected alias 'u', got '%s'", subScan.Alias)
	}

	// Verify subquery plan
	// It should be a ProjectionNode (SELECT id) -> TableScanNode

	proj, ok := subScan.SubqueryPlan.(*ProjectionNode)
	if !ok {
		t.Fatalf("Expected subquery plan to be ProjectionNode, got %T", subScan.SubqueryPlan)
	}

	scan, ok := proj.Input.(*TableScanNode)
	if !ok {
		t.Fatalf("Expected TableScanNode input to projection, got %T", proj.Input)
	}

	if scan.Table.Name != "users" {
		t.Errorf("Expected users table, got %s", scan.Table.Name)
	}
}
