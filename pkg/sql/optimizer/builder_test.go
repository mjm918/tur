package optimizer

import (
	"testing"
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
)

func TestBuildPlan_SimpleSelect(t *testing.T) {
	catalog := schema.NewCatalog()
	catalog.CreateTable(&schema.TableDef{Name: "users"})

	stmt := &parser.SelectStmt{
		Columns: []parser.SelectColumn{{Name: "id"}},
		From:    &parser.Table{Name: "users"},
	}

	plan, err := BuildPlan(stmt, catalog)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}

	proj, ok := plan.(*ProjectionNode)
	if !ok {
		t.Fatalf("Expected ProjectionNode, got %T", plan)
	}

	scan, ok := proj.Input.(*TableScanNode)
	if !ok {
		t.Fatalf("Expected TableScanNode input to projection, got %T", proj.Input)
	}
	if scan.Table.Name != "users" {
		t.Errorf("Expected table scan on users, got %s", scan.Table.Name)
	}
}

func TestBuildPlan_Where(t *testing.T) {
	catalog := schema.NewCatalog()
	catalog.CreateTable(&schema.TableDef{Name: "users"})

	stmt := &parser.SelectStmt{
		Columns: []parser.SelectColumn{{Name: "id"}},
		From:    &parser.Table{Name: "users"},
		Where:   &parser.BinaryExpr{}, // Mock where
	}

	plan, err := BuildPlan(stmt, catalog)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}

	proj, ok := plan.(*ProjectionNode)
	if !ok {
		t.Fatalf("Expected ProjectionNode, got %T", plan)
	}

	filter, ok := proj.Input.(*FilterNode)
	if !ok {
		t.Fatalf("Expected FilterNode input to projection, got %T", proj.Input)
	}

	scan, ok := filter.Input.(*TableScanNode)
	if !ok {
		t.Fatalf("Expected TableScanNode input to filter, got %T", filter.Input)
	}
	if scan.Table.Name != "users" {
		t.Errorf("Expected table scan on users")
	}
}

func TestBuildPlan_Join(t *testing.T) {
	catalog := schema.NewCatalog()
	catalog.CreateTable(&schema.TableDef{Name: "users"})
	catalog.CreateTable(&schema.TableDef{Name: "orders"})

	stmt := &parser.SelectStmt{
		Columns: []parser.SelectColumn{{Name: "*", Star: true}}, // Select *
		From: &parser.Join{
			Left:      &parser.Table{Name: "users"},
			Right:     &parser.Table{Name: "orders"},
			Type:      parser.JoinInner,
			Condition: &parser.BinaryExpr{},
		},
	}

	// For Select *, BuildPlan currently just returns the Join node (no Projection wrapped if simplified star handling)
	// Wait, code says: "if !isStar ... node = &ProjectionNode".
	// Since isStar is true, it returns node directly (the join).

	plan, err := BuildPlan(stmt, catalog)
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}

	join, ok := plan.(*NestedLoopJoinNode)
	if !ok {
		t.Fatalf("Expected NestedLoopJoinNode, got %T", plan)
	}

	left, ok := join.Left.(*TableScanNode)
	if !ok {
		t.Fatalf("Expected Left TableScanNode, got %T", join.Left)
	}
	if left.Table.Name != "users" {
		t.Errorf("Expected users on left")
	}

	right, ok := join.Right.(*TableScanNode)
	if !ok {
		t.Fatalf("Expected Right TableScanNode, got %T", join.Right)
	}
	if right.Table.Name != "orders" {
		t.Errorf("Expected orders on right")
	}
}
