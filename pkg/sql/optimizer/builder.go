package optimizer

import (
	"fmt"
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
)

// BuildPlan converts AST to a PlanNode tree
func BuildPlan(stmt *parser.SelectStmt, catalog *schema.Catalog) (PlanNode, error) {
	// 1. Build FROM clause (TableScan or Join)
	// stmt.From is a TableReference interface
	node, err := buildTableReference(stmt.From, catalog)
	if err != nil {
		return nil, err
	}

	// 2. Apply WHERE clause (Filter)
	if stmt.Where != nil {
		node = &FilterNode{
			Input:       node,
			Condition:   stmt.Where,
			Selectivity: 0.1, // Default selectivity, to be refined later
		}
	}

	// 3. Apply Projection (Select columns)
	// Check if SELECT *
	isStar := false
	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		isStar = true
	}

	if !isStar {
		var exprs []parser.Expression
		for _, col := range stmt.Columns {
			// Convert SelectColumn to Expression
			// Currently SelectColumn has just Name string and Star bool
			// So we assume it's a ColumnRef.
			exprs = append(exprs, &parser.ColumnRef{Name: col.Name})
		}
		node = &ProjectionNode{
			Input:       node,
			Expressions: exprs,
		}
	}

	return node, nil
}

func buildTableReference(ref parser.TableReference, catalog *schema.Catalog) (PlanNode, error) {
	switch t := ref.(type) {
	case *parser.Table:
		tableDef := catalog.GetTable(t.Name)
		if tableDef == nil {
			return nil, fmt.Errorf("table %s not found", t.Name)
		}
		// Estimates (placeholders until we have stats)
		cost := 100.0
		rows := int64(1000)

		return &TableScanNode{
			Table: tableDef,
			Alias: t.Alias,
			Cost:  cost,
			Rows:  rows,
		}, nil

	case *parser.Join:
		left, err := buildTableReference(t.Left, catalog)
		if err != nil {
			return nil, err
		}
		right, err := buildTableReference(t.Right, catalog)
		if err != nil {
			return nil, err
		}

		return &NestedLoopJoinNode{
			Left:      left,
			Right:     right,
			Condition: t.Condition,
			JoinType:  t.Type,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported table reference type: %T", ref)
	}
}
