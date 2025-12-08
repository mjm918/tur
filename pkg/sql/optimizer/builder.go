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

	// 3. Apply GROUP BY with aggregations
	if len(stmt.GroupBy) > 0 {
		// Extract aggregate functions from SELECT columns
		aggregates := extractAggregates(stmt.Columns)

		node = &AggregateNode{
			Input:      node,
			GroupBy:    stmt.GroupBy,
			Aggregates: aggregates,
			Having:     stmt.Having,
		}
	}

	// 4. Apply Projection (Select columns)
	// Skip projection when GROUP BY is present - AggregateNode handles column output
	// Check if SELECT *
	isStar := false
	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		isStar = true
	}

	if !isStar && len(stmt.GroupBy) == 0 {
		var exprs []parser.Expression
		for _, col := range stmt.Columns {
			// Convert SelectColumn to Expression
			if col.Expr != nil {
				exprs = append(exprs, col.Expr)
			}
		}
		node = &ProjectionNode{
			Input:       node,
			Expressions: exprs,
		}
	}

	// 5. Apply ORDER BY (Sort)
	if len(stmt.OrderBy) > 0 {
		node = &SortNode{
			Input:   node,
			OrderBy: stmt.OrderBy,
		}
	}

	// 6. Apply LIMIT/OFFSET
	if stmt.Limit != nil || stmt.Offset != nil {
		node = &LimitNode{
			Input:  node,
			Limit:  stmt.Limit,
			Offset: stmt.Offset,
		}
	}

	return node, nil
}

// extractAggregates extracts aggregate function expressions from SELECT columns
func extractAggregates(columns []parser.SelectColumn) []AggregateExpr {
	var aggregates []AggregateExpr

	// For now, we detect aggregate functions by name pattern matching
	// In a full implementation, the parser would create proper FunctionCall nodes
	aggregateFuncs := map[string]bool{
		"COUNT": true, "SUM": true, "AVG": true, "MIN": true, "MAX": true,
	}

	for _, col := range columns {
		if col.Star {
			continue
		}

		// Check if the column expression is a function call
		if funcCall, ok := col.Expr.(*parser.FunctionCall); ok {
			if aggregateFuncs[funcCall.Name] {
				aggregates = append(aggregates, AggregateExpr{
					FuncName: funcCall.Name,
					Arg:      nil, // Arg would be extracted from funcCall.Args
				})
			}
		}
	}

	return aggregates
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

	case *parser.DerivedTable:
		subPlan, err := BuildPlan(t.Subquery, catalog)
		if err != nil {
			return nil, fmt.Errorf("failed to build plan for derived table: %w", err)
		}

		return &SubqueryScanNode{
			SubqueryPlan: subPlan,
			Alias:        t.Alias,
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
