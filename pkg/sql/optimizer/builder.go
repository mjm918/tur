package optimizer

import (
	"fmt"
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
)

// CTEInfo holds information about a materialized CTE
type CTEInfo struct {
	Name    string   // CTE name
	Columns []string // Column names
	Rows    int64    // Number of rows (set after materialization)
}

// BuildPlan converts AST to a PlanNode tree
func BuildPlan(stmt *parser.SelectStmt, catalog *schema.Catalog) (PlanNode, error) {
	return BuildPlanWithCTEs(stmt, catalog, nil)
}

// BuildPlanWithCTEs converts AST to a PlanNode tree, with CTE support
func BuildPlanWithCTEs(stmt *parser.SelectStmt, catalog *schema.Catalog, ctes map[string]*CTEInfo) (PlanNode, error) {
	// 1. Build FROM clause (TableScan or Join)
	// stmt.From may be nil for queries like SELECT 1+1 or SELECT function()
	var node PlanNode
	var err error
	if stmt.From == nil {
		// No FROM clause - use DualNode to produce single row
		node = &DualNode{}
	} else {
		node, err = buildTableReferenceWithCTEs(stmt.From, catalog, ctes)
		if err != nil {
			return nil, err
		}
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

	// 4. Apply Projection (Select columns) or Window Functions
	// Skip projection when GROUP BY is present - AggregateNode handles column output
	// Check if SELECT *
	isStar := false
	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		isStar = true
	}

	if !isStar && len(stmt.GroupBy) == 0 {
		var exprs []parser.Expression
		var windowFuncs []*parser.WindowFunction

		for _, col := range stmt.Columns {
			// Convert SelectColumn to Expression
			if col.Expr != nil {
				exprs = append(exprs, col.Expr)

				// Check if this is a window function
				if wf, ok := col.Expr.(*parser.WindowFunction); ok {
					windowFuncs = append(windowFuncs, wf)
				}
			}
		}

		// If window functions present, use WindowNode
		if len(windowFuncs) > 0 {
			node = &WindowNode{
				Input:           node,
				WindowFunctions: windowFuncs,
				AllExpressions:  exprs,
			}
		} else {
			node = &ProjectionNode{
				Input:       node,
				Expressions: exprs,
			}
		}
	}

	// 5. Apply ORDER BY (Sort)
	if len(stmt.OrderBy) > 0 {
		node = &SortNode{
			Input:   node,
			OrderBy: stmt.OrderBy,
		}
	}

	// 7. Apply LIMIT/OFFSET
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
	return buildTableReferenceWithCTEs(ref, catalog, nil)
}

func buildTableReferenceWithCTEs(ref parser.TableReference, catalog *schema.Catalog, ctes map[string]*CTEInfo) (PlanNode, error) {
	switch t := ref.(type) {
	case *parser.Table:
		// First check if this is a CTE reference
		if ctes != nil {
			if cteInfo, ok := ctes[t.Name]; ok {
				return &CTEScanNode{
					CTEName: cteInfo.Name,
					Alias:   t.Alias,
					Columns: cteInfo.Columns,
					Rows:    cteInfo.Rows,
				}, nil
			}
		}

		// Check if it's a view
		viewDef := catalog.GetView(t.Name)
		if viewDef != nil {
			// Parse the view's SQL to get a SelectStmt
			viewParser := parser.New(viewDef.SQL)
			viewStmt, err := viewParser.Parse()
			if err != nil {
				return nil, fmt.Errorf("failed to parse view %s: %w", t.Name, err)
			}

			selectStmt, ok := viewStmt.(*parser.SelectStmt)
			if !ok {
				return nil, fmt.Errorf("view %s does not contain a SELECT statement", t.Name)
			}

			// Build a plan for the view's query
			viewPlan, err := BuildPlanWithCTEs(selectStmt, catalog, ctes)
			if err != nil {
				return nil, fmt.Errorf("failed to build plan for view %s: %w", t.Name, err)
			}

			// Wrap in SubqueryScanNode with the view name as alias (or explicit alias if provided)
			alias := t.Name
			if t.Alias != "" {
				alias = t.Alias
			}

			return &SubqueryScanNode{
				SubqueryPlan: viewPlan,
				Alias:        alias,
			}, nil
		}

		// Otherwise look up in catalog as a table
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
		subPlan, err := BuildPlanWithCTEs(t.Subquery, catalog, ctes)
		if err != nil {
			return nil, fmt.Errorf("failed to build plan for derived table: %w", err)
		}

		return &SubqueryScanNode{
			SubqueryPlan: subPlan,
			Alias:        t.Alias,
		}, nil

	case *parser.TableFunction:
		return &TableFunctionNode{
			Name:  t.Name,
			Args:  t.Args,
			Alias: t.Alias,
		}, nil

	case *parser.Join:
		left, err := buildTableReferenceWithCTEs(t.Left, catalog, ctes)
		if err != nil {
			return nil, err
		}
		right, err := buildTableReferenceWithCTEs(t.Right, catalog, ctes)
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
