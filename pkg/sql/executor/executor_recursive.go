package executor

import (
	"fmt"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

// executeRecursiveCTE handles execution of recursive CTEs (UNION ALL)
func (e *Executor) executeRecursiveCTE(cte parser.CTE, op *parser.SetOperation, cteData map[string]*cteResult) (*Result, error) {
	// 1. Execute Anchor (Left)
	anchorResult, err := e.executeSelectWithCTEs(op.Left, cteData)
	if err != nil {
		return nil, fmt.Errorf("failed to execute anchor term: %w", err)
	}

	// Determine columns for recursion
	columns := anchorResult.Columns
	if len(cte.Columns) > 0 {
		if len(cte.Columns) != len(columns) {
			return nil, fmt.Errorf("CTE %s column definition mismatch: defined %d, query returned %d", cte.Name, len(cte.Columns), len(columns))
		}
		columns = cte.Columns
	}

	// Create a copy of cteData for this recursion scope
	localCteData := make(map[string]*cteResult)
	for k, v := range cteData {
		localCteData[k] = v
	}

	// Accumulator for final results
	finalRows := make([][]types.Value, len(anchorResult.Rows))
	copy(finalRows, anchorResult.Rows)

	// Working table (initially anchor results)
	workingRows := anchorResult.Rows

	// Limit recursion depth
	maxDepth := 100
	depth := 0

	for len(workingRows) > 0 {
		if depth > maxDepth {
			return nil, fmt.Errorf("recursion limit exceeded (%d)", maxDepth)
		}
		depth++

		// Update the self-reference in localCteData to point to workingRows
		localCteData[cte.Name] = &cteResult{
			columns: columns,
			rows:    workingRows,
		}

		// Execute Recursive Term (Right)
		recursiveResult, err := e.executeSelectWithCTEs(op.Right, localCteData)
		if err != nil {
			return nil, fmt.Errorf("failed to execute recursive term: %w", err)
		}

		if len(recursiveResult.Rows) == 0 {
			break
		}

		// Check if schema matches
		if len(recursiveResult.Columns) != len(anchorResult.Columns) {
			return nil, fmt.Errorf("recursive term has different column count")
		}

		// Add to final result
		finalRows = append(finalRows, recursiveResult.Rows...)

		// Update working table for next iteration
		workingRows = recursiveResult.Rows
	}

	return &Result{
		Columns: columns,
		Rows:    finalRows,
	}, nil
}

// executeSetOperationWithCTEs executes a set operation with CTE context
func (e *Executor) executeSetOperationWithCTEs(stmt *parser.SetOperation, cteData map[string]*cteResult) (*Result, error) {
	// Execute left and right SELECT statements
	leftResult, err := e.executeSelectWithCTEs(stmt.Left, cteData)
	if err != nil {
		return nil, fmt.Errorf("left query error: %w", err)
	}

	rightResult, err := e.executeSelectWithCTEs(stmt.Right, cteData)
	if err != nil {
		return nil, fmt.Errorf("right query error: %w", err)
	}

	// Use left result's columns as the output columns
	columns := leftResult.Columns

	switch stmt.Operator {
	case parser.SetOpUnion:
		if stmt.All {
			// UNION ALL: Simply concatenate results
			rows := append(leftResult.Rows, rightResult.Rows...)
			return &Result{Columns: columns, Rows: rows}, nil
		}
		// UNION: Concatenate and deduplicate
		rows := e.unionDedup(leftResult.Rows, rightResult.Rows)
		return &Result{Columns: columns, Rows: rows}, nil

	case parser.SetOpIntersect:
		if stmt.All {
			// INTERSECT ALL: Keep duplicates based on count in both
			rows := e.intersectAll(leftResult.Rows, rightResult.Rows)
			return &Result{Columns: columns, Rows: rows}, nil
		}
		// INTERSECT: Keep only rows present in both (deduplicated)
		rows := e.intersect(leftResult.Rows, rightResult.Rows)
		return &Result{Columns: columns, Rows: rows}, nil

	case parser.SetOpExcept:
		if stmt.All {
			// EXCEPT ALL: Remove one copy for each matching right row
			rows := e.exceptAll(leftResult.Rows, rightResult.Rows)
			return &Result{Columns: columns, Rows: rows}, nil
		}
		// EXCEPT: Remove all rows present in right from left
		rows := e.except(leftResult.Rows, rightResult.Rows)
		return &Result{Columns: columns, Rows: rows}, nil

	default:
		return nil, fmt.Errorf("unsupported set operation: %v", stmt.Operator)
	}
}
