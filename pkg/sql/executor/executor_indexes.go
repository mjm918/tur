package executor

import (
	"encoding/binary"
	"fmt"
	"strings"

	"tur/pkg/record"
	"tur/pkg/schema"
	"tur/pkg/sql/lexer"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
	"tur/pkg/vdbe"
)

// matchesPartialIndexPredicate evaluates whether a row matches the partial index's
// WHERE clause. Returns true if the index is not partial or if the row matches.
func (e *Executor) matchesPartialIndexPredicate(idx *schema.IndexDef, table *schema.TableDef, values []types.Value) (bool, error) {
	// Non-partial indexes match all rows
	if !idx.IsPartial() {
		return true, nil
	}

	// Parse the WHERE clause SQL using a SELECT statement
	// We need a FROM clause, so use a dummy table name
	whereSQL := "SELECT 1 FROM _dummy WHERE " + idx.WhereClause
	p := parser.New(whereSQL)
	stmt, err := p.Parse()
	if err != nil {
		return false, fmt.Errorf("failed to parse partial index predicate: %w", err)
	}

	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt.Where == nil {
		return false, fmt.Errorf("invalid partial index predicate")
	}

	// Build column index map
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[col.Name] = i
	}

	// Evaluate the predicate
	return e.evaluateCondition(selectStmt.Where, values, colMap)
}

// updateIndexes updates all indexes for the table with the new row
func (e *Executor) updateIndexes(table *schema.TableDef, rowID uint64, values []types.Value) error {
	indexes := e.catalog.GetIndexesForTable(table.Name)
	if len(indexes) == 0 {
		return nil
	}

	// Map column name to value for easy access
	valMap := make(map[string]types.Value)
	for i, col := range table.Columns {
		if i < len(values) {
			valMap[col.Name] = values[i]
		}
	}

	for _, idx := range indexes {
		// For partial indexes, check if row matches the predicate
		matches, err := e.matchesPartialIndexPredicate(idx, table, values)
		if err != nil {
			return fmt.Errorf("failed to evaluate partial index predicate: %w", err)
		}
		if !matches {
			// Row doesn't match partial index predicate, skip indexing
			continue
		}
		// Get B-tree for index
		idxTreeName := "index:" + idx.Name
		idxTree := e.trees[idxTreeName]
		if idxTree == nil {
			var err error
			idxTree, err = e.treeFactory.Open(idx.RootPage)
			if err != nil {
				return fmt.Errorf("failed to open index btree %s: %w", idx.Name, err)
			}
			e.trees[idxTreeName] = idxTree
		}

		// Build index key values from plain columns
		var keyValues []types.Value
		for _, colName := range idx.Columns {
			val, ok := valMap[colName]
			if !ok {
				val = types.NewNull()
			}
			keyValues = append(keyValues, val)
		}

		// Add expression values if this is an expression index
		if idx.IsExpressionIndex() {
			exprValues, err := evaluateIndexExpressions(idx.Expressions, valMap)
			if err != nil {
				return fmt.Errorf("failed to evaluate expression for index %s: %w", idx.Name, err)
			}
			keyValues = append(keyValues, exprValues...)
		}

		// Encode key
		var key []byte
		var value []byte

		if idx.Unique {
			// Check if any column is NULL
			// SQL standard: Multiple NULL values are allowed in unique indexes
			hasNull := false
			for _, kv := range keyValues {
				if kv.IsNull() {
					hasNull = true
					break
				}
			}

			if hasNull {
				// For rows with NULL values, we need to include rowID in key
				// to allow multiple NULLs (since each gets a unique key)
				keyValuesWithRowID := append([]types.Value{}, keyValues...)
				keyValuesWithRowID = append(keyValuesWithRowID, types.NewInt(int64(rowID)))
				key = record.Encode(keyValuesWithRowID)
				// Value is empty since rowID is in the key
				value = []byte{}
			} else {
				// Unique index with no NULLs: Key = Columns, Value = RowID
				key = record.Encode(keyValues)

				// Note: This check is optimistic. For full correctness in concurrent env,
				// we rely on B-Tree locks or MVCC, but for now we check existence.
				existingVal, err := idxTree.Get(key)
				if err == nil && existingVal != nil {
					return fmt.Errorf("unique constraint violation: index %s", idx.Name)
				}

				// Encode RowID as value
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, rowID)
				value = buf
			}
		} else {
			// Non-unique index: Key = Columns + RowID, Value = empty
			// Append RowID to key values to make it unique
			keyValues = append(keyValues, types.NewInt(int64(rowID)))
			key = record.Encode(keyValues)
			value = []byte{}
		}

		if err := idxTree.Insert(key, value); err != nil {
			return fmt.Errorf("failed to update index %s: %w", idx.Name, err)
		}
	}

	return nil
}

// evaluateIndexExpressions parses and evaluates expression strings against row values
func evaluateIndexExpressions(exprStrings []string, valMap map[string]types.Value) ([]types.Value, error) {
	funcRegistry := vdbe.DefaultFunctionRegistry()
	var results []types.Value

	for _, exprSQL := range exprStrings {
		// Parse the expression
		p := parser.New(exprSQL)
		expr, err := p.ParseExpression()
		if err != nil {
			return nil, fmt.Errorf("failed to parse expression %q: %w", exprSQL, err)
		}

		// Evaluate the expression
		val, err := evaluateExpr(expr, valMap, funcRegistry)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate expression %q: %w", exprSQL, err)
		}
		results = append(results, val)
	}

	return results, nil
}

// evaluateExpr evaluates a parsed expression against row values
func evaluateExpr(expr parser.Expression, valMap map[string]types.Value, funcRegistry *vdbe.FunctionRegistry) (types.Value, error) {
	switch e := expr.(type) {
	case *parser.Literal:
		return e.Value, nil

	case *parser.ColumnRef:
		// Look up column value (case-insensitive)
		for colName, val := range valMap {
			if strings.EqualFold(colName, e.Name) {
				return val, nil
			}
		}
		return types.NewNull(), nil

	case *parser.FunctionCall:
		// Evaluate function arguments
		args := make([]types.Value, len(e.Args))
		for i, arg := range e.Args {
			val, err := evaluateExpr(arg, valMap, funcRegistry)
			if err != nil {
				return types.NewNull(), err
			}
			args[i] = val
		}

		// Look up and call the function
		fn := funcRegistry.Lookup(e.Name)
		if fn == nil {
			return types.NewNull(), fmt.Errorf("unknown function: %s", e.Name)
		}
		return fn.Call(args), nil

	case *parser.BinaryExpr:
		// Evaluate left and right operands
		left, err := evaluateExpr(e.Left, valMap, funcRegistry)
		if err != nil {
			return types.NewNull(), err
		}
		right, err := evaluateExpr(e.Right, valMap, funcRegistry)
		if err != nil {
			return types.NewNull(), err
		}

		// Handle NULL propagation for arithmetic
		if left.IsNull() || right.IsNull() {
			return types.NewNull(), nil
		}

		// Evaluate based on operator
		switch e.Op {
		case lexer.PLUS:
			return evalArithmetic(left, right, "+")
		case lexer.MINUS:
			return evalArithmetic(left, right, "-")
		case lexer.STAR:
			return evalArithmetic(left, right, "*")
		case lexer.SLASH:
			return evalArithmetic(left, right, "/")
		default:
			return types.NewNull(), fmt.Errorf("unsupported operator in index expression: %v", e.Op)
		}

	case *parser.UnaryExpr:
		right, err := evaluateExpr(e.Right, valMap, funcRegistry)
		if err != nil {
			return types.NewNull(), err
		}
		if right.IsNull() {
			return types.NewNull(), nil
		}
		if e.Op == lexer.MINUS {
			switch right.Type() {
			case types.TypeInt:
				return types.NewInt(-right.Int()), nil
			case types.TypeFloat:
				return types.NewFloat(-right.Float()), nil
			}
		}
		return types.NewNull(), fmt.Errorf("unsupported unary operator: %v", e.Op)

	default:
		return types.NewNull(), fmt.Errorf("unsupported expression type in index: %T", expr)
	}
}

// evalArithmetic performs arithmetic operations on two values
func evalArithmetic(left, right types.Value, op string) (types.Value, error) {
	// Convert to float if either operand is float
	if left.Type() == types.TypeFloat || right.Type() == types.TypeFloat {
		var l, r float64
		switch left.Type() {
		case types.TypeInt:
			l = float64(left.Int())
		case types.TypeFloat:
			l = left.Float()
		default:
			return types.NewNull(), nil
		}
		switch right.Type() {
		case types.TypeInt:
			r = float64(right.Int())
		case types.TypeFloat:
			r = right.Float()
		default:
			return types.NewNull(), nil
		}

		switch op {
		case "+":
			return types.NewFloat(l + r), nil
		case "-":
			return types.NewFloat(l - r), nil
		case "*":
			return types.NewFloat(l * r), nil
		case "/":
			if r == 0 {
				return types.NewNull(), nil
			}
			return types.NewFloat(l / r), nil
		}
	}

	// Integer arithmetic
	if left.Type() == types.TypeInt && right.Type() == types.TypeInt {
		l, r := left.Int(), right.Int()
		switch op {
		case "+":
			return types.NewInt(l + r), nil
		case "-":
			return types.NewInt(l - r), nil
		case "*":
			return types.NewInt(l * r), nil
		case "/":
			if r == 0 {
				return types.NewNull(), nil
			}
			return types.NewInt(l / r), nil
		}
	}

	return types.NewNull(), nil
}

// deleteFromIndexes removes index entries for a deleted row
func (e *Executor) deleteFromIndexes(table *schema.TableDef, rowID uint64, values []types.Value) error {
	indexes := e.catalog.GetIndexesForTable(table.Name)
	if len(indexes) == 0 {
		return nil
	}

	// Map column name to value for easy access
	valMap := make(map[string]types.Value)
	for i, col := range table.Columns {
		if i < len(values) {
			valMap[col.Name] = values[i]
		}
	}

	for _, idx := range indexes {
		// For partial indexes, check if row matches the predicate
		// Only need to delete if the row was in the index
		matches, err := e.matchesPartialIndexPredicate(idx, table, values)
		if err != nil {
			return fmt.Errorf("failed to evaluate partial index predicate: %w", err)
		}
		if !matches {
			// Row didn't match partial index predicate, wasn't in index
			continue
		}

		// Get B-tree for index
		idxTreeName := "index:" + idx.Name
		idxTree := e.trees[idxTreeName]
		if idxTree == nil {
			var err error
			idxTree, err = e.treeFactory.Open(idx.RootPage)
			if err != nil {
				return fmt.Errorf("failed to open index btree %s: %w", idx.Name, err)
			}
			e.trees[idxTreeName] = idxTree
		}

		// Build index key values from plain columns
		var keyValues []types.Value
		for _, colName := range idx.Columns {
			val, ok := valMap[colName]
			if !ok {
				val = types.NewNull()
			}
			keyValues = append(keyValues, val)
		}

		// Add expression values if this is an expression index
		if idx.IsExpressionIndex() {
			exprValues, err := evaluateIndexExpressions(idx.Expressions, valMap)
			if err != nil {
				// If we can't evaluate expressions, skip this index
				continue
			}
			keyValues = append(keyValues, exprValues...)
		}

		// Build key (same logic as updateIndexes)
		var key []byte
		if idx.Unique {
			// Check if any column is NULL
			hasNull := false
			for _, kv := range keyValues {
				if kv.IsNull() {
					hasNull = true
					break
				}
			}

			if hasNull {
				// For rows with NULL values, rowID is part of the key
				keyValuesWithRowID := append([]types.Value{}, keyValues...)
				keyValuesWithRowID = append(keyValuesWithRowID, types.NewInt(int64(rowID)))
				key = record.Encode(keyValuesWithRowID)
			} else {
				// Unique index with no NULLs: Key = Columns only
				key = record.Encode(keyValues)
			}
		} else {
			// Non-unique index: Key = Columns + RowID
			keyValues = append(keyValues, types.NewInt(int64(rowID)))
			key = record.Encode(keyValues)
		}

		// Delete from index
		if err := idxTree.Delete(key); err != nil {
			// Ignore "key not found" errors as index might not have the entry
			// This can happen for rows inserted before index was created
			continue
		}
	}

	return nil
}
