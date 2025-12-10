package executor

import (
	"encoding/binary"
	"fmt"

	"tur/pkg/btree"
	"tur/pkg/record"
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

// findConflictingRow checks if inserting the given values would violate
// any PRIMARY KEY or UNIQUE constraint. Returns the rowID of the conflicting
// row if found, or -1 if no conflict exists.
func (e *Executor) findConflictingRow(table *schema.TableDef, values []types.Value) (int64, error) {
	indexes := e.catalog.GetIndexesForTable(table.Name)

	valMap := make(map[string]types.Value)
	for i, col := range table.Columns {
		if i < len(values) {
			valMap[col.Name] = values[i]
		}
	}

	for _, idx := range indexes {
		if !idx.Unique {
			continue
		}

		idxTreeName := "index:" + idx.Name
		tree := e.trees[idxTreeName]
		if tree == nil {
			tree = btree.Open(e.pager, idx.RootPage)
			e.trees[idxTreeName] = tree
		}

		var keyValues []types.Value
		hasNull := false
		for _, colName := range idx.Columns {
			val, ok := valMap[colName]
			if !ok {
				val = types.NewNull()
			}
			if val.IsNull() {
				hasNull = true
				break
			}
			keyValues = append(keyValues, val)
		}

		if hasNull {
			continue
		}

		key := record.Encode(keyValues)
		existingVal, err := tree.Get(key)
		if err != nil {
			continue
		}
		if existingVal != nil && len(existingVal) >= 8 {
			rowID := int64(binary.BigEndian.Uint64(existingVal))
			return rowID, nil
		}
	}

	return -1, nil
}

// getRowByID retrieves a row from the table by its internal rowID
func (e *Executor) getRowByID(table *schema.TableDef, rowID int64) ([]types.Value, error) {
	tree := e.trees[table.Name]
	if tree == nil {
		tree = btree.Open(e.pager, table.RootPage)
		e.trees[table.Name] = tree
	}

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(rowID))

	data, err := tree.Get(key)
	if err != nil {
		return nil, fmt.Errorf("row not found: %w", err)
	}

	values := record.Decode(data)

	return values, nil
}

// executeOnDuplicateUpdate performs the UPDATE part of ON DUPLICATE KEY UPDATE
// Returns true if values were actually changed, false if no change was needed
func (e *Executor) executeOnDuplicateUpdate(
	table *schema.TableDef,
	rowID int64,
	newValues []types.Value,
	assignments []parser.Assignment,
) (bool, error) {
	// Get existing row
	existingValues, err := e.getRowByID(table, rowID)
	if err != nil {
		return false, fmt.Errorf("failed to get existing row: %w", err)
	}

	// Build column map
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[col.Name] = i
	}

	// Build values context for VALUES() function
	valuesCtx := make(map[string]types.Value)
	for i, col := range table.Columns {
		if i < len(newValues) {
			valuesCtx[col.Name] = newValues[i]
		}
	}

	// Set the values context on executor for evaluateExpr
	e.valuesContext = valuesCtx
	defer func() { e.valuesContext = nil }()

	// Create updated values (copy of existing)
	updatedValues := make([]types.Value, len(existingValues))
	copy(updatedValues, existingValues)

	// Apply each assignment
	for _, assign := range assignments {
		colIdx, ok := colMap[assign.Column]
		if !ok {
			return false, fmt.Errorf("column %s not found", assign.Column)
		}

		// Evaluate the expression with current row values
		val, err := e.evaluateExpr(assign.Value, existingValues, colMap)
		if err != nil {
			return false, fmt.Errorf("failed to evaluate assignment: %w", err)
		}

		updatedValues[colIdx] = val
	}

	// Check if any values actually changed
	changed := false
	for i := range existingValues {
		if !valuesEqual(existingValues[i], updatedValues[i]) {
			changed = true
			break
		}
	}

	if !changed {
		return false, nil
	}

	// Validate constraints on updated values
	if err := e.validateConstraints(table, updatedValues); err != nil {
		return false, err
	}

	// Get B-tree for table
	tree := e.trees[table.Name]
	if tree == nil {
		tree = btree.Open(e.pager, table.RootPage)
		e.trees[table.Name] = tree
	}

	// Delete old index entries
	if err := e.deleteFromIndexes(table, uint64(rowID), existingValues); err != nil {
		return false, fmt.Errorf("failed to delete old index entries: %w", err)
	}

	// Encode and update the row
	data := record.Encode(updatedValues)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(rowID))

	if err := tree.Insert(key, data); err != nil {
		return false, fmt.Errorf("failed to update row: %w", err)
	}

	// Add new index entries
	if err := e.updateIndexes(table, uint64(rowID), updatedValues); err != nil {
		return false, fmt.Errorf("failed to update indexes: %w", err)
	}

	return true, nil
}
