package executor

import (
	"encoding/binary"
	"fmt"

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
		idxTree := e.trees[idxTreeName]
		if idxTree == nil {
			var err error
			idxTree, err = e.treeFactory.Open(idx.RootPage)
			if err != nil {
				return -1, fmt.Errorf("failed to open index btree %s: %w", idx.Name, err)
			}
			e.trees[idxTreeName] = idxTree
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
		existingVal, err := idxTree.Get(key)
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
	tableTree := e.trees[table.Name]
	if tableTree == nil {
		var err error
		tableTree, err = e.treeFactory.Open(table.RootPage)
		if err != nil {
			return nil, fmt.Errorf("failed to open table btree: %w", err)
		}
		e.trees[table.Name] = tableTree
	}

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(rowID))

	data, err := tableTree.Get(key)
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
	tableTree := e.trees[table.Name]
	if tableTree == nil {
		var err error
		tableTree, err = e.treeFactory.Open(table.RootPage)
		if err != nil {
			return false, fmt.Errorf("failed to open table btree: %w", err)
		}
		e.trees[table.Name] = tableTree
	}

	// Delete old index entries
	if err := e.deleteFromIndexes(table, uint64(rowID), existingValues); err != nil {
		return false, fmt.Errorf("failed to delete old index entries: %w", err)
	}

	// Encode and update the row
	data := record.Encode(updatedValues)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(rowID))

	if err := tableTree.Insert(key, data); err != nil {
		return false, fmt.Errorf("failed to update row: %w", err)
	}

	// Add new index entries
	if err := e.updateIndexes(table, uint64(rowID), updatedValues); err != nil {
		return false, fmt.Errorf("failed to update indexes: %w", err)
	}

	return true, nil
}
