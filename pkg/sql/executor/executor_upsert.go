package executor

import (
	"encoding/binary"
	"fmt"

	"tur/pkg/btree"
	"tur/pkg/record"
	"tur/pkg/schema"
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
