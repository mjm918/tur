package executor

import (
	"encoding/binary"

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
