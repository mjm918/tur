package executor

import (
	"encoding/binary"
	"fmt"

	"tur/pkg/btree"
	"tur/pkg/record"
	"tur/pkg/schema"
	"tur/pkg/types"
)

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
		// Get B-tree for index
		idxTreeName := "index:" + idx.Name
		tree := e.trees[idxTreeName]
		if tree == nil {
			tree = btree.Open(e.pager, idx.RootPage)
			e.trees[idxTreeName] = tree
		}

		// Build index key values
		var keyValues []types.Value
		for _, colName := range idx.Columns {
			val, ok := valMap[colName]
			if !ok {
				val = types.NewNull()
			}
			keyValues = append(keyValues, val)
		}

		// Encode key
		var key []byte
		var value []byte

		if idx.Unique {
			// Unique index: Key = Columns, Value = RowID
			key = record.Encode(keyValues)

			// Check for uniqueness
			// Note: This check is optimistic. For full correctness in concurrent env,
			// we rely on B-Tree locks or MVCC, but for now we check existence.
			existingVal, err := tree.Get(key)
			if err == nil && existingVal != nil {
				return fmt.Errorf("unique constraint violation: index %s", idx.Name)
			}

			// Encode RowID as value
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, rowID)
			value = buf
		} else {
			// Non-unique index: Key = Columns + RowID, Value = empty
			// Append RowID to key values to make it unique
			keyValues = append(keyValues, types.NewInt(int64(rowID)))
			key = record.Encode(keyValues)
			value = []byte{}
		}

		if err := tree.Insert(key, value); err != nil {
			return fmt.Errorf("failed to update index %s: %w", idx.Name, err)
		}
	}

	return nil
}
