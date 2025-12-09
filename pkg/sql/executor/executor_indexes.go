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
				existingVal, err := tree.Get(key)
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

		if err := tree.Insert(key, value); err != nil {
			return fmt.Errorf("failed to update index %s: %w", idx.Name, err)
		}
	}

	return nil
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
		if err := tree.Delete(key); err != nil {
			// Ignore "key not found" errors as index might not have the entry
			// This can happen for rows inserted before index was created
			continue
		}
	}

	return nil
}
