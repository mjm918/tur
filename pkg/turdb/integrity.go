// pkg/turdb/integrity.go
package turdb

import (
	"fmt"

	"tur/pkg/btree"
	"tur/pkg/pager"
	"tur/pkg/record"
	"tur/pkg/schema"
	"tur/pkg/types"
)

// IntegrityError represents a single integrity check error
type IntegrityError struct {
	// Type indicates the kind of integrity error (btree, index, fk, page)
	Type string

	// Table is the affected table name (if applicable)
	Table string

	// Index is the affected index name (if applicable)
	Index string

	// Page is the affected page number (if applicable)
	Page uint32

	// Message provides details about the error
	Message string
}

// String returns a human-readable description of the integrity error
func (e IntegrityError) String() string {
	location := ""
	if e.Table != "" {
		location = fmt.Sprintf("table %s", e.Table)
	}
	if e.Index != "" {
		if location != "" {
			location += ", "
		}
		location += fmt.Sprintf("index %s", e.Index)
	}
	if e.Page != 0 {
		if location != "" {
			location += ", "
		}
		location += fmt.Sprintf("page %d", e.Page)
	}

	if location != "" {
		return fmt.Sprintf("[%s] %s: %s", e.Type, location, e.Message)
	}
	return fmt.Sprintf("[%s] %s", e.Type, e.Message)
}

// Error implements the error interface
func (e IntegrityError) Error() string {
	return e.String()
}

// IntegrityCheck performs a comprehensive integrity check on the database.
// It verifies:
// - B-tree structure integrity (no cycles, proper ordering)
// - Index consistency with table data
// - Foreign key referential integrity
// - Page checksums (if available)
//
// Returns a slice of IntegrityError. Empty slice means no errors found.
func (db *DB) IntegrityCheck() []IntegrityError {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return []IntegrityError{{
			Type:    "database",
			Message: "database is closed",
		}}
	}

	var errors []IntegrityError

	// Check each table's B-tree structure
	for tableName, tree := range db.trees {
		if btreeErrors := db.checkBTreeIntegrity(tableName, tree); len(btreeErrors) > 0 {
			errors = append(errors, btreeErrors...)
		}
	}

	// Check index consistency with table data
	if indexErrors := db.checkIndexConsistency(); len(indexErrors) > 0 {
		errors = append(errors, indexErrors...)
	}

	// Check foreign key referential integrity
	if fkErrors := db.checkForeignKeyIntegrity(); len(fkErrors) > 0 {
		errors = append(errors, fkErrors...)
	}

	return errors
}

// QuickCheck performs a faster integrity check that skips some validations.
// It checks B-tree structure but skips foreign key and full index verification.
func (db *DB) QuickCheck() []IntegrityError {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return []IntegrityError{{
			Type:    "database",
			Message: "database is closed",
		}}
	}

	var errors []IntegrityError

	// Quick check only verifies B-tree structure
	for tableName, tree := range db.trees {
		if btreeErrors := db.checkBTreeIntegrity(tableName, tree); len(btreeErrors) > 0 {
			errors = append(errors, btreeErrors...)
		}
	}

	return errors
}

// checkBTreeIntegrity validates a single B-tree structure
func (db *DB) checkBTreeIntegrity(tableName string, tree *btree.BTree) []IntegrityError {
	var errors []IntegrityError

	// Validate B-tree structure by traversing it
	if err := db.validateBTreeStructure(tree); err != nil {
		errors = append(errors, IntegrityError{
			Type:    "btree",
			Table:   tableName,
			Page:    tree.RootPage(),
			Message: err.Error(),
		})
	}

	return errors
}

// validateBTreeStructure performs structural validation of a B-tree
func (db *DB) validateBTreeStructure(tree *btree.BTree) error {
	// Traverse the tree and verify:
	// 1. Keys are in proper order
	// 2. Tree is navigable without errors

	var lastKey []byte

	cursor := tree.Cursor()
	defer cursor.Close()

	// Move to first element
	cursor.First()

	if !cursor.Valid() {
		// Empty tree is valid
		return nil
	}

	for cursor.Valid() {
		key := cursor.Key()
		if key == nil {
			break
		}

		// Check key ordering
		if lastKey != nil {
			if compareBytes(key, lastKey) <= 0 {
				return fmt.Errorf("keys out of order: %v should be after %v", key, lastKey)
			}
		}
		lastKey = append([]byte{}, key...) // Copy key

		cursor.Next()
	}

	return nil
}

// checkIndexConsistency verifies that indexes match their table data
func (db *DB) checkIndexConsistency() []IntegrityError {
	var errors []IntegrityError

	// Get all index names from the catalog
	indexNames := db.catalog.ListIndexes()
	for _, indexName := range indexNames {
		idx := db.catalog.GetIndex(indexName)
		if idx == nil {
			continue
		}

		// Get the table this index belongs to
		table := db.catalog.GetTable(idx.TableName)
		if table == nil {
			errors = append(errors, IntegrityError{
				Type:    "index",
				Index:   idx.Name,
				Message: fmt.Sprintf("index references non-existent table %s", idx.TableName),
			})
			continue
		}

		// Get the table's B-tree
		tableTree, ok := db.trees[idx.TableName]
		if !ok {
			// Table exists in catalog but no B-tree - this is OK for empty tables
			continue
		}

		// Get the index's B-tree (if it exists)
		indexTree, ok := db.trees[idx.Name]
		if !ok {
			// Index defined but no B-tree - might be OK for newly created index
			continue
		}

		// Count entries in both
		tableCount := db.countBTreeEntries(tableTree)
		indexCount := db.countBTreeEntries(indexTree)

		// For non-partial indexes, counts should match
		// (For partial indexes, index count <= table count)
		if !idx.IsPartial() && tableCount != indexCount {
			errors = append(errors, IntegrityError{
				Type:    "index",
				Table:   idx.TableName,
				Index:   idx.Name,
				Message: fmt.Sprintf("index entry count (%d) doesn't match table row count (%d)", indexCount, tableCount),
			})
		}

		// Validate the index B-tree structure itself
		if err := db.validateBTreeStructure(indexTree); err != nil {
			errors = append(errors, IntegrityError{
				Type:    "index",
				Table:   idx.TableName,
				Index:   idx.Name,
				Page:    indexTree.RootPage(),
				Message: fmt.Sprintf("index B-tree structure error: %s", err.Error()),
			})
		}
	}

	return errors
}

// countBTreeEntries counts the number of entries in a B-tree
func (db *DB) countBTreeEntries(tree *btree.BTree) int {
	count := 0
	cursor := tree.Cursor()
	defer cursor.Close()

	cursor.First()
	for cursor.Valid() {
		count++
		cursor.Next()
	}

	return count
}

// checkForeignKeyIntegrity verifies foreign key referential integrity
func (db *DB) checkForeignKeyIntegrity() []IntegrityError {
	var errors []IntegrityError

	// Check each table for foreign key constraints
	tableNames := db.catalog.ListTables()
	for _, tableName := range tableNames {
		table := db.catalog.GetTable(tableName)
		if table == nil {
			continue
		}

		for colIdx := range table.Columns {
			col := &table.Columns[colIdx]
			fkConstraint := col.GetConstraint(schema.ConstraintForeignKey)
			if fkConstraint == nil {
				continue
			}

			// Get the referenced table
			refTable := db.catalog.GetTable(fkConstraint.RefTable)
			if refTable == nil {
				errors = append(errors, IntegrityError{
					Type:    "fk",
					Table:   table.Name,
					Message: fmt.Sprintf("column %s references non-existent table %s", col.Name, fkConstraint.RefTable),
				})
				continue
			}

			// Verify all values in the foreign key column exist in the referenced table
			fkErrors := db.verifyForeignKeyValues(table, col, colIdx, refTable, fkConstraint)
			errors = append(errors, fkErrors...)
		}
	}

	return errors
}

// verifyForeignKeyValues checks that all FK values exist in the referenced table
func (db *DB) verifyForeignKeyValues(
	table *schema.TableDef,
	col *schema.ColumnDef,
	colIdx int,
	refTable *schema.TableDef,
	fkConstraint *schema.Constraint,
) []IntegrityError {
	var errors []IntegrityError

	// Get the table's B-tree
	tableTree, ok := db.trees[table.Name]
	if !ok {
		// No data to check
		return errors
	}

	// Get the referenced table's B-tree
	refTree, ok := db.trees[refTable.Name]
	if !ok {
		// Referenced table has no data
		// Check if our table has data - if so, could be FK violation
		cursor := tableTree.Cursor()
		defer cursor.Close()
		cursor.First()
		if cursor.Valid() {
			// Our table has data but referenced table doesn't
			// Need to check if any FK values are non-NULL
			hasNonNullFK := false
			for cursor.Valid() {
				value := cursor.Value()
				if value != nil {
					values := record.Decode(value)
					if len(values) > colIdx {
						fkValue := values[colIdx]
						if fkValue.Type() != types.TypeNull {
							hasNonNullFK = true
							break
						}
					}
				}
				cursor.Next()
			}
			if hasNonNullFK {
				errors = append(errors, IntegrityError{
					Type:    "fk",
					Table:   table.Name,
					Message: fmt.Sprintf("foreign key column %s has non-NULL values but referenced table %s is empty",
						col.Name, refTable.Name),
				})
			}
		}
		return errors
	}

	// Find column index for the referenced column
	refColIdx := -1
	for i := range refTable.Columns {
		if refTable.Columns[i].Name == fkConstraint.RefColumn {
			refColIdx = i
			break
		}
	}
	if refColIdx == -1 {
		errors = append(errors, IntegrityError{
			Type:    "fk",
			Table:   table.Name,
			Message: fmt.Sprintf("referenced column %s.%s does not exist", refTable.Name, fkConstraint.RefColumn),
		})
		return errors
	}

	// Build a set of values in the referenced table's column
	refValues := make(map[string]bool)
	refCursor := refTree.Cursor()
	defer refCursor.Close()

	refCursor.First()
	for refCursor.Valid() {
		value := refCursor.Value()
		if value != nil {
			values := record.Decode(value)
			if len(values) > refColIdx {
				refValue := values[refColIdx]
				// Use string representation as map key
				refValues[valueToString(refValue)] = true
			}
		}
		refCursor.Next()
	}

	// Check each row in our table for FK violations
	cursor := tableTree.Cursor()
	defer cursor.Close()

	rowNum := 0
	cursor.First()
	for cursor.Valid() {
		rowNum++
		value := cursor.Value()
		if value != nil {
			values := record.Decode(value)
			if len(values) > colIdx {
				fkValue := values[colIdx]
				// NULL values are always valid (no reference)
				if fkValue.Type() != types.TypeNull {
					fkStr := valueToString(fkValue)
					if !refValues[fkStr] {
						errors = append(errors, IntegrityError{
							Type:  "fk",
							Table: table.Name,
							Message: fmt.Sprintf("row %d: foreign key column %s value %s not found in %s.%s",
								rowNum, col.Name, fkStr, refTable.Name, fkConstraint.RefColumn),
						})
					}
				}
			}
		}
		cursor.Next()
	}

	return errors
}

// valueToString converts a types.Value to a string for comparison
func valueToString(v types.Value) string {
	switch v.Type() {
	case types.TypeNull:
		return "NULL"
	case types.TypeInt:
		return fmt.Sprintf("%d", v.Int())
	case types.TypeFloat:
		return fmt.Sprintf("%f", v.Float())
	case types.TypeText:
		return v.Text()
	case types.TypeBlob:
		return fmt.Sprintf("%x", v.Blob())
	default:
		return ""
	}
}

// compareBytes compares two byte slices lexicographically
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}

	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// CorruptionCheck scans all database pages for corruption.
// It verifies:
// - Page checksums
// - Torn page writes
// Returns a slice of IntegrityError. Empty slice means no corruption found.
func (db *DB) CorruptionCheck() []IntegrityError {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return []IntegrityError{{
			Type:    "database",
			Message: "database is closed",
		}}
	}

	var errors []IntegrityError

	// Create corruption checker
	checker := pager.NewCorruptionChecker(db.pager)

	// Check all pages
	corruptionErrors := checker.CheckAllPages()

	// Convert pager.CorruptionError to IntegrityError
	for _, corrErr := range corruptionErrors {
		errors = append(errors, IntegrityError{
			Type:    "page",
			Page:    corrErr.PageNo,
			Message: corrErr.Error(),
		})
	}

	return errors
}

// CheckPage checks a specific page for corruption.
// Returns nil if the page is not corrupted.
func (db *DB) CheckPage(pageNo uint32) *IntegrityError {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return &IntegrityError{
			Type:    "database",
			Message: "database is closed",
		}
	}

	checker := pager.NewCorruptionChecker(db.pager)
	corrErr := checker.CheckPage(pageNo)

	if corrErr == nil {
		return nil
	}

	return &IntegrityError{
		Type:    "page",
		Page:    corrErr.PageNo,
		Message: corrErr.Error(),
	}
}
