package executor

import (
	"fmt"
	"strings"

	"tur/pkg/btree"
	"tur/pkg/dbfile"
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
)

// initSchemaBTree initializes or opens the schema metadata B-tree on page 1
func (e *Executor) initSchemaBTree() error {
	// Check if this is an existing database (has pages beyond page 0)
	if e.pager.PageCount() > 1 {
		// Existing database - open schema B-tree from page 1
		tree := btree.Open(e.pager, 1)
		e.schemaBTree = tree

		// Load schema entries into catalog
		return e.loadSchemaFromBTree()
	}

	// New database - create schema B-tree on page 1
	tree, err := btree.CreateAtPage(e.pager, 1)
	if err != nil {
		return err
	}
	e.schemaBTree = tree
	return nil
}

// persistSchemaEntry writes a schema entry to the schema B-tree
func (e *Executor) persistSchemaEntry(entry *dbfile.SchemaEntry) error {
	if e.schemaBTree == nil {
		return fmt.Errorf("schema B-tree not initialized")
	}

	// Encode entry to binary
	data := entry.Encode()

	// Use entry name as key
	key := []byte(entry.Name)

	// Insert into schema B-tree
	return e.schemaBTree.Insert(key, data)
}

// getSchemaEntry retrieves a schema entry by name
func (e *Executor) getSchemaEntry(name string) (*dbfile.SchemaEntry, error) {
	if e.schemaBTree == nil {
		return nil, fmt.Errorf("schema B-tree not initialized")
	}

	key := []byte(name)
	value, err := e.schemaBTree.Get(key)
	if err != nil {
		if err == btree.ErrKeyNotFound {
			return nil, fmt.Errorf("schema entry %s not found", name)
		}
		return nil, err
	}

	return dbfile.DecodeSchemaEntry(value)
}

// deleteSchemaEntry removes a schema entry from the schema B-tree
func (e *Executor) deleteSchemaEntry(name string) error {
	if e.schemaBTree == nil {
		return fmt.Errorf("schema B-tree not initialized")
	}

	key := []byte(name)
	return e.schemaBTree.Delete(key)
}

// reconstructCreateTableSQL rebuilds CREATE TABLE SQL from parsed statement
func reconstructCreateTableSQL(stmt *parser.CreateTableStmt) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(stmt.TableName)
	sb.WriteString(" (")

	for i, col := range stmt.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.Name)
		sb.WriteString(" ")
		sb.WriteString(col.Type.String())

		if col.PrimaryKey {
			sb.WriteString(" PRIMARY KEY")
		}
		if col.NotNull {
			sb.WriteString(" NOT NULL")
		}
		if col.Unique {
			sb.WriteString(" UNIQUE")
		}
	}

	sb.WriteString(")")
	return sb.String()
}

// loadSchemaFromBTree reads all schema entries from page 1 and populates the catalog
func (e *Executor) loadSchemaFromBTree() error {
	// Use cursor to iterate over all entries in the schema B-tree
	cursor := e.schemaBTree.Cursor()
	defer cursor.Close()

	cursor.First()
	for cursor.Valid() {
		value := cursor.Value()
		if value == nil {
			cursor.Next()
			continue
		}

		// Decode schema entry
		entry, err := dbfile.DecodeSchemaEntry(value)
		if err != nil {
			return fmt.Errorf("failed to decode schema entry: %w", err)
		}

		// Add to catalog based on type
		if err := e.addSchemaEntryToCatalog(entry); err != nil {
			return fmt.Errorf("failed to load schema entry %s: %w", entry.Name, err)
		}

		cursor.Next()
	}

	return nil
}

// addSchemaEntryToCatalog adds a decoded schema entry to the in-memory catalog
func (e *Executor) addSchemaEntryToCatalog(entry *dbfile.SchemaEntry) error {
	switch entry.Type {
	case dbfile.SchemaEntryTable:
		return e.loadTableSchema(entry)
	case dbfile.SchemaEntryIndex:
		return e.loadIndexSchema(entry)
	case dbfile.SchemaEntryView:
		return e.loadViewSchema(entry)
	case dbfile.SchemaEntryTrigger:
		return e.loadTriggerSchema(entry)
	default:
		return fmt.Errorf("unknown schema type: %d", entry.Type)
	}
}

// loadTableSchema reconstructs a table from its stored SQL
func (e *Executor) loadTableSchema(entry *dbfile.SchemaEntry) error {
	// Parse SQL to reconstruct table definition
	p := parser.New(entry.SQL)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse table SQL: %w", err)
	}

	createStmt, ok := stmt.(*parser.CreateTableStmt)
	if !ok {
		return fmt.Errorf("expected CREATE TABLE statement")
	}

	// Convert parser columns to schema columns
	columns := make([]schema.ColumnDef, len(createStmt.Columns))
	for i, col := range createStmt.Columns {
		columns[i] = schema.ColumnDef{
			Name:       col.Name,
			Type:       col.Type,
			PrimaryKey: col.PrimaryKey,
			NotNull:    col.NotNull,
			VectorDim:  col.VectorDim,
		}

		// Build constraints
		var constraints []schema.Constraint
		if col.PrimaryKey {
			constraints = append(constraints, schema.Constraint{Type: schema.ConstraintPrimaryKey})
		}
		if col.NotNull {
			constraints = append(constraints, schema.Constraint{Type: schema.ConstraintNotNull})
		}
		if col.Unique {
			constraints = append(constraints, schema.Constraint{Type: schema.ConstraintUnique})
		}
		columns[i].Constraints = constraints
	}

	// Create table definition
	table := &schema.TableDef{
		Name:     entry.Name,
		Columns:  columns,
		RootPage: entry.RootPage,
	}

	// Add to catalog
	if err := e.catalog.CreateTable(table); err != nil {
		return err
	}

	// Open existing B-tree for this table
	tree := btree.Open(e.pager, entry.RootPage)
	e.trees[entry.Name] = tree
	e.rowid[entry.Name] = 1 // Will be updated when scanning

	return nil
}

// loadIndexSchema reconstructs an index from its stored SQL
func (e *Executor) loadIndexSchema(entry *dbfile.SchemaEntry) error {
	// TODO: Implement index loading
	return nil
}

// loadViewSchema reconstructs a view from its stored SQL
func (e *Executor) loadViewSchema(entry *dbfile.SchemaEntry) error {
	// TODO: Implement view loading
	return nil
}

// loadTriggerSchema reconstructs a trigger from its stored SQL
func (e *Executor) loadTriggerSchema(entry *dbfile.SchemaEntry) error {
	// TODO: Implement trigger loading
	return nil
}
