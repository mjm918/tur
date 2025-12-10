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

// reconstructCreateIndexSQL rebuilds CREATE INDEX SQL from parsed statement
func reconstructCreateIndexSQL(stmt *parser.CreateIndexStmt) string {
	var sb strings.Builder
	sb.WriteString("CREATE ")
	if stmt.Unique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	sb.WriteString(stmt.IndexName)
	sb.WriteString(" ON ")
	sb.WriteString(stmt.TableName)
	sb.WriteString("(")
	sb.WriteString(strings.Join(stmt.Columns, ", "))
	sb.WriteString(")")
	return sb.String()
}

// reconstructCreateViewSQL rebuilds CREATE VIEW SQL from parsed statement
func reconstructCreateViewSQL(stmt *parser.CreateViewStmt, selectSQL string) string {
	var sb strings.Builder
	sb.WriteString("CREATE VIEW ")
	sb.WriteString(stmt.ViewName)

	// Add column list if present
	if len(stmt.Columns) > 0 {
		sb.WriteString(" (")
		sb.WriteString(strings.Join(stmt.Columns, ", "))
		sb.WriteString(")")
	}

	sb.WriteString(" AS ")
	sb.WriteString(selectSQL)
	return sb.String()
}

// reconstructCreateTriggerSQL rebuilds CREATE TRIGGER SQL from parsed statement
func reconstructCreateTriggerSQL(stmt *parser.CreateTriggerStmt) string {
	var sb strings.Builder
	sb.WriteString("CREATE TRIGGER ")
	sb.WriteString(stmt.TriggerName)
	sb.WriteString(" ")

	// Timing
	switch stmt.Timing {
	case parser.TriggerBefore:
		sb.WriteString("BEFORE ")
	case parser.TriggerAfter:
		sb.WriteString("AFTER ")
	}

	// Event
	switch stmt.Event {
	case parser.TriggerEventInsert:
		sb.WriteString("INSERT ")
	case parser.TriggerEventUpdate:
		sb.WriteString("UPDATE ")
	case parser.TriggerEventDelete:
		sb.WriteString("DELETE ")
	}

	sb.WriteString("ON ")
	sb.WriteString(stmt.TableName)
	sb.WriteString(" BEGIN ")

	// Reconstruct action statements
	for i, action := range stmt.Actions {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(statementToSQL(action))
		sb.WriteString(";")
	}

	sb.WriteString(" END")
	return sb.String()
}

// statementToSQL converts a parsed statement back to SQL
func statementToSQL(stmt parser.Statement) string {
	switch s := stmt.(type) {
	case *parser.InsertStmt:
		return insertStmtToSQL(s)
	case *parser.UpdateStmt:
		return updateStmtToSQL(s)
	case *parser.DeleteStmt:
		return deleteStmtToSQL(s)
	case *parser.SelectStmt:
		return selectStmtToSQL(s)
	default:
		return ""
	}
}

// insertStmtToSQL converts an INSERT statement to SQL
func insertStmtToSQL(stmt *parser.InsertStmt) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(stmt.TableName)

	if len(stmt.Columns) > 0 {
		sb.WriteString(" (")
		sb.WriteString(strings.Join(stmt.Columns, ", "))
		sb.WriteString(")")
	}

	sb.WriteString(" VALUES ")
	for i, row := range stmt.Values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j, val := range row {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(exprToString(val))
		}
		sb.WriteString(")")
	}

	return sb.String()
}

// updateStmtToSQL converts an UPDATE statement to SQL
func updateStmtToSQL(stmt *parser.UpdateStmt) string {
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(stmt.TableName)
	sb.WriteString(" SET ")

	for i, set := range stmt.Assignments {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(set.Column)
		sb.WriteString(" = ")
		sb.WriteString(exprToString(set.Value))
	}

	if stmt.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(exprToString(stmt.Where))
	}

	return sb.String()
}

// deleteStmtToSQL converts a DELETE statement to SQL
func deleteStmtToSQL(stmt *parser.DeleteStmt) string {
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(stmt.TableName)

	if stmt.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(exprToString(stmt.Where))
	}

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
	// Parse SQL
	p := parser.New(entry.SQL)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse index SQL: %w", err)
	}

	createStmt, ok := stmt.(*parser.CreateIndexStmt)
	if !ok {
		return fmt.Errorf("expected CREATE INDEX statement")
	}

	// Reconstruct index definition
	idx := &schema.IndexDef{
		Name:      entry.Name,
		TableName: entry.TableName,
		Columns:   createStmt.Columns,
		Type:      schema.IndexTypeBTree,
		Unique:    createStmt.Unique,
		RootPage:  entry.RootPage,
	}

	// Add to catalog
	if err := e.catalog.CreateIndex(idx); err != nil {
		return err
	}

	// Open existing B-tree for this index
	tree := btree.Open(e.pager, entry.RootPage)
	idxTreeName := "index:" + entry.Name
	e.trees[idxTreeName] = tree

	return nil
}

// loadViewSchema reconstructs a view from its stored SQL
func (e *Executor) loadViewSchema(entry *dbfile.SchemaEntry) error {
	// Parse the CREATE VIEW SQL
	p := parser.New(entry.SQL)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse view SQL: %w", err)
	}

	createStmt, ok := stmt.(*parser.CreateViewStmt)
	if !ok {
		return fmt.Errorf("expected CREATE VIEW statement")
	}

	// Convert the SELECT statement to SQL text for the view definition
	sql := selectStmtToSQL(createStmt.Query)

	view := &schema.ViewDef{
		Name:    entry.Name,
		SQL:     sql,
		Columns: createStmt.Columns,
	}

	return e.catalog.CreateView(view)
}

// loadTriggerSchema reconstructs a trigger from its stored SQL
func (e *Executor) loadTriggerSchema(entry *dbfile.SchemaEntry) error {
	// Parse the CREATE TRIGGER SQL
	p := parser.New(entry.SQL)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse trigger SQL: %w", err)
	}

	createStmt, ok := stmt.(*parser.CreateTriggerStmt)
	if !ok {
		return fmt.Errorf("expected CREATE TRIGGER statement")
	}

	// Convert parser types to schema types
	var timing schema.TriggerTiming
	switch createStmt.Timing {
	case parser.TriggerBefore:
		timing = schema.TriggerBefore
	case parser.TriggerAfter:
		timing = schema.TriggerAfter
	}

	var event schema.TriggerEvent
	switch createStmt.Event {
	case parser.TriggerEventInsert:
		event = schema.TriggerInsert
	case parser.TriggerEventUpdate:
		event = schema.TriggerUpdate
	case parser.TriggerEventDelete:
		event = schema.TriggerDelete
	}

	// Store parsed action statements
	actions := make([]interface{}, len(createStmt.Actions))
	for i, action := range createStmt.Actions {
		actions[i] = action
	}

	trigger := &schema.TriggerDef{
		Name:      entry.Name,
		TableName: entry.TableName,
		Timing:    timing,
		Event:     event,
		SQL:       entry.SQL,
		Actions:   actions,
	}

	return e.catalog.CreateTrigger(trigger)
}
