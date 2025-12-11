package executor

import (
	"encoding/binary"
	"fmt"
	"strings"

	"tur/pkg/dbfile"
	"tur/pkg/schema"
	"tur/pkg/sql/parser"
	"tur/pkg/tree"
)

// initSchemaBTree initializes or opens the schema metadata B-tree on page 1
func (e *Executor) initSchemaBTree() error {
	// Check if this is an existing database (has pages beyond page 0)
	if e.pager.PageCount() > 1 {
		// Existing database - open schema B-tree from page 1
		schemaTree, err := e.treeFactory.Open(1)
		if err != nil {
			return fmt.Errorf("failed to open schema B-tree: %w", err)
		}
		e.schemaBTree = schemaTree

		// Load schema entries into catalog
		return e.loadSchemaFromBTree()
	}

	// New database - create schema B-tree on page 1
	schemaTree, err := e.treeFactory.CreateAtPage(1)
	if err != nil {
		return err
	}
	e.schemaBTree = schemaTree
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
		if err == tree.ErrKeyNotFound {
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

// syncTableRootPage checks if a table's btree root page has changed and updates the schema
// This must be called after any operation that might cause a btree split (Insert, Delete)
func (e *Executor) syncTableRootPage(tableName string) error {
	tableTree := e.trees[tableName]
	if tableTree == nil {
		return nil // No tree to sync
	}

	table := e.catalog.GetTable(tableName)
	if table == nil {
		return nil // Table not found
	}

	// Check if root page changed
	currentRootPage := tableTree.RootPage()
	if currentRootPage == table.RootPage {
		return nil // No change
	}

	// Root page changed - update schema entry
	entry, err := e.getSchemaEntry(tableName)
	if err != nil {
		return fmt.Errorf("failed to get schema entry for %s: %w", tableName, err)
	}

	// Update the root page in the entry
	entry.RootPage = currentRootPage

	// Write back to schema btree
	if err := e.persistSchemaEntry(entry); err != nil {
		return fmt.Errorf("failed to update schema root page for %s: %w", tableName, err)
	}

	// Update in-memory table definition
	table.RootPage = currentRootPage

	return nil
}

// syncIndexRootPage checks if an index's btree root page has changed and updates the schema
func (e *Executor) syncIndexRootPage(indexName string, idx *schema.IndexDef) error {
	idxTreeName := "idx_" + indexName
	indexTree := e.trees[idxTreeName]
	if indexTree == nil {
		return nil // No tree to sync
	}

	// Check if root page changed
	currentRootPage := indexTree.RootPage()
	if currentRootPage == idx.RootPage {
		return nil // No change
	}

	// Root page changed - update schema entry
	entry, err := e.getSchemaEntry(indexName)
	if err != nil {
		return fmt.Errorf("failed to get schema entry for index %s: %w", indexName, err)
	}

	// Update the root page in the entry
	entry.RootPage = currentRootPage

	// Write back to schema btree
	if err := e.persistSchemaEntry(entry); err != nil {
		return fmt.Errorf("failed to update schema root page for index %s: %w", indexName, err)
	}

	// Update in-memory index definition
	idx.RootPage = currentRootPage

	return nil
}

// syncAllRootPages syncs root pages for all open trees to their schema entries
func (e *Executor) syncAllRootPages() error {
	// Sync all tables
	for tableName := range e.trees {
		// Skip index trees (they start with "idx_")
		if len(tableName) > 4 && tableName[:4] == "idx_" {
			continue
		}
		if err := e.syncTableRootPage(tableName); err != nil {
			return err
		}
	}

	// Sync all indexes
	tables := e.catalog.ListTables()
	for _, tableName := range tables {
		indexes := e.catalog.GetIndexesForTable(tableName)
		for _, idx := range indexes {
			if err := e.syncIndexRootPage(idx.Name, idx); err != nil {
				return err
			}
		}
	}

	return nil
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
	case dbfile.SchemaEntryProcedure:
		return e.loadProcedureSchema(entry)
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
	tableTree, err := e.treeFactory.Open(entry.RootPage)
	if err != nil {
		return fmt.Errorf("failed to open table B-tree: %w", err)
	}
	e.trees[entry.Name] = tableTree

	// Scan B-tree to find the maximum rowid for proper ID continuation on inserts
	maxRowid := uint64(0)
	cursor := tableTree.Cursor()
	for cursor.First(); cursor.Valid(); cursor.Next() {
		key := cursor.Key()
		if len(key) >= 8 {
			rowid := binary.BigEndian.Uint64(key)
			if rowid > maxRowid {
				maxRowid = rowid
			}
		}
	}
	cursor.Close()
	e.rowid[entry.Name] = maxRowid + 1

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
	indexTree, err := e.treeFactory.Open(entry.RootPage)
	if err != nil {
		return fmt.Errorf("failed to open index B-tree: %w", err)
	}
	idxTreeName := "index:" + entry.Name
	e.trees[idxTreeName] = indexTree

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

// loadProcedureSchema reconstructs a procedure from its stored SQL
func (e *Executor) loadProcedureSchema(entry *dbfile.SchemaEntry) error {
	// Parse the CREATE PROCEDURE SQL
	p := parser.New(entry.SQL)
	stmt, err := p.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse procedure SQL: %w", err)
	}

	createStmt, ok := stmt.(*parser.CreateProcedureStmt)
	if !ok {
		return fmt.Errorf("expected CREATE PROCEDURE statement")
	}

	// Convert parser parameter types to schema types
	params := make([]schema.ProcedureParam, len(createStmt.Parameters))
	for i, param := range createStmt.Parameters {
		params[i] = schema.ProcedureParam{
			Name: param.Name,
			Type: param.Type,
		}
		switch param.Mode {
		case parser.ParamModeIn:
			params[i].Mode = schema.ParamModeIn
		case parser.ParamModeOut:
			params[i].Mode = schema.ParamModeOut
		case parser.ParamModeInOut:
			params[i].Mode = schema.ParamModeInOut
		}
	}

	// Convert body statements to interface{} slice
	body := make([]interface{}, len(createStmt.Body))
	for i, s := range createStmt.Body {
		body[i] = s
	}

	proc := &schema.ProcedureDef{
		Name:       entry.Name,
		Parameters: params,
		SQL:        entry.SQL,
		Body:       body,
	}

	return e.catalog.CreateProcedure(proc)
}

// reconstructCreateProcedureSQL rebuilds CREATE PROCEDURE SQL from parsed statement
func reconstructCreateProcedureSQL(stmt *parser.CreateProcedureStmt) string {
	var sb strings.Builder
	sb.WriteString("CREATE PROCEDURE ")
	sb.WriteString(stmt.Name)
	sb.WriteString("(")

	// Parameters
	for i, param := range stmt.Parameters {
		if i > 0 {
			sb.WriteString(", ")
		}
		// Write mode
		switch param.Mode {
		case parser.ParamModeIn:
			sb.WriteString("IN ")
		case parser.ParamModeOut:
			sb.WriteString("OUT ")
		case parser.ParamModeInOut:
			sb.WriteString("INOUT ")
		}
		sb.WriteString(param.Name)
		sb.WriteString(" ")
		sb.WriteString(param.Type.String())
	}

	sb.WriteString(") BEGIN ")

	// Body statements
	for i, bodyStmt := range stmt.Body {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(procedureStatementToSQL(bodyStmt))
		sb.WriteString(";")
	}

	sb.WriteString(" END")
	return sb.String()
}

// procedureStatementToSQL converts a procedure body statement to SQL
func procedureStatementToSQL(stmt parser.Statement) string {
	switch s := stmt.(type) {
	case *parser.InsertStmt:
		return insertStmtToSQL(s)
	case *parser.UpdateStmt:
		return updateStmtToSQL(s)
	case *parser.DeleteStmt:
		return deleteStmtToSQL(s)
	case *parser.SelectStmt:
		return selectStmtToSQL(s)
	case *parser.SetStmt:
		return setStmtToSQL(s)
	case *parser.DeclareStmt:
		return declareStmtToSQL(s)
	case *parser.LoopStmt:
		return loopStmtToSQL(s)
	case *parser.LeaveStmt:
		return "LEAVE " + s.Label
	case *parser.IfStmt:
		return ifStmtToSQL(s)
	default:
		return ""
	}
}

// setStmtToSQL converts a SET statement to SQL
func setStmtToSQL(stmt *parser.SetStmt) string {
	var sb strings.Builder
	sb.WriteString("SET ")
	sb.WriteString(exprToString(stmt.Variable))
	sb.WriteString(" = ")
	sb.WriteString(exprToString(stmt.Value))
	return sb.String()
}

// declareStmtToSQL converts a DECLARE statement to SQL
func declareStmtToSQL(stmt *parser.DeclareStmt) string {
	var sb strings.Builder
	sb.WriteString("DECLARE ")
	sb.WriteString(stmt.Name)
	sb.WriteString(" ")
	sb.WriteString(stmt.Type.String())
	if stmt.DefaultValue != nil {
		sb.WriteString(" DEFAULT ")
		sb.WriteString(exprToString(stmt.DefaultValue))
	}
	return sb.String()
}

// loopStmtToSQL converts a LOOP statement to SQL
func loopStmtToSQL(stmt *parser.LoopStmt) string {
	var sb strings.Builder
	if stmt.Label != "" {
		sb.WriteString(stmt.Label)
		sb.WriteString(": ")
	}
	sb.WriteString("LOOP ")
	for i, bodyStmt := range stmt.Body {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(procedureStatementToSQL(bodyStmt))
		sb.WriteString(";")
	}
	sb.WriteString(" END LOOP")
	return sb.String()
}

// ifStmtToSQL converts an IF statement to SQL
func ifStmtToSQL(stmt *parser.IfStmt) string {
	var sb strings.Builder
	sb.WriteString("IF ")
	sb.WriteString(exprToString(stmt.Condition))
	sb.WriteString(" THEN ")
	for i, thenStmt := range stmt.ThenBranch {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(procedureStatementToSQL(thenStmt))
		sb.WriteString(";")
	}
	for _, elseif := range stmt.ElsIfClauses {
		sb.WriteString(" ELSEIF ")
		sb.WriteString(exprToString(elseif.Condition))
		sb.WriteString(" THEN ")
		for i, elseifStmt := range elseif.Body {
			if i > 0 {
				sb.WriteString(" ")
			}
			sb.WriteString(procedureStatementToSQL(elseifStmt))
			sb.WriteString(";")
		}
	}
	if len(stmt.ElseBranch) > 0 {
		sb.WriteString(" ELSE ")
		for i, elseStmt := range stmt.ElseBranch {
			if i > 0 {
				sb.WriteString(" ")
			}
			sb.WriteString(procedureStatementToSQL(elseStmt))
			sb.WriteString(";")
		}
	}
	sb.WriteString(" END IF")
	return sb.String()
}
