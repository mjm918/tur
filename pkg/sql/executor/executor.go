// pkg/sql/executor/executor.go
package executor

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"tur/pkg/btree"
	"tur/pkg/cache"
	"tur/pkg/hnsw"
	"tur/pkg/mvcc"
	"tur/pkg/pager"
	"tur/pkg/record"
	"tur/pkg/schema"
	"tur/pkg/sql/lexer"
	"tur/pkg/sql/optimizer"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
	"tur/pkg/vdbe"
)

// Result holds the result of executing a SQL statement
type Result struct {
	Columns      []string
	Rows         [][]types.Value
	RowsAffected int64
}

// Executor executes SQL statements
type Executor struct {
	pager       *pager.Pager
	catalog     *schema.Catalog
	trees       map[string]*btree.BTree    // table name -> btree
	rowid       map[string]uint64          // table name -> next rowid
	maxRowid    map[string]int64           // table name -> max INTEGER PRIMARY KEY value (for AUTOINCREMENT)
	txManager   *mvcc.TransactionManager
	currentTx   *mvcc.Transaction          // current active transaction (nil if none)
	hnswIndexes map[string]*hnsw.Index     // HNSW index name -> index
	queryCache  *cache.QueryCache          // optional query result cache
}

// New creates a new Executor
func New(p *pager.Pager) *Executor {
	return &Executor{
		pager:     p,
		catalog:   schema.NewCatalog(),
		trees:     make(map[string]*btree.BTree),
		rowid:     make(map[string]uint64),
		maxRowid:  make(map[string]int64),
		txManager: mvcc.NewTransactionManager(),
	}
}

// Close closes the executor and syncs data
func (e *Executor) Close() error {
	return e.pager.Close()
}

// GetCatalog returns the schema catalog for inspecting statistics
func (e *Executor) GetCatalog() *schema.Catalog {
	return e.catalog
}

// SetTransaction sets the current transaction context for execution.
// This allows external code to control the transaction used by Execute.
// Pass nil to clear the transaction (auto-commit mode).
func (e *Executor) SetTransaction(tx *mvcc.Transaction) {
	e.currentTx = tx
}

// GetTransaction returns the current transaction context.
func (e *Executor) GetTransaction() *mvcc.Transaction {
	return e.currentTx
}

// SetQueryCache sets the query result cache for the executor.
// Pass nil to disable query caching.
func (e *Executor) SetQueryCache(qc *cache.QueryCache) {
	e.queryCache = qc
}

// GetQueryCache returns the query result cache, if any.
func (e *Executor) GetQueryCache() *cache.QueryCache {
	return e.queryCache
}

// InvalidateQueryCache invalidates cache entries for the specified table.
// Called automatically on INSERT, UPDATE, DELETE operations.
func (e *Executor) InvalidateQueryCache(tableName string) {
	if e.queryCache != nil {
		e.queryCache.InvalidateTable(tableName)
	}
}

// Execute parses and executes a SQL statement
func (e *Executor) Execute(sql string) (*Result, error) {
	p := parser.New(sql)
	stmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		return e.executeCreateTable(s)
	case *parser.DropTableStmt:
		return e.executeDropTable(s)
	case *parser.CreateIndexStmt:
		return e.executeCreateIndex(s)
	case *parser.DropIndexStmt:
		return e.executeDropIndex(s)
	case *parser.InsertStmt:
		return e.executeInsert(s)
	case *parser.SelectStmt:
		return e.executeSelect(s)
	case *parser.UpdateStmt:
		return e.executeUpdate(s)
	case *parser.DeleteStmt:
		return e.executeDelete(s)
	case *parser.AnalyzeStmt:
		return e.executeAnalyze(s)
	case *parser.AlterTableStmt:
		return e.executeAlterTable(s)
	case *parser.BeginStmt:
		return e.executeBegin(s)
	case *parser.CommitStmt:
		return e.executeCommit(s)
	case *parser.RollbackStmt:
		return e.executeRollback(s)
	case *parser.SavepointStmt:
		return e.executeSavepoint(s)
	case *parser.RollbackToStmt:
		return e.executeRollbackTo(s)
	case *parser.ReleaseStmt:
		return e.executeRelease(s)
	case *parser.SetOperation:
		return e.executeSetOperation(s)
	case *parser.CreateViewStmt:
		return e.executeCreateView(s)
	case *parser.DropViewStmt:
		return e.executeDropView(s)
	case *parser.ExplainStmt:
		return e.executeExplain(s)
	case *parser.CreateTriggerStmt:
		return e.executeCreateTrigger(s)
	case *parser.DropTriggerStmt:
		return e.executeDropTrigger(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// executeCreateTable handles CREATE TABLE
func (e *Executor) executeCreateTable(stmt *parser.CreateTableStmt) (*Result, error) {
	// Check if table already exists
	if e.catalog.GetTable(stmt.TableName) != nil {
		return nil, fmt.Errorf("table %s already exists", stmt.TableName)
	}

	// Create B-tree for the table
	tree, err := btree.Create(e.pager)
	if err != nil {
		return nil, fmt.Errorf("failed to create btree: %w", err)
	}

	// Convert column definitions with constraints
	columns := make([]schema.ColumnDef, len(stmt.Columns))
	for i, col := range stmt.Columns {
		columns[i] = schema.ColumnDef{
			Name:        col.Name,
			Type:        col.Type,
			PrimaryKey:  col.PrimaryKey,
			NotNull:     col.NotNull,
			VectorDim:   col.VectorDim,
			NoNormalize: col.NoNormalize,
		}

		// Build column constraints
		var constraints []schema.Constraint

		// PRIMARY KEY
		if col.PrimaryKey {
			constraints = append(constraints, schema.Constraint{
				Type: schema.ConstraintPrimaryKey,
			})
		}

		// NOT NULL
		if col.NotNull {
			constraints = append(constraints, schema.Constraint{
				Type: schema.ConstraintNotNull,
			})
		}

		// UNIQUE
		if col.Unique {
			constraints = append(constraints, schema.Constraint{
				Type: schema.ConstraintUnique,
			})
		}

		// CHECK
		if col.CheckExpr != nil {
			constraints = append(constraints, schema.Constraint{
				Type:            schema.ConstraintCheck,
				CheckExpression: exprToString(col.CheckExpr),
			})
		}

		// FOREIGN KEY (column-level REFERENCES)
		if col.ForeignKey != nil {
			// Validate that the referenced table and column exist
			if err := e.validateForeignKeyReference(col.ForeignKey.RefTable, col.ForeignKey.RefColumn); err != nil {
				return nil, fmt.Errorf("FOREIGN KEY constraint on column '%s': %w", col.Name, err)
			}
			constraints = append(constraints, schema.Constraint{
				Type:      schema.ConstraintForeignKey,
				RefTable:  col.ForeignKey.RefTable,
				RefColumn: col.ForeignKey.RefColumn,
				OnDelete:  convertFKAction(col.ForeignKey.OnDelete),
				OnUpdate:  convertFKAction(col.ForeignKey.OnUpdate),
			})
		}

		// DEFAULT
		if col.DefaultExpr != nil {
			// Evaluate the default expression at table creation time
			// to store the resolved value
			defaultVal, err := e.evaluateExpr(col.DefaultExpr, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate DEFAULT expression for column %s: %w", col.Name, err)
			}
			constraints = append(constraints, schema.Constraint{
				Type:         schema.ConstraintDefault,
				DefaultValue: &defaultVal,
			})
		}

		columns[i].Constraints = constraints
	}

	// Convert table-level constraints
	var tableConstraints []schema.TableConstraint
	for _, tc := range stmt.TableConstraints {
		schemaTC := schema.TableConstraint{
			Name:    tc.Name,
			Columns: tc.Columns,
		}

		switch tc.Type {
		case parser.TableConstraintPrimaryKey:
			schemaTC.Type = schema.ConstraintPrimaryKey
		case parser.TableConstraintUnique:
			schemaTC.Type = schema.ConstraintUnique
		case parser.TableConstraintForeignKey:
			// Validate that the referenced table and columns exist
			for i, refCol := range tc.RefColumns {
				if err := e.validateForeignKeyReference(tc.RefTable, refCol); err != nil {
					colName := ""
					if i < len(tc.Columns) {
						colName = tc.Columns[i]
					}
					return nil, fmt.Errorf("FOREIGN KEY constraint on column '%s': %w", colName, err)
				}
			}
			schemaTC.Type = schema.ConstraintForeignKey
			schemaTC.RefTable = tc.RefTable
			schemaTC.RefColumns = tc.RefColumns
			schemaTC.OnDelete = convertFKAction(tc.OnDelete)
			schemaTC.OnUpdate = convertFKAction(tc.OnUpdate)
		case parser.TableConstraintCheck:
			schemaTC.Type = schema.ConstraintCheck
			schemaTC.CheckExpression = exprToString(tc.CheckExpr)
		}

		tableConstraints = append(tableConstraints, schemaTC)
	}

	// Create table definition
	table := &schema.TableDef{
		Name:             stmt.TableName,
		Columns:          columns,
		RootPage:         tree.RootPage(),
		TableConstraints: tableConstraints,
	}

	// Add to catalog
	if err := e.catalog.CreateTable(table); err != nil {
		return nil, err
	}

	// Store tree reference
	e.trees[stmt.TableName] = tree
	e.rowid[stmt.TableName] = 1

	// Auto-create unique index for PRIMARY KEY columns
	if err := e.createPrimaryKeyIndex(table); err != nil {
		return nil, fmt.Errorf("failed to create primary key index: %w", err)
	}

	// Auto-create unique indexes for UNIQUE constraints
	if err := e.createUniqueConstraintIndexes(table); err != nil {
		return nil, fmt.Errorf("failed to create unique constraint indexes: %w", err)
	}

	return &Result{}, nil
}

// executeCreateView handles CREATE VIEW
func (e *Executor) executeCreateView(stmt *parser.CreateViewStmt) (*Result, error) {
	// Check if view already exists
	if e.catalog.GetView(stmt.ViewName) != nil {
		if stmt.IfNotExists {
			return &Result{}, nil
		}
		return nil, fmt.Errorf("view %s already exists", stmt.ViewName)
	}

	// Convert the SELECT statement to SQL text
	// We store the SQL definition so views can be expanded when queried
	sql := selectStmtToSQL(stmt.Query)

	view := &schema.ViewDef{
		Name:    stmt.ViewName,
		SQL:     sql,
		Columns: stmt.Columns,
	}

	if err := e.catalog.CreateView(view); err != nil {
		return nil, err
	}

	return &Result{}, nil
}

// executeDropView handles DROP VIEW
func (e *Executor) executeDropView(stmt *parser.DropViewStmt) (*Result, error) {
	// Check if view exists
	if e.catalog.GetView(stmt.ViewName) == nil {
		if stmt.IfExists {
			return &Result{}, nil
		}
		return nil, fmt.Errorf("view %s not found", stmt.ViewName)
	}

	if err := e.catalog.DropView(stmt.ViewName); err != nil {
		return nil, err
	}

	return &Result{}, nil
}

// executeCreateTrigger handles CREATE TRIGGER
func (e *Executor) executeCreateTrigger(stmt *parser.CreateTriggerStmt) (*Result, error) {
	// Check if trigger already exists
	if e.catalog.GetTrigger(stmt.TriggerName) != nil {
		return nil, fmt.Errorf("trigger %s already exists", stmt.TriggerName)
	}

	// Check if target table exists
	if e.catalog.GetTable(stmt.TableName) == nil {
		return nil, fmt.Errorf("table %s not found", stmt.TableName)
	}

	// Convert parser types to schema types
	var timing schema.TriggerTiming
	switch stmt.Timing {
	case parser.TriggerBefore:
		timing = schema.TriggerBefore
	case parser.TriggerAfter:
		timing = schema.TriggerAfter
	}

	var event schema.TriggerEvent
	switch stmt.Event {
	case parser.TriggerEventInsert:
		event = schema.TriggerInsert
	case parser.TriggerEventUpdate:
		event = schema.TriggerUpdate
	case parser.TriggerEventDelete:
		event = schema.TriggerDelete
	}

	// Store parsed action statements
	actions := make([]interface{}, len(stmt.Actions))
	for i, action := range stmt.Actions {
		actions[i] = action
	}

	trigger := &schema.TriggerDef{
		Name:      stmt.TriggerName,
		TableName: stmt.TableName,
		Timing:    timing,
		Event:     event,
		SQL:       "", // TODO: Store original SQL for persistence
		Actions:   actions,
	}

	if err := e.catalog.CreateTrigger(trigger); err != nil {
		return nil, err
	}

	return &Result{}, nil
}

// executeDropTrigger handles DROP TRIGGER
func (e *Executor) executeDropTrigger(stmt *parser.DropTriggerStmt) (*Result, error) {
	// Check if trigger exists
	if e.catalog.GetTrigger(stmt.TriggerName) == nil {
		if stmt.IfExists {
			return &Result{}, nil
		}
		return nil, fmt.Errorf("trigger %s not found", stmt.TriggerName)
	}

	if err := e.catalog.DropTrigger(stmt.TriggerName); err != nil {
		return nil, err
	}

	return &Result{}, nil
}

// TriggerContext holds the OLD and NEW row values for trigger execution
type TriggerContext struct {
	OldRow   []types.Value       // OLD row values (nil for INSERT)
	NewRow   []types.Value       // NEW row values (nil for DELETE)
	ColMap   map[string]int      // Column name to index mapping
	Table    *schema.TableDef    // Table definition
}

// fireTriggers executes all triggers for a table/timing/event combination
// Returns ErrTriggerAbort if RAISE(ABORT) is called, ErrTriggerIgnore for RAISE(IGNORE)
func (e *Executor) fireTriggers(tableName string, timing schema.TriggerTiming, event schema.TriggerEvent, ctx *TriggerContext) error {
	triggers := e.catalog.GetTriggersForTable(tableName, timing, event)
	if len(triggers) == 0 {
		return nil
	}

	for _, trigger := range triggers {
		if err := e.executeTriggerActions(trigger, ctx); err != nil {
			return err
		}
	}

	return nil
}

// executeTriggerActions executes the action statements of a trigger
func (e *Executor) executeTriggerActions(trigger *schema.TriggerDef, ctx *TriggerContext) error {
	for _, action := range trigger.Actions {
		stmt, ok := action.(parser.Statement)
		if !ok {
			continue
		}

		// Execute the trigger action statement
		if err := e.executeTriggerStatement(stmt, ctx); err != nil {
			return err
		}
	}
	return nil
}

// executeTriggerStatement executes a single trigger action statement with NEW/OLD context
func (e *Executor) executeTriggerStatement(stmt parser.Statement, ctx *TriggerContext) error {
	// For now, execute statements directly without NEW/OLD resolution
	// TODO: Add NEW/OLD column reference resolution in a later iteration
	switch s := stmt.(type) {
	case *parser.InsertStmt:
		_, err := e.executeInsertWithTriggerContext(s, ctx)
		return err
	case *parser.UpdateStmt:
		_, err := e.executeUpdate(s)
		return err
	case *parser.DeleteStmt:
		_, err := e.executeDelete(s)
		return err
	case *parser.SelectStmt:
		// SELECT in triggers is typically used for RAISE validation
		_, err := e.executeSelect(s)
		return err
	default:
		return fmt.Errorf("unsupported statement type in trigger: %T", stmt)
	}
}

// executeInsertWithTriggerContext executes INSERT with trigger context (for nested triggers)
func (e *Executor) executeInsertWithTriggerContext(stmt *parser.InsertStmt, ctx *TriggerContext) (*Result, error) {
	// For now, just call regular executeInsert but without firing triggers (to prevent infinite recursion)
	// A proper implementation would track trigger depth and allow limited nesting
	return e.executeInsertNoTriggers(stmt)
}

// selectStmtToSQL converts a SelectStmt back to SQL text
// This is a simplified implementation that stores the structure
func selectStmtToSQL(stmt *parser.SelectStmt) string {
	if stmt == nil {
		return ""
	}

	// Build a basic SQL representation
	var sql string
	sql = "SELECT "

	// Columns
	for i, col := range stmt.Columns {
		if i > 0 {
			sql += ", "
		}
		if col.Star {
			sql += "*"
		} else if col.Expr != nil {
			sql += exprToString(col.Expr)
			if col.Alias != "" {
				sql += " AS " + col.Alias
			}
		}
	}

	// FROM
	if stmt.From != nil {
		sql += " FROM " + tableRefToSQL(stmt.From)
	}

	// WHERE
	if stmt.Where != nil {
		sql += " WHERE " + exprToString(stmt.Where)
	}

	// GROUP BY
	if len(stmt.GroupBy) > 0 {
		sql += " GROUP BY "
		for i, expr := range stmt.GroupBy {
			if i > 0 {
				sql += ", "
			}
			sql += exprToString(expr)
		}
	}

	// HAVING
	if stmt.Having != nil {
		sql += " HAVING " + exprToString(stmt.Having)
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		sql += " ORDER BY "
		for i, ob := range stmt.OrderBy {
			if i > 0 {
				sql += ", "
			}
			sql += exprToString(ob.Expr)
			if ob.Direction == parser.OrderDesc {
				sql += " DESC"
			}
		}
	}

	// LIMIT
	if stmt.Limit != nil {
		sql += " LIMIT " + exprToString(stmt.Limit)
	}

	// OFFSET
	if stmt.Offset != nil {
		sql += " OFFSET " + exprToString(stmt.Offset)
	}

	return sql
}

// tableRefToSQL converts a TableReference to SQL text
func tableRefToSQL(ref parser.TableReference) string {
	switch t := ref.(type) {
	case *parser.Table:
		if t.Alias != "" {
			return t.Name + " AS " + t.Alias
		}
		return t.Name
	case *parser.Join:
		left := tableRefToSQL(t.Left)
		right := tableRefToSQL(t.Right)
		joinType := "JOIN"
		switch t.Type {
		case parser.JoinLeft:
			joinType = "LEFT JOIN"
		case parser.JoinRight:
			joinType = "RIGHT JOIN"
		case parser.JoinFull:
			joinType = "FULL JOIN"
		}
		return left + " " + joinType + " " + right + " ON " + exprToString(t.Condition)
	default:
		return ""
	}
}

// convertFKAction converts parser FK action to schema FK action
func convertFKAction(action parser.FKAction) schema.ForeignKeyAction {
	switch action {
	case parser.FKActionNoAction:
		return schema.FKActionNoAction
	case parser.FKActionRestrict:
		return schema.FKActionRestrict
	case parser.FKActionCascade:
		return schema.FKActionCascade
	case parser.FKActionSetNull:
		return schema.FKActionSetNull
	case parser.FKActionSetDefault:
		return schema.FKActionSetDefault
	default:
		return schema.FKActionNoAction
	}
}

// validateForeignKeyReference validates that a foreign key reference points to an existing table and column
func (e *Executor) validateForeignKeyReference(refTable, refColumn string) error {
	// Check if referenced table exists
	table := e.catalog.GetTable(refTable)
	if table == nil {
		return fmt.Errorf("referenced table '%s' does not exist", refTable)
	}

	// Check if referenced column exists
	col, _ := table.GetColumn(refColumn)
	if col == nil {
		return fmt.Errorf("referenced column '%s' does not exist in table '%s'", refColumn, refTable)
	}

	return nil
}

// validateForeignKeyValue checks that a value exists in the referenced table/column
func (e *Executor) validateForeignKeyValue(refTable, refColumn string, value types.Value) error {
	// Get the referenced table
	table := e.catalog.GetTable(refTable)
	if table == nil {
		return fmt.Errorf("referenced table '%s' does not exist", refTable)
	}

	// Get the column index
	_, colIdx := table.GetColumn(refColumn)
	if colIdx < 0 {
		return fmt.Errorf("referenced column '%s' does not exist in table '%s'", refColumn, refTable)
	}

	// Get or open the B-tree for the referenced table
	tree := e.trees[refTable]
	if tree == nil {
		if table.RootPage == 0 {
			// Table has no root page yet (empty), so value doesn't exist
			return fmt.Errorf("no matching value found in '%s.%s'", refTable, refColumn)
		}
		tree = btree.Open(e.pager, table.RootPage)
		e.trees[refTable] = tree
	}

	// Scan the referenced table for the value
	cursor := tree.Cursor()
	defer cursor.Close()

	for cursor.First(); cursor.Valid(); cursor.Next() {
		rowData := cursor.Value()
		rowValues := record.Decode(rowData)

		if colIdx >= len(rowValues) {
			continue
		}

		refValue := rowValues[colIdx]

		// Check if the values match
		if valuesEqual(value, refValue) {
			return nil // Found a match
		}
	}

	return fmt.Errorf("no matching value found in '%s.%s'", refTable, refColumn)
}

// exprToString converts an expression to a string representation
func exprToString(expr parser.Expression) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *parser.Literal:
		if e.Value.IsNull() {
			return "NULL"
		}
		switch e.Value.Type() {
		case types.TypeInt:
			return fmt.Sprintf("%d", e.Value.Int())
		case types.TypeFloat:
			return fmt.Sprintf("%g", e.Value.Float())
		case types.TypeText:
			return fmt.Sprintf("'%s'", e.Value.Text())
		default:
			return "?"
		}
	case *parser.ColumnRef:
		return e.Name
	case *parser.BinaryExpr:
		left := exprToString(e.Left)
		right := exprToString(e.Right)
		op := tokenToOp(e.Op)
		return fmt.Sprintf("(%s %s %s)", left, op, right)
	case *parser.UnaryExpr:
		right := exprToString(e.Right)
		op := tokenToOp(e.Op)
		return fmt.Sprintf("%s%s", op, right)
	case *parser.FunctionCall:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = exprToString(arg)
		}
		return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))
	default:
		return ""
	}
}

// tokenToOp converts a lexer token type to operator string
func tokenToOp(t lexer.TokenType) string {
	switch t {
	case lexer.PLUS:
		return "+"
	case lexer.MINUS:
		return "-"
	case lexer.STAR:
		return "*"
	case lexer.SLASH:
		return "/"
	case lexer.EQ:
		return "="
	case lexer.NEQ:
		return "!="
	case lexer.LT:
		return "<"
	case lexer.GT:
		return ">"
	case lexer.LTE:
		return "<="
	case lexer.GTE:
		return ">="
	case lexer.AND:
		return "AND"
	case lexer.OR:
		return "OR"
	default:
		return "?"
	}
}

// executeDropTable handles DROP TABLE [IF EXISTS] table_name [CASCADE]
func (e *Executor) executeDropTable(stmt *parser.DropTableStmt) (*Result, error) {
	// Check if table exists
	tableDef := e.catalog.GetTable(stmt.TableName)
	if tableDef == nil {
		// If table doesn't exist and IF EXISTS is specified, silently succeed
		if stmt.IfExists {
			return &Result{}, nil
		}
		return nil, fmt.Errorf("table not found")
	}

	// TODO: Check for dependent views and triggers (if CASCADE is not specified)

	// TODO: If CASCADE is specified, drop associated indexes and handle foreign keys

	// Drop the table from catalog
	if err := e.catalog.DropTable(stmt.TableName); err != nil {
		return nil, err
	}

	// Clean up in-memory structures
	delete(e.trees, stmt.TableName)
	delete(e.rowid, stmt.TableName)

	// TODO: Add table's B-tree pages to free list

	return &Result{}, nil
}

// executeCreateIndex handles CREATE INDEX
func (e *Executor) executeCreateIndex(stmt *parser.CreateIndexStmt) (*Result, error) {
	// Check if table exists
	table := e.catalog.GetTable(stmt.TableName)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", stmt.TableName)
	}

	// Validate all columns exist in the table and build column index map
	colIndexes := make([]int, len(stmt.Columns))
	for i, colName := range stmt.Columns {
		_, idx := table.GetColumn(colName)
		if idx < 0 {
			return nil, fmt.Errorf("column %s not found in table %s", colName, stmt.TableName)
		}
		colIndexes[i] = idx
	}

	// Convert expressions to SQL strings for storage
	var expressionStrings []string
	for _, expr := range stmt.Expressions {
		exprSQL := exprToString(expr)
		expressionStrings = append(expressionStrings, exprSQL)
	}

	// Create B-tree for the index
	indexTree, err := btree.Create(e.pager)
	if err != nil {
		return nil, fmt.Errorf("failed to create index btree: %w", err)
	}

	// Store the index tree in our map
	idxTreeName := "index:" + stmt.IndexName
	e.trees[idxTreeName] = indexTree

	// Get the table's B-tree to scan existing data
	tableTree := e.trees[stmt.TableName]
	if tableTree == nil && table.RootPage != 0 {
		tableTree = btree.Open(e.pager, table.RootPage)
		e.trees[stmt.TableName] = tableTree
	}

	// Prepare predicate evaluation for partial indexes
	var whereExpr parser.Expression
	if stmt.Where != nil {
		whereExpr = stmt.Where
	}

	// Build column index map for predicate evaluation
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[col.Name] = i
	}

	// Populate index from existing table data
	// Note: Expression indexes are evaluated during insert/update via updateIndexes()
	if tableTree != nil {
		cursor := tableTree.Cursor()
		defer cursor.Close()

		for cursor.First(); cursor.Valid(); cursor.Next() {
			key := cursor.Key()
			value := cursor.Value()

			// Extract rowid from key (8 bytes, big-endian)
			if len(key) < 8 {
				continue
			}
			rowID := binary.BigEndian.Uint64(key)

			// Decode row values
			values := record.Decode(value)

			// For partial indexes, check if row matches predicate
			if whereExpr != nil {
				matches, err := e.evaluateCondition(whereExpr, values, colMap)
				if err != nil {
					return nil, fmt.Errorf("failed to evaluate partial index predicate: %w", err)
				}
				if !matches {
					continue // Skip rows that don't match the predicate
				}
			}

			// Build index key from the indexed column values
			var keyValues []types.Value
			for _, colIdx := range colIndexes {
				if colIdx < len(values) {
					keyValues = append(keyValues, values[colIdx])
				} else {
					keyValues = append(keyValues, types.NewNull())
				}
			}

			// Encode key and value based on uniqueness
			var indexKey []byte
			var indexValue []byte

			if stmt.Unique {
				// Unique index: Key = Columns, Value = RowID
				indexKey = record.Encode(keyValues)

				// Check for uniqueness violation
				existingVal, err := indexTree.Get(indexKey)
				if err == nil && existingVal != nil {
					return nil, fmt.Errorf("UNIQUE constraint failed: duplicate key in index %s", stmt.IndexName)
				}

				// Encode RowID as value
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, rowID)
				indexValue = buf
			} else {
				// Non-unique index: Key = Columns + RowID, Value = empty
				keyValues = append(keyValues, types.NewInt(int64(rowID)))
				indexKey = record.Encode(keyValues)
				indexValue = []byte{}
			}

			// Insert into index
			if err := indexTree.Insert(indexKey, indexValue); err != nil {
				return nil, fmt.Errorf("failed to build index %s: %w", stmt.IndexName, err)
			}
		}
	}

	// Create index definition
	idx := &schema.IndexDef{
		Name:        stmt.IndexName,
		TableName:   stmt.TableName,
		Columns:     stmt.Columns,
		Expressions: expressionStrings,
		Type:        schema.IndexTypeBTree,
		Unique:      stmt.Unique,
		RootPage:    indexTree.RootPage(),
	}

	// Store WHERE clause for partial indexes
	if stmt.Where != nil {
		idx.WhereClause = exprToString(stmt.Where)
	}

	// Add to catalog
	if err := e.catalog.CreateIndex(idx); err != nil {
		return nil, err
	}

	return &Result{}, nil
}

// executeDropIndex handles DROP INDEX [IF EXISTS] index_name
func (e *Executor) executeDropIndex(stmt *parser.DropIndexStmt) (*Result, error) {
	// Check if index exists
	indexDef := e.catalog.GetIndex(stmt.IndexName)
	if indexDef == nil {
		// If index doesn't exist and IF EXISTS is specified, silently succeed
		if stmt.IfExists {
			return &Result{}, nil
		}
		return nil, fmt.Errorf("index not found")
	}

	// Drop the index from catalog
	if err := e.catalog.DropIndex(stmt.IndexName); err != nil {
		return nil, err
	}

	// Clean up in-memory B-tree structure
	idxTreeName := "index:" + stmt.IndexName
	delete(e.trees, idxTreeName)

	return &Result{}, nil
}

// executeInsert handles INSERT with trigger support
func (e *Executor) executeInsert(stmt *parser.InsertStmt) (*Result, error) {
	return e.executeInsertInternal(stmt, true)
}

// executeInsertNoTriggers handles INSERT without firing triggers (used by triggers themselves)
func (e *Executor) executeInsertNoTriggers(stmt *parser.InsertStmt) (*Result, error) {
	return e.executeInsertInternal(stmt, false)
}

// executeInsertInternal is the internal INSERT implementation with optional trigger firing
func (e *Executor) executeInsertInternal(stmt *parser.InsertStmt, fireTriggers bool) (*Result, error) {
	// Get table
	table := e.catalog.GetTable(stmt.TableName)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", stmt.TableName)
	}

	// Get or create B-tree
	tree := e.trees[stmt.TableName]
	if tree == nil {
		tree = btree.Open(e.pager, table.RootPage)
		e.trees[stmt.TableName] = tree
	}

	// Build column map for trigger context
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[col.Name] = i
	}

	// Find INTEGER PRIMARY KEY column for AUTOINCREMENT
	intPKColIdx := e.findIntegerPrimaryKeyColumn(table)

	var rowsAffected int64

	// Get rows to insert - either from VALUES or SELECT
	var rowsToInsert [][]types.Value

	if stmt.SelectStmt != nil {
		// INSERT SELECT: Execute the SELECT statement
		selectResult, err := e.executeSelect(stmt.SelectStmt)
		if err != nil {
			return nil, fmt.Errorf("failed to execute SELECT in INSERT: %w", err)
		}
		rowsToInsert = selectResult.Rows
	} else {
		// INSERT VALUES: Evaluate expression rows
		rowsToInsert = make([][]types.Value, len(stmt.Values))
		for i, row := range stmt.Values {
			values := make([]types.Value, len(row))
			for j, expr := range row {
				val, err := e.evaluateExpr(expr, nil, nil)
				if err != nil {
					return nil, err
				}
				values[j] = val
			}
			rowsToInsert[i] = values
		}
	}

	// Build column mapping if column list is specified
	colMapping := e.buildColumnMapping(stmt.Columns, table)

	// Insert each row
	for _, inputValues := range rowsToInsert {
		// Map input values to full column list
		values := e.mapInputToColumns(inputValues, colMapping, table)

		// Handle INTEGER PRIMARY KEY auto-increment
		if intPKColIdx >= 0 {
			if values[intPKColIdx].IsNull() {
				// Auto-generate ID for NULL INTEGER PRIMARY KEY
				nextID := e.getNextAutoIncrementID(stmt.TableName)
				values[intPKColIdx] = types.NewInt(nextID)
			} else {
				// Track the explicit ID to update max rowid
				if values[intPKColIdx].Type() == types.TypeInt {
					e.trackMaxRowID(stmt.TableName, values[intPKColIdx].Int())
				}
			}
		}

		// Fire BEFORE INSERT triggers
		if fireTriggers {
			ctx := &TriggerContext{
				OldRow: nil, // INSERT has no OLD row
				NewRow: values,
				ColMap: colMap,
				Table:  table,
			}
			if err := e.fireTriggers(stmt.TableName, schema.TriggerBefore, schema.TriggerInsert, ctx); err != nil {
				if err == schema.ErrTriggerIgnore {
					// RAISE(IGNORE) - skip this row silently
					continue
				}
				return nil, err
			}
		}

		// Validate constraints
		if err := e.validateConstraints(table, values); err != nil {
			return nil, err
		}

		// Validate types and Normalize Vectors
		for idx, val := range values {
			colDef := table.Columns[idx]
			if colDef.Type == types.TypeVector && !val.IsNull() {
				if val.Type() != types.TypeBlob {
					return nil, fmt.Errorf("column %s expects VECTOR (blob), got %v", colDef.Name, val.Type())
				}

				// Parse vector to validate dimension and normalize
				blob := val.Blob()
				vec, err := types.VectorFromBytes(blob)
				if err != nil {
					return nil, fmt.Errorf("invalid vector data for column %s: %w", colDef.Name, err)
				}

				if vec.Dimension() != colDef.VectorDim {
					return nil, fmt.Errorf("column %s expects VECTOR(%d), got dimension %d", colDef.Name, colDef.VectorDim, vec.Dimension())
				}

				// Normalize and update value (unless NONORMALIZE is set)
				if !colDef.NoNormalize {
					vec.Normalize()
					values[idx] = types.NewBlob(vec.ToBytes())
				}
			}
		}

		// Encode row as record
		data := record.Encode(values)

		// Generate rowid key
		rowid := e.rowid[stmt.TableName]
		e.rowid[stmt.TableName]++

		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, rowid)

		// Insert into B-tree
		if err := tree.Insert(key, data); err != nil {
			return nil, fmt.Errorf("failed to insert: %w", err)
		}

		// Update indexes
		if err := e.updateIndexes(table, rowid, values); err != nil {
			return nil, err
		}

		// Fire AFTER INSERT triggers
		if fireTriggers {
			ctx := &TriggerContext{
				OldRow: nil,
				NewRow: values,
				ColMap: colMap,
				Table:  table,
			}
			if err := e.fireTriggers(stmt.TableName, schema.TriggerAfter, schema.TriggerInsert, ctx); err != nil {
				return nil, err
			}
		}

		rowsAffected++
	}

	// Update statistics incrementally if they exist
	if rowsAffected > 0 {
		e.incrementTableRowCount(stmt.TableName, rowsAffected)
		// Invalidate query cache for this table
		e.InvalidateQueryCache(stmt.TableName)
	}

	return &Result{RowsAffected: rowsAffected}, nil
}

// executeUpdate handles UPDATE statements
func (e *Executor) executeUpdate(stmt *parser.UpdateStmt) (*Result, error) {
	// Get table
	table := e.catalog.GetTable(stmt.TableName)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", stmt.TableName)
	}

	// Get or create B-tree
	tree := e.trees[stmt.TableName]
	if tree == nil {
		tree = btree.Open(e.pager, table.RootPage)
		e.trees[stmt.TableName] = tree
	}

	// Build column map for expression evaluation
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[col.Name] = i
	}

	// Validate assignment columns exist
	for _, assign := range stmt.Assignments {
		if _, ok := colMap[assign.Column]; !ok {
			return nil, fmt.Errorf("column %s not found in table %s", assign.Column, stmt.TableName)
		}
	}

	// Collect rows to update: iterate through all rows, evaluate WHERE clause
	type updateEntry struct {
		key       []byte
		oldValues []types.Value
	}
	var toUpdate []updateEntry

	cursor := tree.Cursor()
	defer cursor.Close()

	for cursor.First(); cursor.Valid(); cursor.Next() {
		key := cursor.Key()
		value := cursor.Value()

		// Decode row
		values := record.Decode(value)

		// Evaluate WHERE clause if present
		if stmt.Where != nil {
			match, err := e.evaluateCondition(stmt.Where, values, colMap)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		// Copy key and values for update
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)

		toUpdate = append(toUpdate, updateEntry{
			key:       keyCopy,
			oldValues: values,
		})
	}

	// Apply updates
	var rowsAffected int64
	for _, entry := range toUpdate {
		// Create new row values based on old values and assignments
		newValues := make([]types.Value, len(entry.oldValues))
		copy(newValues, entry.oldValues)

		// Apply assignments
		for _, assign := range stmt.Assignments {
			colIdx := colMap[assign.Column]

			// Evaluate expression with current row values (for expressions like value = value + 1)
			newVal, err := e.evaluateExpr(assign.Value, entry.oldValues, colMap)
			if err != nil {
				return nil, err
			}
			newValues[colIdx] = newVal
		}

		// Fire BEFORE UPDATE triggers
		ctx := &TriggerContext{
			OldRow: entry.oldValues,
			NewRow: newValues,
			ColMap: colMap,
			Table:  table,
		}
		if err := e.fireTriggers(stmt.TableName, schema.TriggerBefore, schema.TriggerUpdate, ctx); err != nil {
			return nil, err
		}

		// Validate constraints on new row
		if err := e.validateConstraints(table, newValues); err != nil {
			return nil, err
		}

		// Handle vector normalization
		for idx, val := range newValues {
			colDef := table.Columns[idx]
			if colDef.Type == types.TypeVector && !val.IsNull() {
				if val.Type() != types.TypeBlob {
					return nil, fmt.Errorf("column %s expects VECTOR (blob), got %v", colDef.Name, val.Type())
				}

				blob := val.Blob()
				vec, err := types.VectorFromBytes(blob)
				if err != nil {
					return nil, fmt.Errorf("invalid vector data for column %s: %w", colDef.Name, err)
				}

				if vec.Dimension() != colDef.VectorDim {
					return nil, fmt.Errorf("column %s expects VECTOR(%d), got dimension %d", colDef.Name, colDef.VectorDim, vec.Dimension())
				}

				vec.Normalize()
				newValues[idx] = types.NewBlob(vec.ToBytes())
			}
		}

		// Encode new row
		data := record.Encode(newValues)

		// Get rowid for index updates
		rowid := binary.BigEndian.Uint64(entry.key)

		// Delete old index entries before adding new ones
		// This is important for UNIQUE indexes where the value might not change
		if err := e.deleteFromIndexes(table, rowid, entry.oldValues); err != nil {
			return nil, err
		}

		// Update in B-tree (Insert handles both insert and update)
		if err := tree.Insert(entry.key, data); err != nil {
			return nil, fmt.Errorf("failed to update row: %w", err)
		}

		// Add new index entries
		if err := e.updateIndexes(table, rowid, newValues); err != nil {
			return nil, err
		}

		// Check and propagate foreign key updates (CASCADE/SET NULL ON UPDATE)
		if err := e.checkForeignKeyOnUpdate(table, entry.oldValues, newValues, colMap); err != nil {
			return nil, err
		}

		// Fire AFTER UPDATE triggers
		if err := e.fireTriggers(stmt.TableName, schema.TriggerAfter, schema.TriggerUpdate, ctx); err != nil {
			return nil, err
		}

		rowsAffected++
	}

	// Invalidate query cache for this table if any rows were updated
	if rowsAffected > 0 {
		e.InvalidateQueryCache(stmt.TableName)
	}

	return &Result{RowsAffected: rowsAffected}, nil
}

// executeDelete handles DELETE statements
func (e *Executor) executeDelete(stmt *parser.DeleteStmt) (*Result, error) {
	// Get table
	table := e.catalog.GetTable(stmt.TableName)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", stmt.TableName)
	}

	// Get or create B-tree
	tree := e.trees[stmt.TableName]
	if tree == nil {
		tree = btree.Open(e.pager, table.RootPage)
		e.trees[stmt.TableName] = tree
	}

	// Build column map for expression evaluation
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[col.Name] = i
	}

	// Collect entries to delete: iterate through all rows, evaluate WHERE clause
	type deleteEntry struct {
		key    []byte
		values []types.Value
	}
	var entriesToDelete []deleteEntry

	cursor := tree.Cursor()
	defer cursor.Close()

	for cursor.First(); cursor.Valid(); cursor.Next() {
		key := cursor.Key()
		value := cursor.Value()

		// Decode row
		values := record.Decode(value)

		// Evaluate WHERE clause if present
		if stmt.Where != nil {
			match, err := e.evaluateCondition(stmt.Where, values, colMap)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		// Copy key for deletion (cursor key may be reused)
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		entriesToDelete = append(entriesToDelete, deleteEntry{key: keyCopy, values: values})
	}

	// Delete collected rows
	var rowsAffected int64
	for _, entry := range entriesToDelete {
		// Extract rowid from key for index deletion
		rowid := binary.BigEndian.Uint64(entry.key)

		// Fire BEFORE DELETE triggers
		ctx := &TriggerContext{
			OldRow: entry.values, // DELETE has OLD row
			NewRow: nil,          // DELETE has no NEW row
			ColMap: colMap,
			Table:  table,
		}
		if err := e.fireTriggers(stmt.TableName, schema.TriggerBefore, schema.TriggerDelete, ctx); err != nil {
			return nil, err
		}

		// Check foreign key constraints before deletion
		if err := e.checkForeignKeyOnDelete(table, entry.values, colMap); err != nil {
			return nil, err
		}

		// Delete from indexes first
		if err := e.deleteFromIndexes(table, rowid, entry.values); err != nil {
			return nil, fmt.Errorf("failed to delete from indexes: %w", err)
		}

		// Delete from main table
		if err := tree.Delete(entry.key); err != nil {
			return nil, fmt.Errorf("failed to delete row: %w", err)
		}

		// Fire AFTER DELETE triggers
		if err := e.fireTriggers(stmt.TableName, schema.TriggerAfter, schema.TriggerDelete, ctx); err != nil {
			return nil, err
		}

		rowsAffected++
	}

	// Update statistics incrementally if they exist (decrement for DELETE)
	if rowsAffected > 0 {
		e.incrementTableRowCount(stmt.TableName, -rowsAffected)
		// Invalidate query cache for this table
		e.InvalidateQueryCache(stmt.TableName)
	}

	return &Result{RowsAffected: rowsAffected}, nil
}

// checkForeignKeyOnDelete checks if deleting a row would violate foreign key constraints
// in other tables that reference this table. For CASCADE/SET NULL actions, it performs
// the appropriate modifications to the referencing rows.
func (e *Executor) checkForeignKeyOnDelete(table *schema.TableDef, values []types.Value, colMap map[string]int) error {
	// For each column that might be referenced, check for FK constraints
	for i, col := range table.Columns {
		// Get all FK references to this table/column
		refs := e.catalog.GetForeignKeyReferences(table.Name, col.Name)
		if len(refs) == 0 {
			continue
		}

		// Get the value being deleted
		if i >= len(values) {
			continue
		}
		deletedValue := values[i]

		// Check each referencing table for rows that reference this value
		for _, ref := range refs {
			// Get the referencing table
			refTable := e.catalog.GetTable(ref.ReferencingTable)
			if refTable == nil {
				continue
			}

			// Get or open the B-tree for the referencing table
			refTree := e.trees[ref.ReferencingTable]
			if refTree == nil {
				if refTable.RootPage == 0 {
					continue
				}
				refTree = btree.Open(e.pager, refTable.RootPage)
				e.trees[ref.ReferencingTable] = refTree
			}

			// Find the column index in the referencing table
			var refColIdx int = -1
			if ref.ReferencingColumn != "" {
				// Column-level FK
				_, refColIdx = refTable.GetColumn(ref.ReferencingColumn)
			} else if len(ref.ReferencingColumns) > 0 {
				// Table-level FK - find the corresponding column
				_, refColIdx = refTable.GetColumn(ref.ReferencingColumns[0])
			}
			if refColIdx < 0 {
				continue
			}

			// Collect matching rows first (to avoid modifying while iterating)
			type matchingRow struct {
				key    []byte
				values []types.Value
			}
			var matchingRows []matchingRow

			cursor := refTree.Cursor()
			for cursor.First(); cursor.Valid(); cursor.Next() {
				refRowData := cursor.Value()
				refRowValues := record.Decode(refRowData)

				if refColIdx >= len(refRowValues) {
					continue
				}

				refValue := refRowValues[refColIdx]

				// Check if the FK value matches the value being deleted
				if valuesEqual(refValue, deletedValue) {
					matchingRows = append(matchingRows, matchingRow{
						key:    cursor.Key(),
						values: refRowValues,
					})
				}
			}
			cursor.Close()

			// Process matching rows based on ON DELETE action
			if len(matchingRows) > 0 {
				switch ref.OnDelete {
				case schema.FKActionNoAction, schema.FKActionRestrict:
					return fmt.Errorf("FOREIGN KEY constraint failed: table '%s' has rows referencing this record (ON DELETE %s)",
						ref.ReferencingTable, ref.OnDelete.String())

				case schema.FKActionCascade:
					// CASCADE: Delete all referencing rows
					for _, row := range matchingRows {
						if err := refTree.Delete(row.key); err != nil {
							return fmt.Errorf("CASCADE delete failed: %w", err)
						}
						// Convert key to rowID for index deletion
						rowID := binary.BigEndian.Uint64(row.key)
						if err := e.deleteFromIndexes(refTable, rowID, row.values); err != nil {
							return fmt.Errorf("CASCADE delete from indexes failed: %w", err)
						}
					}

				case schema.FKActionSetNull:
					// SET NULL: Update FK column to NULL
					for _, row := range matchingRows {
						newValues := make([]types.Value, len(row.values))
						copy(newValues, row.values)
						newValues[refColIdx] = types.NewNull()
						newData := record.Encode(newValues)
						if err := refTree.Insert(row.key, newData); err != nil {
							return fmt.Errorf("SET NULL update failed: %w", err)
						}
						// Convert key to rowID for index update
						rowID := binary.BigEndian.Uint64(row.key)
						// Delete old index entries and add new ones
						if err := e.deleteFromIndexes(refTable, rowID, row.values); err != nil {
							return fmt.Errorf("SET NULL delete from indexes failed: %w", err)
						}
						if err := e.updateIndexes(refTable, rowID, newValues); err != nil {
							return fmt.Errorf("SET NULL update indexes failed: %w", err)
						}
					}

				case schema.FKActionSetDefault:
					// SET DEFAULT: Not yet supported
					return fmt.Errorf("ON DELETE SET DEFAULT not yet implemented")
				}
			}
		}
	}

	return nil
}

// checkForeignKeyOnUpdate checks if updating a row would violate foreign key constraints
// in other tables that reference this table. For CASCADE/SET NULL actions, it performs
// the appropriate modifications to the referencing rows.
func (e *Executor) checkForeignKeyOnUpdate(table *schema.TableDef, oldValues, newValues []types.Value, colMap map[string]int) error {
	// For each column that might be referenced, check for FK constraints
	for i, col := range table.Columns {
		// Get all FK references to this table/column
		refs := e.catalog.GetForeignKeyReferences(table.Name, col.Name)
		if len(refs) == 0 {
			continue
		}

		// Get the old and new values
		if i >= len(oldValues) || i >= len(newValues) {
			continue
		}
		oldValue := oldValues[i]
		newValue := newValues[i]

		// If the value hasn't changed, no need to propagate
		if valuesEqual(oldValue, newValue) {
			continue
		}

		// Check each referencing table for rows that reference the old value
		for _, ref := range refs {
			// Get the referencing table
			refTable := e.catalog.GetTable(ref.ReferencingTable)
			if refTable == nil {
				continue
			}

			// Get or open the B-tree for the referencing table
			refTree := e.trees[ref.ReferencingTable]
			if refTree == nil {
				if refTable.RootPage == 0 {
					continue
				}
				refTree = btree.Open(e.pager, refTable.RootPage)
				e.trees[ref.ReferencingTable] = refTree
			}

			// Find the column index in the referencing table
			var refColIdx int = -1
			if ref.ReferencingColumn != "" {
				// Column-level FK
				_, refColIdx = refTable.GetColumn(ref.ReferencingColumn)
			} else if len(ref.ReferencingColumns) > 0 {
				// Table-level FK - find the corresponding column
				_, refColIdx = refTable.GetColumn(ref.ReferencingColumns[0])
			}
			if refColIdx < 0 {
				continue
			}

			// Collect matching rows first (to avoid modifying while iterating)
			type matchingRow struct {
				key    []byte
				values []types.Value
			}
			var matchingRows []matchingRow

			cursor := refTree.Cursor()
			for cursor.First(); cursor.Valid(); cursor.Next() {
				refRowData := cursor.Value()
				refRowValues := record.Decode(refRowData)

				if refColIdx >= len(refRowValues) {
					continue
				}

				refValue := refRowValues[refColIdx]

				// Check if the FK value matches the old value being updated
				if valuesEqual(refValue, oldValue) {
					matchingRows = append(matchingRows, matchingRow{
						key:    cursor.Key(),
						values: refRowValues,
					})
				}
			}
			cursor.Close()

			// Process matching rows based on ON UPDATE action
			if len(matchingRows) > 0 {
				switch ref.OnUpdate {
				case schema.FKActionNoAction, schema.FKActionRestrict:
					return fmt.Errorf("FOREIGN KEY constraint failed: table '%s' has rows referencing the old value (ON UPDATE %s)",
						ref.ReferencingTable, ref.OnUpdate.String())

				case schema.FKActionCascade:
					// CASCADE: Update FK column to the new value
					for _, row := range matchingRows {
						updatedValues := make([]types.Value, len(row.values))
						copy(updatedValues, row.values)
						updatedValues[refColIdx] = newValue
						updatedData := record.Encode(updatedValues)
						if err := refTree.Insert(row.key, updatedData); err != nil {
							return fmt.Errorf("CASCADE update failed: %w", err)
						}
						// Update indexes for the modified row
						rowID := binary.BigEndian.Uint64(row.key)
						if err := e.deleteFromIndexes(refTable, rowID, row.values); err != nil {
							return fmt.Errorf("CASCADE update delete from indexes failed: %w", err)
						}
						if err := e.updateIndexes(refTable, rowID, updatedValues); err != nil {
							return fmt.Errorf("CASCADE update indexes failed: %w", err)
						}
					}

				case schema.FKActionSetNull:
					// SET NULL: Update FK column to NULL
					for _, row := range matchingRows {
						updatedValues := make([]types.Value, len(row.values))
						copy(updatedValues, row.values)
						updatedValues[refColIdx] = types.NewNull()
						updatedData := record.Encode(updatedValues)
						if err := refTree.Insert(row.key, updatedData); err != nil {
							return fmt.Errorf("SET NULL update failed: %w", err)
						}
						// Update indexes for the modified row
						rowID := binary.BigEndian.Uint64(row.key)
						if err := e.deleteFromIndexes(refTable, rowID, row.values); err != nil {
							return fmt.Errorf("SET NULL update delete from indexes failed: %w", err)
						}
						if err := e.updateIndexes(refTable, rowID, updatedValues); err != nil {
							return fmt.Errorf("SET NULL update indexes failed: %w", err)
						}
					}

				case schema.FKActionSetDefault:
					// SET DEFAULT: Not yet supported
					return fmt.Errorf("ON UPDATE SET DEFAULT not yet implemented")
				}
			}
		}
	}

	return nil
}

// valuesEqual compares two values for equality (used in FK checks)
func valuesEqual(a, b types.Value) bool {
	if a.IsNull() && b.IsNull() {
		return true
	}
	if a.IsNull() || b.IsNull() {
		return false
	}
	if a.Type() != b.Type() {
		// Try numeric comparison
		if isNumeric(a) && isNumeric(b) {
			return toFloat(a) == toFloat(b)
		}
		return false
	}

	switch a.Type() {
	case types.TypeInt:
		return a.Int() == b.Int()
	case types.TypeFloat:
		return a.Float() == b.Float()
	case types.TypeText:
		return a.Text() == b.Text()
	case types.TypeBlob:
		aBlob, bBlob := a.Blob(), b.Blob()
		if len(aBlob) != len(bBlob) {
			return false
		}
		for i := range aBlob {
			if aBlob[i] != bBlob[i] {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func isNumeric(v types.Value) bool {
	return v.Type() == types.TypeInt || v.Type() == types.TypeFloat
}

func toFloat(v types.Value) float64 {
	switch v.Type() {
	case types.TypeInt:
		return float64(v.Int())
	case types.TypeFloat:
		return v.Float()
	default:
		return 0
	}
}

// incrementTableRowCount increments the row count in table statistics
// This provides lightweight incremental updates without full ANALYZE
func (e *Executor) incrementTableRowCount(tableName string, delta int64) {
	stats := e.catalog.GetTableStatistics(tableName)
	if stats == nil {
		// No statistics exist yet, nothing to update
		return
	}

	// Create updated statistics with new row count
	stats.RowCount += delta
	// Note: We don't update column statistics here as that would require
	// scanning the new data. Full column stats require ANALYZE.
	_ = e.catalog.UpdateTableStatistics(tableName, stats)
}

// validateConstraints validates row values against table constraints
func (e *Executor) validateConstraints(table *schema.TableDef, values []types.Value) error {
	// Build column map for expression evaluation
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[col.Name] = i
	}

	// Check column-level constraints
	for idx, colDef := range table.Columns {
		if idx >= len(values) {
			continue
		}
		val := values[idx]

		for _, constraint := range colDef.Constraints {
			switch constraint.Type {
			case schema.ConstraintPrimaryKey:
				// PRIMARY KEY implies NOT NULL for non-INTEGER columns
				// INTEGER PRIMARY KEY allows NULL which triggers AUTOINCREMENT
				if val.IsNull() && colDef.Type != types.TypeInt {
					return fmt.Errorf("NOT NULL constraint violation: PRIMARY KEY column '%s' cannot be NULL", colDef.Name)
				}

			case schema.ConstraintNotNull:
				if val.IsNull() {
					return fmt.Errorf("NOT NULL constraint violation: column '%s' cannot be NULL", colDef.Name)
				}

			case schema.ConstraintCheck:
				// Skip CHECK validation if value is NULL (SQL standard behavior)
				if val.IsNull() {
					continue
				}
				// Parse and evaluate the check expression
				if err := e.validateCheckConstraint(constraint.CheckExpression, values, colMap); err != nil {
					return fmt.Errorf("CHECK constraint violation on column '%s': %w", colDef.Name, err)
				}

			case schema.ConstraintForeignKey:
				// Skip FK validation if value is NULL (NULL is allowed in FK columns)
				if val.IsNull() {
					continue
				}
				// Validate that the referenced value exists
				if err := e.validateForeignKeyValue(constraint.RefTable, constraint.RefColumn, val); err != nil {
					return fmt.Errorf("FOREIGN KEY constraint failed: column '%s' references '%s.%s': %w",
						colDef.Name, constraint.RefTable, constraint.RefColumn, err)
				}
			}
		}
	}

	// Check table-level constraints
	for _, tc := range table.TableConstraints {
		switch tc.Type {
		case schema.ConstraintCheck:
			// Evaluate table-level CHECK constraint
			if err := e.validateCheckConstraint(tc.CheckExpression, values, colMap); err != nil {
				return fmt.Errorf("CHECK constraint violation: %w", err)
			}

		case schema.ConstraintForeignKey:
			// Validate table-level FOREIGN KEY constraint
			for i, colName := range tc.Columns {
				colIdx, ok := colMap[colName]
				if !ok || colIdx >= len(values) {
					continue
				}
				val := values[colIdx]

				// Skip FK validation if value is NULL
				if val.IsNull() {
					continue
				}

				// Get corresponding ref column
				refCol := ""
				if i < len(tc.RefColumns) {
					refCol = tc.RefColumns[i]
				}

				if err := e.validateForeignKeyValue(tc.RefTable, refCol, val); err != nil {
					return fmt.Errorf("FOREIGN KEY constraint failed: column '%s' references '%s.%s': %w",
						colName, tc.RefTable, refCol, err)
				}
			}
		}
	}

	return nil
}

// validateCheckConstraint evaluates a CHECK expression against row values
func (e *Executor) validateCheckConstraint(checkExpr string, values []types.Value, colMap map[string]int) error {
	if checkExpr == "" {
		return nil
	}

	// Parse the check expression
	// Wrap in SELECT to make it a valid statement for parsing
	p := parser.New("SELECT * FROM t WHERE " + checkExpr)
	stmt, err := p.Parse()
	if err != nil {
		// If parsing fails, we can't validate - skip (lenient approach)
		return nil
	}

	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt.Where == nil {
		return nil
	}

	// Evaluate the expression
	result, err := e.evaluateCondition(selectStmt.Where, values, colMap)
	if err != nil {
		return err
	}

	if !result {
		return fmt.Errorf("check expression '%s' evaluated to false", checkExpr)
	}

	return nil
}

// executeSelect handles SELECT using the new optimizer and plan-based execution
func (e *Executor) executeSelect(stmt *parser.SelectStmt) (*Result, error) {
	return e.executeSelectWithCTEs(stmt, nil)
}

// executeSelectWithCTEs handles SELECT with optional CTE context
func (e *Executor) executeSelectWithCTEs(stmt *parser.SelectStmt, cteData map[string]*cteResult) (*Result, error) {
	// Handle WITH clause (CTEs)
	if stmt.With != nil {
		// Materialize each CTE
		cteData = make(map[string]*cteResult)
		for _, cte := range stmt.With.CTEs {
			// Execute the CTE query
			var result *Result
			var err error

			switch q := cte.Query.(type) {
			case *parser.SelectStmt:
				result, err = e.executeSelectWithCTEs(q, cteData)
			case *parser.SetOperation:
				if stmt.With.Recursive {
					result, err = e.executeRecursiveCTE(cte, q, cteData)
				} else {
					result, err = e.executeSetOperationWithCTEs(q, cteData)
				}
			default:
				err = fmt.Errorf("unsupported CTE query type: %T", cte.Query)
			}

			if err != nil {
				return nil, fmt.Errorf("error executing CTE %s: %w", cte.Name, err)
			}

			// Apply defined column names
			columns := result.Columns
			if len(cte.Columns) > 0 {
				if len(cte.Columns) != len(columns) {
					return nil, fmt.Errorf("CTE %s column definition mismatch: defined %d, query returned %d", cte.Name, len(cte.Columns), len(columns))
				}
				columns = cte.Columns
			}

			// Store materialized results
			cteData[cte.Name] = &cteResult{
				columns: columns,
				rows:    result.Rows,
			}
		}
	}

	// Build CTE info for the optimizer
	var cteInfo map[string]*optimizer.CTEInfo
	if cteData != nil {
		cteInfo = make(map[string]*optimizer.CTEInfo)
		for name, data := range cteData {
			cteInfo[name] = &optimizer.CTEInfo{
				Name:    name,
				Columns: data.columns,
				Rows:    int64(len(data.rows)),
			}
		}
	}

	// 1. Build Logical Plan
	plan, err := optimizer.BuildPlanWithCTEs(stmt, e.catalog, cteInfo)
	if err != nil {
		return nil, fmt.Errorf("build plan error: %w", err)
	}

	// 2. Optimize Plan
	opt := optimizer.NewOptimizer()
	plan = opt.Optimize(plan)

	// 3. Execute Plan (with CTE data context)
	iterator, columns, err := e.executePlanWithCTEs(plan, cteData)
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}
	defer iterator.Close()

	// 4. Collect results
	var rows [][]types.Value
	for iterator.Next() {
		val := iterator.Value()
		// Copy value to avoid reference issues if underlying buffer reuse occurs
		rowCopy := make([]types.Value, len(val))
		copy(rowCopy, val)
		rows = append(rows, rowCopy)
	}

	// Check for errors during iteration (e.g., RAISE in triggers)
	if err := iterator.Err(); err != nil {
		return nil, err
	}

	return &Result{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// cteResult holds materialized CTE results
type cteResult struct {
	columns []string
	rows    [][]types.Value
}

// executePlan executes a plan node and returns an iterator and column names
func (e *Executor) executePlan(plan optimizer.PlanNode) (RowIterator, []string, error) {
	return e.executePlanWithCTEs(plan, nil)
}

// executePlanWithCTEs executes a plan node with CTE context
func (e *Executor) executePlanWithCTEs(plan optimizer.PlanNode, cteData map[string]*cteResult) (RowIterator, []string, error) {
	switch node := plan.(type) {
	case *optimizer.CTEScanNode:
		// Return an iterator over materialized CTE data
		cte, ok := cteData[node.CTEName]
		if !ok {
			return nil, nil, fmt.Errorf("CTE %s not found", node.CTEName)
		}

		// Build column names with alias prefix
		var cols []string
		prefix := node.CTEName
		if node.Alias != "" {
			prefix = node.Alias
		}
		for _, col := range cte.columns {
			cols = append(cols, prefix+"."+col)
		}

		return &CTEScanIterator{
			rows:  cte.rows,
			index: -1,
		}, cols, nil

	case *optimizer.TableScanNode:
		// Get B-tree
		tree := e.trees[node.Table.Name]
		if tree == nil {
			// Try to open if not cached (though executeCreateTable caches it, restart might clear it)
			if node.Table.RootPage == 0 {
				return nil, nil, fmt.Errorf("table %s has invalid root page", node.Table.Name)
			}
			tree = btree.Open(e.pager, node.Table.RootPage)
			e.trees[node.Table.Name] = tree
		}

		iterator := NewTableScanIterator(tree)

		// Build column names (with alias prefix if alias exists)
		var cols []string
		prefix := node.Table.Name
		if node.Alias != "" {
			prefix = node.Alias
		}
		for _, col := range node.Table.Columns {
			cols = append(cols, prefix+"."+col.Name)
			// Also add short name?
			// For schema propogation in iterator.go colMap logic, strictly fully qualified is safer?
			// But evaluateExpr uses simple names like "id".
			// So we should probably handle both in colMap construction or here?
			// Let's return fully qualified names here, and helper to build map handles fallback.
		}

		return iterator, cols, nil

	case *optimizer.FilterNode:
		inputIter, inputCols, err := e.executePlanWithCTEs(node.Input, cteData)
		if err != nil {
			return nil, nil, err
		}

		colMap := e.buildColMap(inputCols)

		return &FilterIterator{
			child:     inputIter,
			condition: node.Condition,
			colMap:    colMap,
			executor:  e,
		}, inputCols, nil

	case *optimizer.ProjectionNode:
		inputIter, inputCols, err := e.executePlanWithCTEs(node.Input, cteData)
		if err != nil {
			return nil, nil, err
		}

		colMap := e.buildColMap(inputCols)

		// Projection changes schema
		var outputCols []string
		for _, expr := range node.Expressions {
			// Name?
			// parser.Expression doesn't strictly have a name method.
			// Use alias if we had it, or string representation.
			if colRef, ok := expr.(*parser.ColumnRef); ok {
				outputCols = append(outputCols, colRef.Name)
			} else {
				outputCols = append(outputCols, "?") // Placeholder for complex exprs
			}
		}

		// Check if any expressions are window functions
		hasWindowFunc := false
		for _, expr := range node.Expressions {
			if _, ok := expr.(*parser.WindowFunction); ok {
				hasWindowFunc = true
				break
			}
		}

		if hasWindowFunc {
			// Use WindowIterator for window function support
			return NewWindowIterator(inputIter, node.Expressions, colMap, e), outputCols, nil
		}

		return &ProjectionIterator{
			child:       inputIter,
			expressions: node.Expressions,
			colMap:      colMap,
			executor:    e,
		}, outputCols, nil

	case *optimizer.WindowNode:
		inputIter, inputCols, err := e.executePlanWithCTEs(node.Input, cteData)
		if err != nil {
			return nil, nil, err
		}

		colMap := e.buildColMap(inputCols)

		// Build output column names
		var outputCols []string
		for _, expr := range node.AllExpressions {
			if colRef, ok := expr.(*parser.ColumnRef); ok {
				outputCols = append(outputCols, colRef.Name)
			} else if wf, ok := expr.(*parser.WindowFunction); ok {
				// Use function name for window function columns
				if funcCall, ok := wf.Function.(*parser.FunctionCall); ok {
					outputCols = append(outputCols, funcCall.Name)
				} else {
					outputCols = append(outputCols, "?")
				}
			} else {
				outputCols = append(outputCols, "?")
			}
		}

		return NewWindowFunctionIterator(
			inputIter,
			node.AllExpressions,
			node.WindowFunctions,
			colMap,
			e,
		), outputCols, nil

	case *optimizer.NestedLoopJoinNode:
		leftIter, leftCols, err := e.executePlanWithCTEs(node.Left, cteData)
		if err != nil {
			return nil, nil, err
		}

		// Right side needs to be iterate-able repeatedly.
		// My NestedLoopJoinIterator handles materialization of Right.
		// So we just get the iterator once.
		rightIter, rightCols, err := e.executePlanWithCTEs(node.Right, cteData)
		if err != nil {
			leftIter.Close()
			return nil, nil, err
		}

		combinedCols := append(append([]string{}, leftCols...), rightCols...)
		colMap := e.buildColMap(combinedCols)

		return &NestedLoopJoinIterator{
			left:           leftIter,
			right:          rightIter,
			condition:      node.Condition,
			executor:       e,
			joinType:       node.JoinType,
			combinedMap:    colMap,
			leftSchemaLen:  len(leftCols),
			rightSchemaLen: len(rightCols),
		}, combinedCols, nil

	case *optimizer.HashJoinNode:
		leftIter, leftCols, err := e.executePlanWithCTEs(node.Left, cteData)
		if err != nil {
			return nil, nil, err
		}

		rightIter, rightCols, err := e.executePlanWithCTEs(node.Right, cteData)
		if err != nil {
			leftIter.Close()
			return nil, nil, err
		}

		combinedCols := append(append([]string{}, leftCols...), rightCols...)
		colMap := e.buildColMap(combinedCols)

		// Find column index for left key
		leftKeyIdx := -1
		leftColMap := e.buildColMap(leftCols)
		if idx, ok := leftColMap[node.LeftKey]; ok {
			leftKeyIdx = idx
		}

		// Find column index for right key
		rightKeyIdx := -1
		rightColMap := e.buildColMap(rightCols)
		if idx, ok := rightColMap[node.RightKey]; ok {
			rightKeyIdx = idx
		}

		if leftKeyIdx < 0 {
			leftIter.Close()
			rightIter.Close()
			return nil, nil, fmt.Errorf("hash join: left key column '%s' not found", node.LeftKey)
		}
		if rightKeyIdx < 0 {
			leftIter.Close()
			rightIter.Close()
			return nil, nil, fmt.Errorf("hash join: right key column '%s' not found", node.RightKey)
		}

		return &HashJoinIterator{
			left:           leftIter,
			right:          rightIter,
			executor:       e,
			leftKeyIdx:     leftKeyIdx,
			rightKeyIdx:    rightKeyIdx,
			leftSchemaLen:  len(leftCols),
			rightSchemaLen: len(rightCols),
			combinedMap:    colMap,
		}, combinedCols, nil

	case *optimizer.SortNode:
		inputIter, inputCols, err := e.executePlanWithCTEs(node.Input, cteData)
		if err != nil {
			return nil, nil, err
		}

		colMap := e.buildColMap(inputCols)

		return &SortIterator{
			child:    inputIter,
			orderBy:  node.OrderBy,
			colMap:   colMap,
			executor: e,
		}, inputCols, nil

	case *optimizer.LimitNode:
		inputIter, inputCols, err := e.executePlanWithCTEs(node.Input, cteData)
		if err != nil {
			return nil, nil, err
		}

		// Evaluate limit and offset expressions to get integer values
		limit := int64(-1) // -1 means no limit
		offset := int64(0)

		if node.Limit != nil {
			limitVal, err := e.evaluateLiteralExpr(node.Limit)
			if err != nil {
				inputIter.Close()
				return nil, nil, fmt.Errorf("evaluating LIMIT: %w", err)
			}
			limit = limitVal
		}

		if node.Offset != nil {
			offsetVal, err := e.evaluateLiteralExpr(node.Offset)
			if err != nil {
				inputIter.Close()
				return nil, nil, fmt.Errorf("evaluating OFFSET: %w", err)
			}
			offset = offsetVal
		}

		return &LimitIterator{
			child:  inputIter,
			limit:  limit,
			offset: offset,
		}, inputCols, nil

	case *optimizer.AggregateNode:
		inputIter, inputCols, err := e.executePlanWithCTEs(node.Input, cteData)
		if err != nil {
			return nil, nil, err
		}

		colMap := e.buildColMap(inputCols)

		// Build output column names: groupBy columns + aggregate results
		var outputCols []string
		for _, expr := range node.GroupBy {
			if colRef, ok := expr.(*parser.ColumnRef); ok {
				outputCols = append(outputCols, colRef.Name)
			} else {
				outputCols = append(outputCols, "?")
			}
		}
		// Add a column for COUNT(*) as placeholder
		outputCols = append(outputCols, "COUNT(*)")

		return &HashGroupByIterator{
			child:    inputIter,
			groupBy:  node.GroupBy,
			having:   node.Having,
			colMap:   colMap,
			executor: e,
		}, outputCols, nil

	case *optimizer.DualNode:
		// DualNode produces a single row with no columns (for SELECT without FROM)
		return &DualIterator{}, nil, nil

	case *optimizer.SubqueryScanNode:
		// Execute the subquery plan (used for views and derived tables)
		subIter, subCols, err := e.executePlanWithCTEs(node.SubqueryPlan, cteData)
		if err != nil {
			return nil, nil, fmt.Errorf("executing subquery: %w", err)
		}

		// Rename columns with the alias prefix if provided
		var cols []string
		for _, col := range subCols {
			// If the column already has a prefix, extract just the column name
			parts := strings.Split(col, ".")
			colName := parts[len(parts)-1]
			if node.Alias != "" {
				cols = append(cols, node.Alias+"."+colName)
			} else {
				cols = append(cols, col)
			}
		}

		return subIter, cols, nil

	case *optimizer.TableFunctionNode:
		return e.executeTableFunction(node, cteData)

	default:
		return nil, nil, fmt.Errorf("unsupported plan node: %T", plan)
	}
}

// evaluateLiteralExpr evaluates an expression that should be a literal integer
func (e *Executor) evaluateLiteralExpr(expr parser.Expression) (int64, error) {
	switch ex := expr.(type) {
	case *parser.Literal:
		switch ex.Value.Type() {
		case types.TypeInt:
			return ex.Value.Int(), nil
		case types.TypeFloat:
			return int64(ex.Value.Float()), nil
		default:
			return 0, fmt.Errorf("expected integer literal, got %v", ex.Value.Type())
		}
	default:
		return 0, fmt.Errorf("expected literal expression for LIMIT/OFFSET, got %T", expr)
	}
}

// buildColMap creates a mapping from column names to indices, handling short names
func (e *Executor) buildColMap(cols []string) map[string]int {
	m := make(map[string]int)

	// First pass: add exact names
	for i, name := range cols {
		m[name] = i
	}

	// Second pass: add short names if unambiguous
	// "table.col" -> "col"
	// Count occurrences of short names
	counts := make(map[string]int)
	for _, name := range cols {
		// derived from simple string split logic
		// if name contains dot
		// But wait, name in cols[i] is whatever executePlan returned.
		// For TableScan I returned "Alias.Col".
		// parse logic?
		// naive split
		// But if column name itself contains dot (unlikely in simple SQL)?

		// Find last dot?
		// For now simple assumption: Table.Col
		for i := len(name) - 1; i >= 0; i-- {
			if name[i] == '.' {
				short := name[i+1:]
				counts[short]++
				break
			}
		}
	}

	for i, name := range cols {
		for j := len(name) - 1; j >= 0; j-- {
			if name[j] == '.' {
				short := name[j+1:]
				if counts[short] == 1 {
					m[short] = i
				}
				break
			}
		}
	}

	return m
}

// evaluateExpr evaluates an expression to a value
func (e *Executor) evaluateExpr(expr parser.Expression, rowValues []types.Value, colMap map[string]int) (types.Value, error) {
	switch ex := expr.(type) {
	case *parser.Literal:
		return ex.Value, nil
	case *parser.ColumnRef:
		if colMap == nil {
			return types.NewNull(), fmt.Errorf("column reference not allowed here")
		}
		idx, ok := colMap[ex.Name]
		if !ok {
			return types.NewNull(), fmt.Errorf("column %s not found", ex.Name)
		}
		if idx < len(rowValues) {
			return rowValues[idx], nil
		}
		return types.NewNull(), nil
	case *parser.UnaryExpr:
		right, err := e.evaluateExpr(ex.Right, rowValues, colMap)
		if err != nil {
			return types.NewNull(), err
		}
		if ex.Op == lexer.MINUS {
			if right.Type() == types.TypeInt {
				return types.NewInt(-right.Int()), nil
			}
			if right.Type() == types.TypeFloat {
				return types.NewFloat(-right.Float()), nil
			}
		}
		return types.NewNull(), fmt.Errorf("unsupported unary operator")
	case *parser.BinaryExpr:
		return e.evaluateBinaryExpr(ex, rowValues, colMap)
	case *parser.SubqueryExpr:
		// Execute the subquery and return the scalar result
		result, err := e.executeSelect(ex.Query)
		if err != nil {
			return types.NewNull(), fmt.Errorf("subquery error: %w", err)
		}
		// Scalar subquery must return exactly one row and one column
		if len(result.Rows) == 0 {
			return types.NewNull(), nil
		}
		if len(result.Rows) > 1 {
			return types.NewNull(), fmt.Errorf("scalar subquery returned more than one row")
		}
		if len(result.Rows[0]) == 0 {
			return types.NewNull(), nil
		}
		return result.Rows[0][0], nil
	case *parser.FunctionCall:
		return e.evaluateFunctionCall(ex, rowValues, colMap)
	case *parser.WindowFunction:
		// Window function results are stored in the row under the function name
		// The WindowFunctionIterator has already computed the values
		funcCall, ok := ex.Function.(*parser.FunctionCall)
		if !ok {
			return types.NewNull(), fmt.Errorf("window function has non-FunctionCall inner expression")
		}
		// Look up the window function result by name
		idx, ok := colMap[funcCall.Name]
		if !ok {
			return types.NewNull(), fmt.Errorf("window function result column %s not found", funcCall.Name)
		}
		if idx < len(rowValues) {
			return rowValues[idx], nil
		}
		return types.NewNull(), nil
	case *parser.RaiseExpr:
		// RAISE expressions are used in triggers to abort or ignore
		if ex.Type == parser.RaiseAbort {
			return types.NewNull(), &schema.TriggerAbortError{Message: ex.Message}
		}
		// RaiseIgnore
		return types.NewNull(), schema.ErrTriggerIgnore
	default:
		return types.NewNull(), fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evaluateBinaryExpr evaluates a binary expression
func (e *Executor) evaluateBinaryExpr(expr *parser.BinaryExpr, rowValues []types.Value, colMap map[string]int) (types.Value, error) {
	left, err := e.evaluateExpr(expr.Left, rowValues, colMap)
	if err != nil {
		return types.NewNull(), err
	}
	right, err := e.evaluateExpr(expr.Right, rowValues, colMap)
	if err != nil {
		return types.NewNull(), err
	}

	switch expr.Op {
	case lexer.PLUS:
		return e.addValues(left, right)
	case lexer.MINUS:
		return e.subtractValues(left, right)
	case lexer.STAR:
		return e.multiplyValues(left, right)
	case lexer.SLASH:
		return e.divideValues(left, right)
	default:
		// Comparison operators return 0 or 1
		cmp := e.compareValues(left, right)
		var result bool
		switch expr.Op {
		case lexer.EQ:
			result = cmp == 0
		case lexer.NEQ:
			result = cmp != 0
		case lexer.LT:
			result = cmp < 0
		case lexer.GT:
			result = cmp > 0
		case lexer.LTE:
			result = cmp <= 0
		case lexer.GTE:
			result = cmp >= 0
		default:
			return types.NewNull(), fmt.Errorf("unsupported operator: %v", expr.Op)
		}
		if result {
			return types.NewInt(1), nil
		}
		return types.NewInt(0), nil
	}
}

// evaluateCondition evaluates a WHERE condition and returns true/false
func (e *Executor) evaluateCondition(expr parser.Expression, rowValues []types.Value, colMap map[string]int) (bool, error) {
	switch ex := expr.(type) {
	case *parser.BinaryExpr:
		if ex.Op == lexer.AND {
			left, err := e.evaluateCondition(ex.Left, rowValues, colMap)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evaluateCondition(ex.Right, rowValues, colMap)
		}
		if ex.Op == lexer.OR {
			left, err := e.evaluateCondition(ex.Left, rowValues, colMap)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evaluateCondition(ex.Right, rowValues, colMap)
		}
		// Comparison
		val, err := e.evaluateBinaryExpr(ex, rowValues, colMap)
		if err != nil {
			return false, err
		}
		return val.Int() != 0, nil
	case *parser.InExpr:
		return e.evaluateInExpr(ex, rowValues, colMap)
	case *parser.ExistsExpr:
		return e.evaluateExistsExpr(ex, rowValues, colMap)
	default:
		val, err := e.evaluateExpr(expr, rowValues, colMap)
		if err != nil {
			return false, err
		}
		return !val.IsNull() && val.Int() != 0, nil
	}
}

// evaluateInExpr evaluates an IN expression
func (e *Executor) evaluateInExpr(expr *parser.InExpr, rowValues []types.Value, colMap map[string]int) (bool, error) {
	// Evaluate the left side
	leftVal, err := e.evaluateExpr(expr.Left, rowValues, colMap)
	if err != nil {
		return false, err
	}

	// If left value is NULL, result is NULL (which is false in boolean context)
	if leftVal.IsNull() {
		return false, nil
	}

	// Get the values to check against
	var checkValues []types.Value
	if expr.Subquery != nil {
		// Execute the subquery
		result, err := e.executeSelect(expr.Subquery)
		if err != nil {
			return false, fmt.Errorf("IN subquery error: %w", err)
		}
		// Collect all first-column values
		for _, row := range result.Rows {
			if len(row) > 0 {
				checkValues = append(checkValues, row[0])
			}
		}
	} else {
		// Evaluate the value list
		for _, valExpr := range expr.Values {
			val, err := e.evaluateExpr(valExpr, rowValues, colMap)
			if err != nil {
				return false, err
			}
			checkValues = append(checkValues, val)
		}
	}

	// Check if leftVal is in checkValues
	found := false
	for _, v := range checkValues {
		if e.compareValues(leftVal, v) == 0 {
			found = true
			break
		}
	}

	// Handle NOT IN
	if expr.Not {
		return !found, nil
	}
	return found, nil
}

// evaluateExistsExpr evaluates an EXISTS expression
func (e *Executor) evaluateExistsExpr(expr *parser.ExistsExpr, rowValues []types.Value, colMap map[string]int) (bool, error) {
	// For correlated subqueries, we need to pass the outer row context
	// This is a simplified implementation that handles basic EXISTS
	// For now, we execute the subquery and check if it returns any rows
	result, err := e.executeSelectWithContext(expr.Subquery, rowValues, colMap)
	if err != nil {
		return false, fmt.Errorf("EXISTS subquery error: %w", err)
	}

	exists := len(result.Rows) > 0

	// Handle NOT EXISTS
	if expr.Not {
		return !exists, nil
	}
	return exists, nil
}

// executeSelectWithContext executes a SELECT statement with outer row context for correlated subqueries
func (e *Executor) executeSelectWithContext(stmt *parser.SelectStmt, outerRow []types.Value, outerColMap map[string]int) (*Result, error) {
	// For correlated subqueries, we need to make outer columns available
	// during the subquery execution. This is done by wrapping the WHERE condition
	// to substitute outer column references with their values.

	// Create a copy of the statement with substituted outer references
	stmtCopy := e.substituteOuterReferences(stmt, outerRow, outerColMap)

	return e.executeSelect(stmtCopy)
}

// substituteOuterReferences replaces references to outer query columns with literal values
func (e *Executor) substituteOuterReferences(stmt *parser.SelectStmt, outerRow []types.Value, outerColMap map[string]int) *parser.SelectStmt {
	if stmt == nil || outerRow == nil || outerColMap == nil {
		return stmt
	}

	// Create a shallow copy of the statement
	stmtCopy := *stmt

	// Substitute references in the WHERE clause
	if stmtCopy.Where != nil {
		stmtCopy.Where = e.substituteExprOuterRefs(stmtCopy.Where, outerRow, outerColMap)
	}

	return &stmtCopy
}

// substituteExprOuterRefs recursively substitutes outer column references in an expression
func (e *Executor) substituteExprOuterRefs(expr parser.Expression, outerRow []types.Value, outerColMap map[string]int) parser.Expression {
	if expr == nil {
		return nil
	}

	switch ex := expr.(type) {
	case *parser.ColumnRef:
		// Check if this column is from the outer query
		if idx, ok := outerColMap[ex.Name]; ok {
			if idx < len(outerRow) {
				return &parser.Literal{Value: outerRow[idx]}
			}
		}
		return expr
	case *parser.BinaryExpr:
		return &parser.BinaryExpr{
			Left:  e.substituteExprOuterRefs(ex.Left, outerRow, outerColMap),
			Op:    ex.Op,
			Right: e.substituteExprOuterRefs(ex.Right, outerRow, outerColMap),
		}
	case *parser.UnaryExpr:
		return &parser.UnaryExpr{
			Op:    ex.Op,
			Right: e.substituteExprOuterRefs(ex.Right, outerRow, outerColMap),
		}
	default:
		return expr
	}
}

// evaluateFunctionCall evaluates a function call expression
func (e *Executor) evaluateFunctionCall(expr *parser.FunctionCall, rowValues []types.Value, colMap map[string]int) (types.Value, error) {
	// Evaluate arguments
	var args []types.Value
	for _, argExpr := range expr.Args {
		val, err := e.evaluateExpr(argExpr, rowValues, colMap)
		if err != nil {
			return types.NewNull(), err
		}
		args = append(args, val)
	}

	// Handle built-in functions (case-insensitive)
	switch strings.ToUpper(expr.Name) {
	case "MAX":
		if len(args) == 0 {
			return types.NewNull(), nil
		}
		return args[0], nil // For scalar context, return the value
	case "MIN":
		if len(args) == 0 {
			return types.NewNull(), nil
		}
		return args[0], nil
	case "COUNT":
		// COUNT in scalar context
		return types.NewInt(1), nil
	case "SUM", "AVG":
		if len(args) == 0 {
			return types.NewNull(), nil
		}
		return args[0], nil
	case "COALESCE":
		for _, arg := range args {
			if !arg.IsNull() {
				return arg, nil
			}
		}
		return types.NewNull(), nil
	case "ABS":
		if len(args) == 0 {
			return types.NewNull(), nil
		}
		if args[0].Type() == types.TypeInt {
			v := args[0].Int()
			if v < 0 {
				v = -v
			}
			return types.NewInt(v), nil
		}
		if args[0].Type() == types.TypeFloat {
			v := args[0].Float()
			if v < 0 {
				v = -v
			}
			return types.NewFloat(v), nil
		}
		return types.NewNull(), nil
	case "VECTOR_QUANTIZE":
		return e.executeVectorQuantize(args)
	default:
		// Fall back to function registry for scalar functions
		funcRegistry := vdbe.DefaultFunctionRegistry()
		fn := funcRegistry.Lookup(expr.Name)
		if fn != nil {
			return fn.Call(args), nil
		}
		return types.NewNull(), fmt.Errorf("unknown function: %s", expr.Name)
	}
}

// Helper functions for arithmetic

func (e *Executor) addValues(left, right types.Value) (types.Value, error) {
	if left.Type() == types.TypeInt && right.Type() == types.TypeInt {
		return types.NewInt(left.Int() + right.Int()), nil
	}
	if left.Type() == types.TypeFloat || right.Type() == types.TypeFloat {
		l := e.toFloat(left)
		r := e.toFloat(right)
		return types.NewFloat(l + r), nil
	}
	return types.NewNull(), nil
}

func (e *Executor) subtractValues(left, right types.Value) (types.Value, error) {
	if left.Type() == types.TypeInt && right.Type() == types.TypeInt {
		return types.NewInt(left.Int() - right.Int()), nil
	}
	if left.Type() == types.TypeFloat || right.Type() == types.TypeFloat {
		l := e.toFloat(left)
		r := e.toFloat(right)
		return types.NewFloat(l - r), nil
	}
	return types.NewNull(), nil
}

func (e *Executor) multiplyValues(left, right types.Value) (types.Value, error) {
	if left.Type() == types.TypeInt && right.Type() == types.TypeInt {
		return types.NewInt(left.Int() * right.Int()), nil
	}
	if left.Type() == types.TypeFloat || right.Type() == types.TypeFloat {
		l := e.toFloat(left)
		r := e.toFloat(right)
		return types.NewFloat(l * r), nil
	}
	return types.NewNull(), nil
}

func (e *Executor) divideValues(left, right types.Value) (types.Value, error) {
	if left.Type() == types.TypeFloat || right.Type() == types.TypeFloat {
		l := e.toFloat(left)
		r := e.toFloat(right)
		if r == 0 {
			return types.NewNull(), nil
		}
		return types.NewFloat(l / r), nil
	}
	if left.Type() == types.TypeInt && right.Type() == types.TypeInt {
		if right.Int() == 0 {
			return types.NewNull(), nil
		}
		return types.NewInt(left.Int() / right.Int()), nil
	}
	return types.NewNull(), nil
}

func (e *Executor) toFloat(v types.Value) float64 {
	switch v.Type() {
	case types.TypeInt:
		return float64(v.Int())
	case types.TypeFloat:
		return v.Float()
	default:
		return 0
	}
}

// compareValues compares two values, returns -1, 0, or 1
func (e *Executor) compareValues(left, right types.Value) int {
	// Handle NULL
	if left.IsNull() && right.IsNull() {
		return 0
	}
	if left.IsNull() {
		return -1
	}
	if right.IsNull() {
		return 1
	}

	// Same type comparisons
	if left.Type() == right.Type() {
		switch left.Type() {
		case types.TypeInt:
			l, r := left.Int(), right.Int()
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
			return 0
		case types.TypeFloat:
			l, r := left.Float(), right.Float()
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
			return 0
		case types.TypeText:
			l, r := left.Text(), right.Text()
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
			return 0
		}
	}

	// Mixed numeric types
	if (left.Type() == types.TypeInt || left.Type() == types.TypeFloat) &&
		(right.Type() == types.TypeInt || right.Type() == types.TypeFloat) {
		l := e.toFloat(left)
		r := e.toFloat(right)
		if l < r {
			return -1
		}
		if l > r {
			return 1
		}
		return 0
	}

	// Default: compare by type order
	if left.Type() < right.Type() {
		return -1
	}
	return 1
}

// executeAnalyze handles ANALYZE statement
// Collects statistics for the specified table(s) and stores them in the catalog
func (e *Executor) executeAnalyze(stmt *parser.AnalyzeStmt) (*Result, error) {
	var tablesToAnalyze []string

	if stmt.TableName == "" {
		// Analyze all tables
		tablesToAnalyze = e.catalog.ListTables()
	} else {
		// Check if the name is a table
		if e.catalog.GetTable(stmt.TableName) != nil {
			tablesToAnalyze = []string{stmt.TableName}
		} else {
			return nil, fmt.Errorf("table not found: %s", stmt.TableName)
		}
	}

	tablesAnalyzed := int64(0)

	for _, tableName := range tablesToAnalyze {
		table := e.catalog.GetTable(tableName)
		if table == nil {
			continue
		}

		// Get the B-tree for this table
		tree := e.trees[tableName]
		if tree == nil {
			// Table exists but no tree (empty table)
			stats := CreateTableStatistics(tableName, nil, table.Columns, 0)
			_ = e.catalog.UpdateTableStatistics(tableName, stats)
			tablesAnalyzed++
			continue
		}

		// Scan all rows from the table
		rows, err := e.scanAllRows(tableName, table)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table %s: %w", tableName, err)
		}

		totalRows := int64(len(rows))

		// Sample if necessary
		sampler := NewTableSampler(1000) // Default sample size
		samples := sampler.Sample(rows)

		// Collect statistics with histogram (4 buckets by default)
		stats := &schema.TableStatistics{
			TableName:    tableName,
			RowCount:     totalRows,
			LastAnalyzed: time.Now(),
			ColumnStats:  CollectColumnStatisticsWithHistogram(samples, table.Columns, totalRows, 4),
		}

		// Store in catalog
		err = e.catalog.UpdateTableStatistics(tableName, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to update statistics for %s: %w", tableName, err)
		}

		tablesAnalyzed++
	}

	return &Result{
		RowsAffected: tablesAnalyzed,
	}, nil
}

// scanAllRows scans all rows from a table and returns them as a slice of value slices
func (e *Executor) scanAllRows(tableName string, table *schema.TableDef) ([][]types.Value, error) {
	tree := e.trees[tableName]
	if tree == nil {
		return nil, nil
	}

	var rows [][]types.Value

	// Iterate through the B-tree using a cursor
	cursor := tree.Cursor()
	cursor.First()

	for cursor.Valid() {
		valBytes := cursor.Value()
		if valBytes != nil {
			// Decode the record - returns []types.Value directly
			row := record.Decode(valBytes)
			if row != nil {
				// Copy to avoid any potential buffer reuse issues
				rowCopy := make([]types.Value, len(row))
				copy(rowCopy, row)
				rows = append(rows, rowCopy)
			}
		}
		cursor.Next()
	}

	cursor.Close()
	return rows, nil
}

// executeAlterTable handles ALTER TABLE statements
func (e *Executor) executeAlterTable(stmt *parser.AlterTableStmt) (*Result, error) {
	switch stmt.Action {
	case parser.AlterActionAddColumn:
		return e.executeAlterTableAddColumn(stmt)
	case parser.AlterActionDropColumn:
		return e.executeAlterTableDropColumn(stmt)
	case parser.AlterActionRenameTable:
		return e.executeAlterTableRename(stmt)
	default:
		return nil, fmt.Errorf("unsupported ALTER TABLE action")
	}
}

// executeAlterTableAddColumn handles ALTER TABLE ADD COLUMN
func (e *Executor) executeAlterTableAddColumn(stmt *parser.AlterTableStmt) (*Result, error) {
	// Convert parser column def to schema column def
	col := schema.ColumnDef{
		Name:       stmt.NewColumn.Name,
		Type:       stmt.NewColumn.Type,
		PrimaryKey: stmt.NewColumn.PrimaryKey,
		NotNull:    stmt.NewColumn.NotNull,
		VectorDim:  stmt.NewColumn.VectorDim,
	}

	// Build constraints if any
	var constraints []schema.Constraint

	if stmt.NewColumn.PrimaryKey {
		constraints = append(constraints, schema.Constraint{Type: schema.ConstraintPrimaryKey})
	}
	if stmt.NewColumn.NotNull {
		constraints = append(constraints, schema.Constraint{Type: schema.ConstraintNotNull})
	}
	if stmt.NewColumn.Unique {
		constraints = append(constraints, schema.Constraint{Type: schema.ConstraintUnique})
	}
	if stmt.NewColumn.CheckExpr != nil {
		constraints = append(constraints, schema.Constraint{
			Type:            schema.ConstraintCheck,
			CheckExpression: exprToString(stmt.NewColumn.CheckExpr),
		})
	}
	if stmt.NewColumn.ForeignKey != nil {
		constraints = append(constraints, schema.Constraint{
			Type:      schema.ConstraintForeignKey,
			RefTable:  stmt.NewColumn.ForeignKey.RefTable,
			RefColumn: stmt.NewColumn.ForeignKey.RefColumn,
			OnDelete:  convertFKAction(stmt.NewColumn.ForeignKey.OnDelete),
			OnUpdate:  convertFKAction(stmt.NewColumn.ForeignKey.OnUpdate),
		})
	}

	col.Constraints = constraints

	// Add column to catalog
	if err := e.catalog.AddColumn(stmt.TableName, col); err != nil {
		return nil, fmt.Errorf("failed to add column: %w", err)
	}

	return &Result{}, nil
}

// executeAlterTableDropColumn handles ALTER TABLE DROP COLUMN
func (e *Executor) executeAlterTableDropColumn(stmt *parser.AlterTableStmt) (*Result, error) {
	if err := e.catalog.DropColumn(stmt.TableName, stmt.ColumnName); err != nil {
		return nil, fmt.Errorf("failed to drop column: %w", err)
	}

	return &Result{}, nil
}

// executeAlterTableRename handles ALTER TABLE RENAME TO
func (e *Executor) executeAlterTableRename(stmt *parser.AlterTableStmt) (*Result, error) {
	// Update B-tree reference
	if tree, exists := e.trees[stmt.TableName]; exists {
		delete(e.trees, stmt.TableName)
		e.trees[stmt.NewName] = tree
	}

	// Update rowid reference
	if rowid, exists := e.rowid[stmt.TableName]; exists {
		delete(e.rowid, stmt.TableName)
		e.rowid[stmt.NewName] = rowid
	}

	// Rename in catalog
	if err := e.catalog.RenameTable(stmt.TableName, stmt.NewName); err != nil {
		return nil, fmt.Errorf("failed to rename table: %w", err)
	}

	return &Result{}, nil
}

// HasActiveTransaction returns true if there is an active transaction
func (e *Executor) HasActiveTransaction() bool {
	return e.currentTx != nil && e.currentTx.IsActive()
}

// executeBegin handles BEGIN [TRANSACTION]
func (e *Executor) executeBegin(_ *parser.BeginStmt) (*Result, error) {
	// Check if there's already an active transaction
	if e.HasActiveTransaction() {
		return nil, fmt.Errorf("cannot start a transaction within a transaction")
	}

	// Start a new transaction
	e.currentTx = e.txManager.Begin()

	return &Result{}, nil
}

// executeCommit handles COMMIT [TRANSACTION]
func (e *Executor) executeCommit(_ *parser.CommitStmt) (*Result, error) {
	// Check if there's an active transaction
	if !e.HasActiveTransaction() {
		return nil, fmt.Errorf("cannot commit: no transaction is active")
	}

	// Commit the transaction
	if err := e.txManager.Commit(e.currentTx); err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}

	e.currentTx = nil

	return &Result{}, nil
}

// executeRollback handles ROLLBACK [TRANSACTION]
func (e *Executor) executeRollback(_ *parser.RollbackStmt) (*Result, error) {
	// Check if there's an active transaction
	if !e.HasActiveTransaction() {
		return nil, fmt.Errorf("cannot rollback: no transaction is active")
	}

	// Rollback the transaction
	if err := e.txManager.Rollback(e.currentTx); err != nil {
		return nil, fmt.Errorf("rollback failed: %w", err)
	}

	e.currentTx = nil

	return &Result{}, nil
}

// executeSavepoint handles SAVEPOINT savepoint_name
func (e *Executor) executeSavepoint(stmt *parser.SavepointStmt) (*Result, error) {
	// Check if there's an active transaction
	if !e.HasActiveTransaction() {
		return nil, fmt.Errorf("cannot create savepoint: no transaction is active")
	}

	// Create savepoint in the transaction
	if err := e.currentTx.Savepoint(stmt.Name); err != nil {
		return nil, fmt.Errorf("savepoint failed: %w", err)
	}

	return &Result{}, nil
}

// executeRollbackTo handles ROLLBACK TO [SAVEPOINT] savepoint_name
func (e *Executor) executeRollbackTo(stmt *parser.RollbackToStmt) (*Result, error) {
	// Check if there's an active transaction
	if !e.HasActiveTransaction() {
		return nil, fmt.Errorf("cannot rollback to savepoint: no transaction is active")
	}

	// Rollback to the savepoint
	if err := e.currentTx.RollbackTo(stmt.Name); err != nil {
		return nil, fmt.Errorf("rollback to savepoint failed: %w", err)
	}

	return &Result{}, nil
}

// executeRelease handles RELEASE [SAVEPOINT] savepoint_name
func (e *Executor) executeRelease(stmt *parser.ReleaseStmt) (*Result, error) {
	// Check if there's an active transaction
	if !e.HasActiveTransaction() {
		return nil, fmt.Errorf("cannot release savepoint: no transaction is active")
	}

	// Release the savepoint
	if err := e.currentTx.Release(stmt.Name); err != nil {
		return nil, fmt.Errorf("release savepoint failed: %w", err)
	}

	return &Result{}, nil
}

// executeSetOperation handles UNION, INTERSECT, EXCEPT operations
func (e *Executor) executeSetOperation(stmt *parser.SetOperation) (*Result, error) {
	// Execute left and right SELECT statements
	leftResult, err := e.executeSelect(stmt.Left)
	if err != nil {
		return nil, fmt.Errorf("left query error: %w", err)
	}

	rightResult, err := e.executeSelect(stmt.Right)
	if err != nil {
		return nil, fmt.Errorf("right query error: %w", err)
	}

	// Use left result's columns as the output columns
	columns := leftResult.Columns

	switch stmt.Operator {
	case parser.SetOpUnion:
		if stmt.All {
			// UNION ALL: Simply concatenate results
			rows := append(leftResult.Rows, rightResult.Rows...)
			return &Result{Columns: columns, Rows: rows}, nil
		}
		// UNION: Concatenate and deduplicate
		rows := e.unionDedup(leftResult.Rows, rightResult.Rows)
		return &Result{Columns: columns, Rows: rows}, nil

	case parser.SetOpIntersect:
		if stmt.All {
			// INTERSECT ALL: Keep duplicates based on count in both
			rows := e.intersectAll(leftResult.Rows, rightResult.Rows)
			return &Result{Columns: columns, Rows: rows}, nil
		}
		// INTERSECT: Keep only rows present in both (deduplicated)
		rows := e.intersect(leftResult.Rows, rightResult.Rows)
		return &Result{Columns: columns, Rows: rows}, nil

	case parser.SetOpExcept:
		if stmt.All {
			// EXCEPT ALL: Remove one copy for each matching right row
			rows := e.exceptAll(leftResult.Rows, rightResult.Rows)
			return &Result{Columns: columns, Rows: rows}, nil
		}
		// EXCEPT: Remove all rows present in right from left
		rows := e.except(leftResult.Rows, rightResult.Rows)
		return &Result{Columns: columns, Rows: rows}, nil

	default:
		return nil, fmt.Errorf("unsupported set operation: %v", stmt.Operator)
	}
}

// rowKey creates a string key for a row for use in maps
func rowKey(row []types.Value) string {
	var key string
	for i, val := range row {
		if i > 0 {
			key += "|"
		}
		if val.IsNull() {
			key += "NULL"
		} else {
			switch val.Type() {
			case types.TypeInt:
				key += fmt.Sprintf("I:%d", val.Int())
			case types.TypeFloat:
				key += fmt.Sprintf("F:%f", val.Float())
			case types.TypeText:
				key += fmt.Sprintf("T:%s", val.Text())
			case types.TypeBlob:
				key += fmt.Sprintf("B:%x", val.Blob())
			default:
				key += "?"
			}
		}
	}
	return key
}

// unionDedup returns the union of two result sets with duplicates removed
func (e *Executor) unionDedup(left, right [][]types.Value) [][]types.Value {
	seen := make(map[string]bool)
	var result [][]types.Value

	// Add all rows from left, tracking seen rows
	for _, row := range left {
		key := rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	// Add rows from right that haven't been seen
	for _, row := range right {
		key := rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// intersect returns rows present in both left and right (deduplicated)
func (e *Executor) intersect(left, right [][]types.Value) [][]types.Value {
	// Build set of right rows
	rightSet := make(map[string]bool)
	for _, row := range right {
		rightSet[rowKey(row)] = true
	}

	// Keep left rows that are in right, deduplicating
	seen := make(map[string]bool)
	var result [][]types.Value
	for _, row := range left {
		key := rowKey(row)
		if rightSet[key] && !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// intersectAll returns intersection preserving duplicates based on min count
func (e *Executor) intersectAll(left, right [][]types.Value) [][]types.Value {
	// Count occurrences in right
	rightCounts := make(map[string]int)
	for _, row := range right {
		rightCounts[rowKey(row)]++
	}

	// For each left row, include if count in right > 0, decrement count
	var result [][]types.Value
	for _, row := range left {
		key := rowKey(row)
		if rightCounts[key] > 0 {
			rightCounts[key]--
			result = append(result, row)
		}
	}

	return result
}

// except returns left rows not present in right (deduplicated)
func (e *Executor) except(left, right [][]types.Value) [][]types.Value {
	// Build set of right rows
	rightSet := make(map[string]bool)
	for _, row := range right {
		rightSet[rowKey(row)] = true
	}

	// Keep left rows not in right, deduplicating
	seen := make(map[string]bool)
	var result [][]types.Value
	for _, row := range left {
		key := rowKey(row)
		if !rightSet[key] && !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// exceptAll returns left minus right, removing one right occurrence per match
func (e *Executor) exceptAll(left, right [][]types.Value) [][]types.Value {
	// Count occurrences in right
	rightCounts := make(map[string]int)
	for _, row := range right {
		rightCounts[rowKey(row)]++
	}

	// Include left rows, skipping one for each right occurrence
	var result [][]types.Value
	for _, row := range left {
		key := rowKey(row)
		if rightCounts[key] > 0 {
			rightCounts[key]--
			// Skip this row (removed by EXCEPT)
		} else {
			result = append(result, row)
		}
	}

	return result
}

// executeExplain handles EXPLAIN and EXPLAIN QUERY PLAN statements
func (e *Executor) executeExplain(stmt *parser.ExplainStmt) (*Result, error) {
	if stmt.QueryPlan {
		return e.executeExplainQueryPlan(stmt)
	}
	return e.executeExplainBytecode(stmt)
}

// executeExplainBytecode outputs VDBE bytecode for EXPLAIN statement
func (e *Executor) executeExplainBytecode(stmt *parser.ExplainStmt) (*Result, error) {
	// Compile the inner statement to VDBE bytecode
	// Use defer/recover to handle panics in compiler (some statements not fully supported)
	var program *vdbe.Program
	var compileErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				compileErr = fmt.Errorf("compiler panic: %v", r)
			}
		}()
		compiler := vdbe.NewCompiler(e.catalog, e.pager)
		program, compileErr = compiler.Compile(stmt.Statement)
	}()

	if compileErr != nil || program == nil {
		// If compilation fails, fall back to query plan mode
		return e.executeExplainQueryPlan(stmt)
	}

	// Build result with SQLite-compatible EXPLAIN format
	// Columns: addr, opcode, p1, p2, p3, p4, p5, comment
	result := &Result{
		Columns: []string{"addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment"},
		Rows:    make([][]types.Value, 0, program.Len()),
	}

	for i := 0; i < program.Len(); i++ {
		instr := program.Get(i)
		if instr == nil {
			continue
		}

		// Format P4 as string
		var p4Str string
		if instr.P4 != nil {
			p4Str = fmt.Sprintf("%v", instr.P4)
		}

		// Generate comment based on opcode
		comment := e.generateOpcodeComment(instr)

		row := []types.Value{
			types.NewInt(int64(i)),           // addr
			types.NewText(instr.Op.String()), // opcode
			types.NewInt(int64(instr.P1)),    // p1
			types.NewInt(int64(instr.P2)),    // p2
			types.NewInt(int64(instr.P3)),    // p3
			types.NewText(p4Str),             // p4
			types.NewInt(int64(instr.P5)),    // p5
			types.NewText(comment),           // comment
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// generateOpcodeComment generates a human-readable comment for an instruction
func (e *Executor) generateOpcodeComment(instr *vdbe.Instruction) string {
	switch instr.Op {
	case vdbe.OpInit:
		return "Start at " + fmt.Sprintf("%d", instr.P2)
	case vdbe.OpHalt:
		return "End of program"
	case vdbe.OpOpenRead:
		return fmt.Sprintf("Open cursor %d for reading", instr.P1)
	case vdbe.OpOpenWrite:
		return fmt.Sprintf("Open cursor %d for writing", instr.P1)
	case vdbe.OpRewind:
		return fmt.Sprintf("Rewind cursor %d; goto %d if empty", instr.P1, instr.P2)
	case vdbe.OpNext:
		return fmt.Sprintf("Advance cursor %d; goto %d if more", instr.P1, instr.P2)
	case vdbe.OpColumn:
		return fmt.Sprintf("Read column %d from cursor %d into r[%d]", instr.P2, instr.P1, instr.P3)
	case vdbe.OpResultRow:
		return fmt.Sprintf("Output r[%d..%d]", instr.P1, instr.P1+instr.P2-1)
	case vdbe.OpClose:
		return fmt.Sprintf("Close cursor %d", instr.P1)
	case vdbe.OpInteger:
		return fmt.Sprintf("r[%d] = %d", instr.P2, instr.P1)
	case vdbe.OpString:
		if s, ok := instr.P4.(string); ok {
			return fmt.Sprintf("r[%d] = '%s'", instr.P2, s)
		}
		return fmt.Sprintf("r[%d] = string", instr.P2)
	case vdbe.OpInsert:
		return fmt.Sprintf("Insert into cursor %d", instr.P1)
	case vdbe.OpGoto:
		return fmt.Sprintf("Goto %d", instr.P2)
	default:
		return ""
	}
}

// executeExplainQueryPlan outputs query plan for EXPLAIN QUERY PLAN
func (e *Executor) executeExplainQueryPlan(stmt *parser.ExplainStmt) (*Result, error) {
	// Build result with SQLite-compatible EXPLAIN QUERY PLAN format
	// Columns: id, parent, notused, detail
	result := &Result{
		Columns: []string{"id", "parent", "notused", "detail"},
		Rows:    make([][]types.Value, 0),
	}

	// For SELECT statements, we can generate a query plan
	selectStmt, isSelect := stmt.Statement.(*parser.SelectStmt)
	if isSelect {
		return e.explainSelectPlan(selectStmt, result)
	}

	// For other statements, generate a simple plan
	return e.explainGenericPlan(stmt.Statement, result)
}

// explainSelectPlan generates query plan for SELECT statement
func (e *Executor) explainSelectPlan(stmt *parser.SelectStmt, result *Result) (*Result, error) {
	rowID := 0

	// Use the optimizer to build and explain the plan
	plan, err := optimizer.BuildPlan(stmt, e.catalog)
	if err != nil {
		// Fall back to simple plan if optimizer fails
		return e.explainSelectSimple(stmt, result)
	}

	// Recursively explain the plan nodes
	e.explainPlanNode(plan, 0, &rowID, result)

	return result, nil
}

// explainPlanNode recursively explains plan nodes
func (e *Executor) explainPlanNode(node optimizer.PlanNode, parentID int, rowID *int, result *Result) {
	currentID := *rowID
	*rowID++

	var detail string

	switch n := node.(type) {
	case *optimizer.TableScanNode:
		detail = fmt.Sprintf("SCAN TABLE %s", n.Table.Name)
		if n.Alias != "" && n.Alias != n.Table.Name {
			detail += " AS " + n.Alias
		}
		if n.Cost > 0 {
			detail += fmt.Sprintf(" (~%d rows)", n.Rows)
		}

	case *optimizer.IndexScanNode:
		tableName := ""
		if n.Table != nil {
			tableName = n.Table.Name
		}
		detail = fmt.Sprintf("SEARCH TABLE %s USING INDEX %s", tableName, n.IndexName)

	case *optimizer.FilterNode:
		detail = fmt.Sprintf("FILTER")
		// Add child
		row := []types.Value{
			types.NewInt(int64(currentID)),
			types.NewInt(int64(parentID)),
			types.NewInt(0),
			types.NewText(detail),
		}
		result.Rows = append(result.Rows, row)
		e.explainPlanNode(n.Input, currentID, rowID, result)
		return

	case *optimizer.NestedLoopJoinNode:
		detail = fmt.Sprintf("NESTED LOOP JOIN")
		row := []types.Value{
			types.NewInt(int64(currentID)),
			types.NewInt(int64(parentID)),
			types.NewInt(0),
			types.NewText(detail),
		}
		result.Rows = append(result.Rows, row)
		e.explainPlanNode(n.Left, currentID, rowID, result)
		e.explainPlanNode(n.Right, currentID, rowID, result)
		return

	case *optimizer.HashJoinNode:
		detail = fmt.Sprintf("HASH JOIN")
		row := []types.Value{
			types.NewInt(int64(currentID)),
			types.NewInt(int64(parentID)),
			types.NewInt(0),
			types.NewText(detail),
		}
		result.Rows = append(result.Rows, row)
		e.explainPlanNode(n.Left, currentID, rowID, result)
		e.explainPlanNode(n.Right, currentID, rowID, result)
		return

	case *optimizer.SortNode:
		detail = "SORT"
		row := []types.Value{
			types.NewInt(int64(currentID)),
			types.NewInt(int64(parentID)),
			types.NewInt(0),
			types.NewText(detail),
		}
		result.Rows = append(result.Rows, row)
		e.explainPlanNode(n.Input, currentID, rowID, result)
		return

	case *optimizer.LimitNode:
		detail = "LIMIT"
		// Try to extract the limit value if it's a literal
		if n.Limit != nil {
			if lit, ok := n.Limit.(*parser.Literal); ok {
				detail = fmt.Sprintf("LIMIT %d", lit.Value.Int())
			}
		}
		if n.Offset != nil {
			if lit, ok := n.Offset.(*parser.Literal); ok {
				detail += fmt.Sprintf(" OFFSET %d", lit.Value.Int())
			} else {
				detail += " OFFSET ?"
			}
		}
		row := []types.Value{
			types.NewInt(int64(currentID)),
			types.NewInt(int64(parentID)),
			types.NewInt(0),
			types.NewText(detail),
		}
		result.Rows = append(result.Rows, row)
		e.explainPlanNode(n.Input, currentID, rowID, result)
		return

	case *optimizer.AggregateNode:
		detail = "AGGREGATE"
		row := []types.Value{
			types.NewInt(int64(currentID)),
			types.NewInt(int64(parentID)),
			types.NewInt(0),
			types.NewText(detail),
		}
		result.Rows = append(result.Rows, row)
		e.explainPlanNode(n.Input, currentID, rowID, result)
		return

	case *optimizer.ProjectionNode:
		// Skip projection node, just process child
		e.explainPlanNode(n.Input, parentID, rowID, result)
		*rowID-- // Don't count this as a row
		return

	default:
		detail = fmt.Sprintf("OPERATION (%T)", node)
	}

	row := []types.Value{
		types.NewInt(int64(currentID)),
		types.NewInt(int64(parentID)),
		types.NewInt(0),
		types.NewText(detail),
	}
	result.Rows = append(result.Rows, row)
}

// explainSelectSimple generates a simple plan when optimizer is not available
func (e *Executor) explainSelectSimple(stmt *parser.SelectStmt, result *Result) (*Result, error) {
	rowID := 0

	// Extract table names from FROM clause
	tables := e.extractTableNames(stmt.From)

	for _, tableName := range tables {
		row := []types.Value{
			types.NewInt(int64(rowID)),
			types.NewInt(0),
			types.NewInt(0),
			types.NewText(fmt.Sprintf("SCAN TABLE %s", tableName)),
		}
		result.Rows = append(result.Rows, row)
		rowID++
	}

	return result, nil
}

// extractTableNames extracts table names from a table reference
func (e *Executor) extractTableNames(ref parser.TableReference) []string {
	var names []string

	switch t := ref.(type) {
	case *parser.Table:
		names = append(names, t.Name)
	case *parser.Join:
		names = append(names, e.extractTableNames(t.Left)...)
		names = append(names, e.extractTableNames(t.Right)...)
	case *parser.DerivedTable:
		names = append(names, "(subquery)")
	}

	return names
}

// explainGenericPlan generates a plan for non-SELECT statements
func (e *Executor) explainGenericPlan(stmt parser.Statement, result *Result) (*Result, error) {
	var detail string

	switch s := stmt.(type) {
	case *parser.InsertStmt:
		detail = fmt.Sprintf("INSERT INTO TABLE %s", s.TableName)
	case *parser.UpdateStmt:
		detail = fmt.Sprintf("UPDATE TABLE %s", s.TableName)
	case *parser.DeleteStmt:
		detail = fmt.Sprintf("DELETE FROM TABLE %s", s.TableName)
	case *parser.CreateTableStmt:
		detail = fmt.Sprintf("CREATE TABLE %s", s.TableName)
	default:
		detail = fmt.Sprintf("OPERATION (%T)", stmt)
	}

	row := []types.Value{
		types.NewInt(0),
		types.NewInt(0),
		types.NewInt(0),
		types.NewText(detail),
	}
	result.Rows = append(result.Rows, row)

	return result, nil
}

// findIntegerPrimaryKeyColumn returns the index of an INTEGER PRIMARY KEY column,
// or -1 if no such column exists. This is used for AUTOINCREMENT behavior.
func (e *Executor) findIntegerPrimaryKeyColumn(table *schema.TableDef) int {
	for i, col := range table.Columns {
		if col.PrimaryKey && col.Type == types.TypeInt {
			return i
		}
	}
	return -1
}

// buildColumnMapping builds a mapping from input column positions to table column positions.
// Returns nil if stmt.Columns is nil/empty (meaning all columns in order).
func (e *Executor) buildColumnMapping(stmtColumns []string, table *schema.TableDef) []int {
	if len(stmtColumns) == 0 {
		return nil // No mapping needed, values are in table column order
	}

	mapping := make([]int, len(stmtColumns))
	for i, colName := range stmtColumns {
		for j, col := range table.Columns {
			if strings.EqualFold(col.Name, colName) {
				mapping[i] = j
				break
			}
		}
	}
	return mapping
}

// mapInputToColumns maps input values to a full row according to column mapping.
// If colMapping is nil, returns inputValues as-is (assumes all columns in order).
// For missing columns, DEFAULT values are applied if available, otherwise NULL.
func (e *Executor) mapInputToColumns(inputValues []types.Value, colMapping []int, table *schema.TableDef) []types.Value {
	if colMapping == nil {
		return inputValues
	}

	// Create a full row with DEFAULT values or NULLs for all columns
	values := make([]types.Value, len(table.Columns))
	for i, col := range table.Columns {
		// Check for DEFAULT constraint
		defaultConstraint := col.GetConstraint(schema.ConstraintDefault)
		if defaultConstraint != nil && defaultConstraint.DefaultValue != nil {
			values[i] = *defaultConstraint.DefaultValue
		} else {
			values[i] = types.NewNull()
		}
	}

	// Map input values to their table positions (overrides defaults)
	for i, val := range inputValues {
		if i < len(colMapping) {
			values[colMapping[i]] = val
		}
	}

	return values
}

// getNextAutoIncrementID returns the next auto-increment ID for the table.
func (e *Executor) getNextAutoIncrementID(tableName string) int64 {
	maxID := e.maxRowid[tableName]
	nextID := maxID + 1
	e.maxRowid[tableName] = nextID
	return nextID
}

// trackMaxRowID updates the max rowid if the given id is larger.
func (e *Executor) trackMaxRowID(tableName string, id int64) {
	if id > e.maxRowid[tableName] {
		e.maxRowid[tableName] = id
	}
}

// createPrimaryKeyIndex creates a unique index for PRIMARY KEY columns.
// This is called automatically during CREATE TABLE.
func (e *Executor) createPrimaryKeyIndex(table *schema.TableDef) error {
	// Find PRIMARY KEY columns
	var pkColumns []string

	// Check for column-level PRIMARY KEY
	for _, col := range table.Columns {
		if col.PrimaryKey {
			pkColumns = append(pkColumns, col.Name)
		}
	}

	// Check for table-level PRIMARY KEY (composite key)
	for _, tc := range table.TableConstraints {
		if tc.Type == schema.ConstraintPrimaryKey {
			pkColumns = tc.Columns
			break
		}
	}

	// No PRIMARY KEY defined
	if len(pkColumns) == 0 {
		return nil
	}

	// Generate index name: pk_<tablename> or pk_<tablename>_<column>
	var indexName string
	if len(pkColumns) == 1 {
		indexName = fmt.Sprintf("pk_%s_%s", table.Name, pkColumns[0])
	} else {
		indexName = fmt.Sprintf("pk_%s", table.Name)
	}

	// Create B-tree for the index
	indexTree, err := btree.Create(e.pager)
	if err != nil {
		return fmt.Errorf("failed to create primary key index btree: %w", err)
	}

	// Store the index tree
	idxTreeName := "index:" + indexName
	e.trees[idxTreeName] = indexTree

	// Build column index map
	colIndexes := make([]int, len(pkColumns))
	for i, colName := range pkColumns {
		_, idx := table.GetColumn(colName)
		if idx < 0 {
			return fmt.Errorf("primary key column %s not found", colName)
		}
		colIndexes[i] = idx
	}

	// Create index definition
	idx := &schema.IndexDef{
		Name:      indexName,
		TableName: table.Name,
		Columns:   pkColumns,
		Type:      schema.IndexTypeBTree,
		Unique:    true, // PRIMARY KEY is always unique
		RootPage:  indexTree.RootPage(),
	}

	// Add to catalog
	if err := e.catalog.CreateIndex(idx); err != nil {
		return err
	}

	return nil
}

// createUniqueConstraintIndexes creates unique indexes for UNIQUE column constraints.
// This is called automatically during CREATE TABLE.
func (e *Executor) createUniqueConstraintIndexes(table *schema.TableDef) error {
	// Process column-level UNIQUE constraints
	for _, col := range table.Columns {
		// Check if column has UNIQUE constraint
		hasUnique := false
		for _, constraint := range col.Constraints {
			if constraint.Type == schema.ConstraintUnique {
				hasUnique = true
				break
			}
		}

		if !hasUnique {
			continue
		}

		// Skip if this column is already a PRIMARY KEY (PRIMARY KEY already creates unique index)
		if col.PrimaryKey {
			continue
		}

		// Generate index name: uq_<tablename>_<column>
		indexName := fmt.Sprintf("uq_%s_%s", table.Name, col.Name)

		// Check if index already exists (shouldn't happen, but be safe)
		if e.catalog.GetIndex(indexName) != nil {
			continue
		}

		// Create B-tree for the index
		indexTree, err := btree.Create(e.pager)
		if err != nil {
			return fmt.Errorf("failed to create unique index btree for column %s: %w", col.Name, err)
		}

		// Store the index tree
		idxTreeName := "index:" + indexName
		e.trees[idxTreeName] = indexTree

		// Create index definition
		idx := &schema.IndexDef{
			Name:      indexName,
			TableName: table.Name,
			Columns:   []string{col.Name},
			Type:      schema.IndexTypeBTree,
			Unique:    true,
			RootPage:  indexTree.RootPage(),
		}

		// Add to catalog
		if err := e.catalog.CreateIndex(idx); err != nil {
			return err
		}
	}

	// Process table-level UNIQUE constraints
	for _, tc := range table.TableConstraints {
		if tc.Type != schema.ConstraintUnique {
			continue
		}

		// Generate index name: uq_<tablename> or uq_<tablename>_<col1>_<col2>...
		var indexName string
		if tc.Name != "" {
			indexName = tc.Name
		} else if len(tc.Columns) == 1 {
			indexName = fmt.Sprintf("uq_%s_%s", table.Name, tc.Columns[0])
		} else {
			indexName = fmt.Sprintf("uq_%s_%s", table.Name, strings.Join(tc.Columns, "_"))
		}

		// Check if index already exists
		if e.catalog.GetIndex(indexName) != nil {
			continue
		}

		// Create B-tree for the index
		indexTree, err := btree.Create(e.pager)
		if err != nil {
			return fmt.Errorf("failed to create unique index btree: %w", err)
		}

		// Store the index tree
		idxTreeName := "index:" + indexName
		e.trees[idxTreeName] = indexTree

		// Create index definition
		idx := &schema.IndexDef{
			Name:      indexName,
			TableName: table.Name,
			Columns:   tc.Columns,
			Type:      schema.IndexTypeBTree,
			Unique:    true,
			RootPage:  indexTree.RootPage(),
		}

		// Add to catalog
		if err := e.catalog.CreateIndex(idx); err != nil {
			return err
		}
	}

	return nil
}
