// pkg/sql/executor/executor.go
package executor

import (
	"encoding/binary"
	"fmt"

	"tur/pkg/btree"
	"tur/pkg/pager"
	"tur/pkg/record"
	"tur/pkg/schema"
	"tur/pkg/sql/lexer"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

// Result holds the result of executing a SQL statement
type Result struct {
	Columns      []string
	Rows         [][]types.Value
	RowsAffected int64
}

// Executor executes SQL statements
type Executor struct {
	pager   *pager.Pager
	catalog *schema.Catalog
	trees   map[string]*btree.BTree // table name -> btree
	rowid   map[string]uint64       // table name -> next rowid
}

// New creates a new Executor
func New(p *pager.Pager) *Executor {
	return &Executor{
		pager:   p,
		catalog: schema.NewCatalog(),
		trees:   make(map[string]*btree.BTree),
		rowid:   make(map[string]uint64),
	}
}

// Close closes the executor and syncs data
func (e *Executor) Close() error {
	return e.pager.Close()
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
			Name:       col.Name,
			Type:       col.Type,
			PrimaryKey: col.PrimaryKey,
			NotNull:    col.NotNull,
			VectorDim:  col.VectorDim,
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
			constraints = append(constraints, schema.Constraint{
				Type:      schema.ConstraintForeignKey,
				RefTable:  col.ForeignKey.RefTable,
				RefColumn: col.ForeignKey.RefColumn,
				OnDelete:  convertFKAction(col.ForeignKey.OnDelete),
				OnUpdate:  convertFKAction(col.ForeignKey.OnUpdate),
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

	return &Result{}, nil
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
		return fmt.Sprintf("%s %s %s", left, op, right)
	case *parser.UnaryExpr:
		right := exprToString(e.Right)
		op := tokenToOp(e.Op)
		return fmt.Sprintf("%s%s", op, right)
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

// executeDropTable handles DROP TABLE
func (e *Executor) executeDropTable(stmt *parser.DropTableStmt) (*Result, error) {
	if err := e.catalog.DropTable(stmt.TableName); err != nil {
		return nil, err
	}

	delete(e.trees, stmt.TableName)
	delete(e.rowid, stmt.TableName)

	return &Result{}, nil
}

// executeCreateIndex handles CREATE INDEX
func (e *Executor) executeCreateIndex(stmt *parser.CreateIndexStmt) (*Result, error) {
	// Check if table exists
	table := e.catalog.GetTable(stmt.TableName)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", stmt.TableName)
	}

	// Validate all columns exist in the table
	for _, colName := range stmt.Columns {
		col, _ := table.GetColumn(colName)
		if col == nil {
			return nil, fmt.Errorf("column %s not found in table %s", colName, stmt.TableName)
		}
	}

	// Create B-tree for the index
	tree, err := btree.Create(e.pager)
	if err != nil {
		return nil, fmt.Errorf("failed to create index btree: %w", err)
	}

	// Create index definition
	idx := &schema.IndexDef{
		Name:      stmt.IndexName,
		TableName: stmt.TableName,
		Columns:   stmt.Columns,
		Type:      schema.IndexTypeBTree,
		Unique:    stmt.Unique,
		RootPage:  tree.RootPage(),
	}

	// Add to catalog
	if err := e.catalog.CreateIndex(idx); err != nil {
		return nil, err
	}

	return &Result{}, nil
}

// executeDropIndex handles DROP INDEX
func (e *Executor) executeDropIndex(stmt *parser.DropIndexStmt) (*Result, error) {
	if err := e.catalog.DropIndex(stmt.IndexName); err != nil {
		return nil, err
	}

	return &Result{}, nil
}

// executeInsert handles INSERT
func (e *Executor) executeInsert(stmt *parser.InsertStmt) (*Result, error) {
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

	var rowsAffected int64

	// Insert each row
	for _, row := range stmt.Values {
		// Evaluate expressions to get values
		values := make([]types.Value, len(row))
		for i, expr := range row {
			val, err := e.evaluateExpr(expr, nil, nil)
			if err != nil {
				return nil, err
			}
			values[i] = val
		}

		// Validate constraints
		if err := e.validateConstraints(table, values); err != nil {
			return nil, err
		}

		// Validate types (basic check for Vector)
		// We iterate values assuming they align with table columns (current existing assumption in executor)
		for idx, val := range values {
			colDef := table.Columns[idx]
			if colDef.Type == types.TypeVector && !val.IsNull() {
				if val.Type() != types.TypeBlob {
					return nil, fmt.Errorf("column %s expects VECTOR (blob), got %v", colDef.Name, val.Type())
				}
				blob := val.Blob()
				expectedSize := 4 + colDef.VectorDim*4
				if len(blob) != expectedSize {
					return nil, fmt.Errorf("column %s expects VECTOR(%d) with size %d, got %d bytes", colDef.Name, colDef.VectorDim, expectedSize, len(blob))
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

		rowsAffected++
	}

	return &Result{RowsAffected: rowsAffected}, nil
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

// executeSelect handles SELECT
func (e *Executor) executeSelect(stmt *parser.SelectStmt) (*Result, error) {
	// Get table
	table := e.catalog.GetTable(stmt.From)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", stmt.From)
	}

	// Get or create B-tree
	tree := e.trees[stmt.From]
	if tree == nil {
		tree = btree.Open(e.pager, table.RootPage)
		e.trees[stmt.From] = tree
	}

	// Determine which columns to return
	var resultColumns []string
	var columnIndices []int

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		// SELECT *
		for _, col := range table.Columns {
			resultColumns = append(resultColumns, col.Name)
		}
		for i := range table.Columns {
			columnIndices = append(columnIndices, i)
		}
	} else {
		// Specific columns
		for _, col := range stmt.Columns {
			_, idx := table.GetColumn(col.Name)
			if idx == -1 {
				return nil, fmt.Errorf("column %s not found", col.Name)
			}
			resultColumns = append(resultColumns, col.Name)
			columnIndices = append(columnIndices, idx)
		}
	}

	// Build column name to index map for WHERE evaluation
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[col.Name] = i
	}

	// Scan table
	var rows [][]types.Value
	cursor := tree.Cursor()
	defer cursor.Close()

	for cursor.First(); cursor.Valid(); cursor.Next() {
		value := cursor.Value()
		if value == nil {
			continue
		}

		// Decode record
		rowValues := record.Decode(value)

		// Evaluate WHERE clause if present
		if stmt.Where != nil {
			match, err := e.evaluateCondition(stmt.Where, rowValues, colMap)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		// Extract requested columns
		row := make([]types.Value, len(columnIndices))
		for i, idx := range columnIndices {
			if idx < len(rowValues) {
				row[i] = rowValues[idx]
			} else {
				row[i] = types.NewNull()
			}
		}
		rows = append(rows, row)
	}

	return &Result{
		Columns: resultColumns,
		Rows:    rows,
	}, nil
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
	default:
		val, err := e.evaluateExpr(expr, rowValues, colMap)
		if err != nil {
			return false, err
		}
		return !val.IsNull() && val.Int() != 0, nil
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
