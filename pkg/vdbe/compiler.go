// pkg/vdbe/compiler.go
package vdbe

import (
	"fmt"

	"tur/pkg/pager"
	"tur/pkg/schema"
	"tur/pkg/sql/lexer"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

// Compiler compiles SQL AST into VDBE bytecode
type Compiler struct {
	catalog    *schema.Catalog
	pager      *pager.Pager
	program    *Program
	nextReg    int // Next available register
	nextCursor int // Next available cursor
}

// NewCompiler creates a new Compiler
func NewCompiler(catalog *schema.Catalog, p *pager.Pager) *Compiler {
	return &Compiler{
		catalog:    catalog,
		pager:      p,
		program:    NewProgram(),
		nextReg:    1, // Reserve register 0
		nextCursor: 0,
	}
}

// NumRegisters returns the number of registers needed
func (c *Compiler) NumRegisters() int {
	return c.nextReg + 10 // Add some buffer
}

// allocReg allocates a register
func (c *Compiler) allocReg() int {
	r := c.nextReg
	c.nextReg++
	return r
}

// allocCursor allocates a cursor
func (c *Compiler) allocCursor() int {
	cursor := c.nextCursor
	c.nextCursor++
	return cursor
}

// Compile compiles a statement into a program
func (c *Compiler) Compile(stmt parser.Statement) (*Program, error) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		return c.compileSelect(s)
	case *parser.InsertStmt:
		return c.compileInsert(s)
	case *parser.UpdateStmt:
		return c.compileUpdate(s)
	case *parser.DeleteStmt:
		return c.compileDelete(s)
	case *parser.CreateTableStmt:
		return c.compileCreateTable(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// compileSelect compiles a SELECT statement
func (c *Compiler) compileSelect(stmt *parser.SelectStmt) (*Program, error) {
	var tableName string
	switch t := stmt.From.(type) {
	case *parser.Table:
		tableName = t.Name
	default:
		return nil, fmt.Errorf("complex table references not supported in compiler")
	}

	// Get table
	table := c.catalog.GetTable(tableName)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// Build column map
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[col.Name] = i
	}

	// Determine output columns
	var outputCols []int
	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		// SELECT *
		for i := range table.Columns {
			outputCols = append(outputCols, i)
		}
	} else {
		for _, col := range stmt.Columns {
			colRef, ok := col.Expr.(*parser.ColumnRef)
			if !ok {
				return nil, fmt.Errorf("complex expressions not supported in simple compiler")
			}
			idx, ok := colMap[colRef.Name]
			if !ok {
				return nil, fmt.Errorf("column %s not found", colRef.Name)
			}
			outputCols = append(outputCols, idx)
		}
	}

	// Allocate registers for output columns
	outputRegs := make([]int, len(outputCols))
	for i := range outputCols {
		outputRegs[i] = c.allocReg()
	}

	// Allocate cursor
	cursorIdx := c.allocCursor()

	// Generate code:
	// Init -> start of loop
	// OpenRead cursor, rootPage
	// Rewind cursor, end
	// loop: Column for each output column
	// [WHERE evaluation if present]
	// ResultRow
	// next: Next cursor, loop
	// end: Close cursor
	// Halt

	// Init jumps to actual start
	addrInit := c.program.AddOp(OpInit, 0, 0, 0) // Will fix P2

	// Open cursor
	c.program.AddOp(OpOpenRead, cursorIdx, int(table.RootPage), 0)

	// Rewind - jump to end if empty
	addrRewind := c.program.AddOp(OpRewind, cursorIdx, 0, 0) // Will fix P2

	// Loop start
	addrLoopStart := c.program.Len()

	// Read columns into registers
	for i, colIdx := range outputCols {
		c.program.AddOp(OpColumn, cursorIdx, colIdx, outputRegs[i])
	}

	// WHERE clause evaluation if present
	var addrSkipRow int
	if stmt.Where != nil {
		// Compile WHERE expression into a register
		condReg := c.allocReg()
		if err := c.compileExpr(stmt.Where, colMap, cursorIdx, condReg); err != nil {
			return nil, err
		}
		// If condition is false, skip to next
		addrSkipRow = c.program.AddOp(OpIfNot, condReg, 0, 0) // Will fix P2
	}

	// ResultRow
	c.program.AddOp(OpResultRow, outputRegs[0], len(outputCols), 0)

	// Fix skip row jump if there was a WHERE
	if stmt.Where != nil {
		addrNext := c.program.Len()
		c.program.ChangeP2(addrSkipRow, addrNext)
	}

	// Next - jump back to loop if more rows
	c.program.AddOp(OpNext, cursorIdx, addrLoopStart, 0)

	// End
	addrEnd := c.program.Len()
	c.program.AddOp(OpClose, cursorIdx, 0, 0)
	c.program.AddOp(OpHalt, 0, 0, 0)

	// Fix jump targets
	c.program.ChangeP2(addrInit, 1) // Jump past Init to OpenRead
	c.program.ChangeP2(addrRewind, addrEnd)

	return c.program, nil
}

// compileExpr compiles an expression into a register
func (c *Compiler) compileExpr(expr parser.Expression, colMap map[string]int, cursorIdx, destReg int) error {
	switch e := expr.(type) {
	case *parser.Literal:
		return c.compileLiteral(e.Value, destReg)

	case *parser.ColumnRef:
		colIdx, ok := colMap[e.Name]
		if !ok {
			return fmt.Errorf("column %s not found", e.Name)
		}
		c.program.AddOp(OpColumn, cursorIdx, colIdx, destReg)
		return nil

	case *parser.BinaryExpr:
		return c.compileBinaryExpr(e, colMap, cursorIdx, destReg)

	default:
		return fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// compileLiteral compiles a literal value
func (c *Compiler) compileLiteral(val types.Value, destReg int) error {
	switch val.Type() {
	case types.TypeInt, types.TypeInt32, types.TypeSmallInt, types.TypeBigInt, types.TypeSerial, types.TypeBigSerial:
		c.program.AddOp(OpInteger, int(val.Int()), destReg, 0)
	case types.TypeText:
		c.program.AddOp4(OpString, len(val.Text()), destReg, 0, val.Text())
	case types.TypeNull:
		c.program.AddOp(OpNull, 0, destReg, 0)
	case types.TypeFloat:
		// For now, store as integer (simplified)
		c.program.AddOp(OpInteger, int(val.Float()), destReg, 0)
	default:
		return fmt.Errorf("unsupported literal type: %v", val.Type())
	}
	return nil
}

// compileBinaryExpr compiles a binary expression
func (c *Compiler) compileBinaryExpr(expr *parser.BinaryExpr, colMap map[string]int, cursorIdx, destReg int) error {
	leftReg := c.allocReg()
	rightReg := c.allocReg()

	if err := c.compileExpr(expr.Left, colMap, cursorIdx, leftReg); err != nil {
		return err
	}
	if err := c.compileExpr(expr.Right, colMap, cursorIdx, rightReg); err != nil {
		return err
	}

	switch expr.Op {
	case lexer.PLUS:
		c.program.AddOp(OpAdd, leftReg, rightReg, destReg)
	case lexer.MINUS:
		c.program.AddOp(OpSubtract, leftReg, rightReg, destReg)
	case lexer.STAR:
		c.program.AddOp(OpMultiply, leftReg, rightReg, destReg)
	case lexer.SLASH:
		c.program.AddOp(OpDivide, leftReg, rightReg, destReg)
	case lexer.EQ, lexer.NEQ, lexer.LT, lexer.GT, lexer.LTE, lexer.GTE:
		return c.compileComparison(expr.Op, leftReg, rightReg, destReg)
	case lexer.AND:
		return c.compileAnd(expr, colMap, cursorIdx, destReg)
	case lexer.OR:
		return c.compileOr(expr, colMap, cursorIdx, destReg)
	default:
		return fmt.Errorf("unsupported binary operator: %v", expr.Op)
	}
	return nil
}

// compileComparison compiles a comparison into a 0/1 result
func (c *Compiler) compileComparison(op lexer.TokenType, leftReg, rightReg, destReg int) error {
	// Generate: if comparison true, set 1, else set 0
	// We use jump-based logic:
	// Integer 0 -> destReg
	// [comparison] leftReg, skip, rightReg
	// Goto end
	// skip: Integer 1 -> destReg
	// end: ...

	c.program.AddOp(OpInteger, 0, destReg, 0) // Default to false

	var opcode Opcode
	switch op {
	case lexer.EQ:
		opcode = OpEq
	case lexer.NEQ:
		opcode = OpNe
	case lexer.LT:
		opcode = OpLt
	case lexer.GT:
		opcode = OpGt
	case lexer.LTE:
		opcode = OpLe
	case lexer.GTE:
		opcode = OpGe
	}

	addrCmp := c.program.AddOp(opcode, leftReg, 0, rightReg) // Jump if true
	addrEnd := c.program.AddOp(OpGoto, 0, 0, 0)              // Skip to end if false
	addrTrue := c.program.Len()
	c.program.AddOp(OpInteger, 1, destReg, 0) // Set to true

	// Fix jumps
	c.program.ChangeP2(addrCmp, addrTrue)
	c.program.ChangeP2(addrEnd, c.program.Len())

	return nil
}

// compileAnd compiles AND expression with short-circuit evaluation
func (c *Compiler) compileAnd(expr *parser.BinaryExpr, colMap map[string]int, cursorIdx, destReg int) error {
	// Evaluate left
	leftReg := c.allocReg()
	if err := c.compileExpr(expr.Left, colMap, cursorIdx, leftReg); err != nil {
		return err
	}

	// If left is false, result is false
	c.program.AddOp(OpInteger, 0, destReg, 0)
	addrIfFalse := c.program.AddOp(OpIfNot, leftReg, 0, 0)

	// Evaluate right
	rightReg := c.allocReg()
	if err := c.compileExpr(expr.Right, colMap, cursorIdx, rightReg); err != nil {
		return err
	}

	// Result is right's truth value
	c.program.AddOp(OpCopy, rightReg, destReg, 0)

	// Fix jump
	c.program.ChangeP2(addrIfFalse, c.program.Len())

	return nil
}

// compileOr compiles OR expression with short-circuit evaluation
func (c *Compiler) compileOr(expr *parser.BinaryExpr, colMap map[string]int, cursorIdx, destReg int) error {
	// Evaluate left
	leftReg := c.allocReg()
	if err := c.compileExpr(expr.Left, colMap, cursorIdx, leftReg); err != nil {
		return err
	}

	// If left is true, result is true
	c.program.AddOp(OpInteger, 1, destReg, 0)
	addrIfTrue := c.program.AddOp(OpIf, leftReg, 0, 0)

	// Evaluate right
	rightReg := c.allocReg()
	if err := c.compileExpr(expr.Right, colMap, cursorIdx, rightReg); err != nil {
		return err
	}

	// Result is right's truth value
	c.program.AddOp(OpCopy, rightReg, destReg, 0)

	// Fix jump
	c.program.ChangeP2(addrIfTrue, c.program.Len())

	return nil
}

// compileInsert compiles an INSERT statement
func (c *Compiler) compileInsert(stmt *parser.InsertStmt) (*Program, error) {
	// Get destination table
	table := c.catalog.GetTable(stmt.TableName)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", stmt.TableName)
	}

	// Build column map from INSERT column list
	colOrder := make([]int, len(stmt.Columns))
	for i, colName := range stmt.Columns {
		_, idx := table.GetColumn(colName)
		if idx == -1 {
			return nil, fmt.Errorf("column %s not found", colName)
		}
		colOrder[i] = idx
	}

	// Check if this is INSERT...SELECT or INSERT...VALUES
	if stmt.SelectStmt != nil {
		return c.compileInsertSelect(stmt, table, colOrder)
	}

	return c.compileInsertValues(stmt, table, colOrder)
}

// compileInsertValues compiles INSERT...VALUES
func (c *Compiler) compileInsertValues(stmt *parser.InsertStmt, table *schema.TableDef, colOrder []int) (*Program, error) {
	// Allocate cursor for writing
	cursorIdx := c.allocCursor()

	// Init
	addrInit := c.program.AddOp(OpInit, 0, 0, 0)

	// Open for write
	c.program.AddOp(OpOpenWrite, cursorIdx, int(table.RootPage), 0)

	// For each row of values
	for rowIdx, row := range stmt.Values {
		// Allocate registers for values in table column order
		valueRegs := make([]int, len(table.Columns))
		for i := range table.Columns {
			valueRegs[i] = c.allocReg()
			// Default to NULL
			c.program.AddOp(OpNull, 0, valueRegs[i], 0)
		}

		// Fill in provided values
		for i, expr := range row {
			colIdx := colOrder[i]
			if err := c.compileExprValue(expr, valueRegs[colIdx]); err != nil {
				return nil, err
			}
		}

		// Generate rowid (use first value if it's the primary key, else use row index)
		rowidReg := c.allocReg()
		_, pkIdx := table.PrimaryKeyColumn()
		if pkIdx >= 0 && pkIdx < len(valueRegs) {
			c.program.AddOp(OpCopy, valueRegs[pkIdx], rowidReg, 0)
		} else {
			c.program.AddOp(OpInteger, rowIdx+1, rowidReg, 0)
		}

		// Make record
		recordReg := c.allocReg()
		c.program.AddOp(OpMakeRecord, valueRegs[0], len(table.Columns), recordReg)

		// Insert
		c.program.AddOp(OpInsert, cursorIdx, recordReg, rowidReg)
	}

	// Close cursor
	c.program.AddOp(OpClose, cursorIdx, 0, 0)

	// Halt
	c.program.AddOp(OpHalt, 0, 0, 0)

	// Fix init jump
	c.program.ChangeP2(addrInit, 1)

	return c.program, nil
}

// compileInsertSelect compiles INSERT...SELECT
// INSERT INTO table (cols) SELECT ... FROM source [WHERE ...]
//
// Generated bytecode:
//
//	Init -> start
//	OpenWrite destCursor, destRootPage    ; Open destination for writing
//	OpenRead srcCursor, srcRootPage       ; Open source for reading
//	Rewind srcCursor, end                 ; Jump if empty
//
// loop:
//
//	[Column loads for SELECT columns]
//	[WHERE evaluation if present]
//	IfNot condReg, next                   ; Skip if WHERE is false
//	[Map source columns to dest columns]
//	Rowid srcCursor, rowidReg             ; Use source rowid (or generate new)
//	MakeRecord startReg, numCols, recordReg
//	Insert destCursor, recordReg, rowidReg
//
// next:
//
//	Next srcCursor, loop
//
// end:
//
//	Close srcCursor
//	Close destCursor
//	Halt
func (c *Compiler) compileInsertSelect(stmt *parser.InsertStmt, destTable *schema.TableDef, colOrder []int) (*Program, error) {
	selectStmt := stmt.SelectStmt

	// Get source table
	var srcTableName string
	switch t := selectStmt.From.(type) {
	case *parser.Table:
		srcTableName = t.Name
	default:
		return nil, fmt.Errorf("complex table references not supported in INSERT...SELECT")
	}

	srcTable := c.catalog.GetTable(srcTableName)
	if srcTable == nil {
		return nil, fmt.Errorf("source table %s not found", srcTableName)
	}

	// Build source column map
	srcColMap := make(map[string]int)
	for i, col := range srcTable.Columns {
		srcColMap[col.Name] = i
	}

	// Determine which source columns to select
	var srcOutputCols []int
	if len(selectStmt.Columns) == 1 && selectStmt.Columns[0].Star {
		// SELECT *
		for i := range srcTable.Columns {
			srcOutputCols = append(srcOutputCols, i)
		}
	} else {
		for _, col := range selectStmt.Columns {
			colRef, ok := col.Expr.(*parser.ColumnRef)
			if !ok {
				return nil, fmt.Errorf("complex expressions not supported in simple compiler")
			}
			idx, ok := srcColMap[colRef.Name]
			if !ok {
				return nil, fmt.Errorf("column %s not found in source table", colRef.Name)
			}
			srcOutputCols = append(srcOutputCols, idx)
		}
	}

	// Verify column count matches
	if len(srcOutputCols) != len(colOrder) {
		return nil, fmt.Errorf("column count mismatch: INSERT has %d columns, SELECT returns %d",
			len(colOrder), len(srcOutputCols))
	}

	// Allocate cursors
	destCursor := c.allocCursor()
	srcCursor := c.allocCursor()

	// Allocate registers for destination values
	destRegs := make([]int, len(destTable.Columns))
	for i := range destTable.Columns {
		destRegs[i] = c.allocReg()
	}

	// Allocate registers for rowid and record
	rowidReg := c.allocReg()
	recordReg := c.allocReg()

	// Init
	addrInit := c.program.AddOp(OpInit, 0, 0, 0)

	// Open destination for write
	c.program.AddOp(OpOpenWrite, destCursor, int(destTable.RootPage), 0)

	// Open source for read
	c.program.AddOp(OpOpenRead, srcCursor, int(srcTable.RootPage), 0)

	// Rewind source - jump to end if empty
	addrRewind := c.program.AddOp(OpRewind, srcCursor, 0, 0)

	// Loop start
	addrLoopStart := c.program.Len()

	// Initialize destination registers to NULL
	for i := range destTable.Columns {
		c.program.AddOp(OpNull, 0, destRegs[i], 0)
	}

	// Read source columns and map to destination registers
	for i, srcColIdx := range srcOutputCols {
		destColIdx := colOrder[i]
		c.program.AddOp(OpColumn, srcCursor, srcColIdx, destRegs[destColIdx])
	}

	// WHERE clause evaluation if present
	var addrSkipInsert int
	if selectStmt.Where != nil {
		condReg := c.allocReg()
		if err := c.compileExpr(selectStmt.Where, srcColMap, srcCursor, condReg); err != nil {
			return nil, err
		}
		addrSkipInsert = c.program.AddOp(OpIfNot, condReg, 0, 0)
	}

	// Get rowid from source (use primary key column if available)
	_, destPKIdx := destTable.PrimaryKeyColumn()
	if destPKIdx >= 0 && destPKIdx < len(destRegs) {
		// Use the primary key column value as rowid
		c.program.AddOp(OpCopy, destRegs[destPKIdx], rowidReg, 0)
	} else {
		// Use source rowid
		c.program.AddOp(OpRowid, srcCursor, rowidReg, 0)
	}

	// Make record from destination registers
	c.program.AddOp(OpMakeRecord, destRegs[0], len(destTable.Columns), recordReg)

	// Insert into destination
	c.program.AddOp(OpInsert, destCursor, recordReg, rowidReg)

	// Fix skip insert jump if there was a WHERE clause
	if selectStmt.Where != nil {
		addrNext := c.program.Len()
		c.program.ChangeP2(addrSkipInsert, addrNext)
	}

	// Next - jump back to loop if more rows
	c.program.AddOp(OpNext, srcCursor, addrLoopStart, 0)

	// End
	addrEnd := c.program.Len()
	c.program.AddOp(OpClose, srcCursor, 0, 0)
	c.program.AddOp(OpClose, destCursor, 0, 0)
	c.program.AddOp(OpHalt, 0, 0, 0)

	// Fix jump targets
	c.program.ChangeP2(addrInit, 1) // Jump past Init
	c.program.ChangeP2(addrRewind, addrEnd)

	return c.program, nil
}

// compileExprValue compiles an expression for INSERT values (no cursor context)
func (c *Compiler) compileExprValue(expr parser.Expression, destReg int) error {
	switch e := expr.(type) {
	case *parser.Literal:
		return c.compileLiteral(e.Value, destReg)
	case *parser.UnaryExpr:
		if e.Op == lexer.MINUS {
			// Compile right side
			tempReg := c.allocReg()
			if err := c.compileExprValue(e.Right, tempReg); err != nil {
				return err
			}
			// Negate: 0 - tempReg -> destReg
			zeroReg := c.allocReg()
			c.program.AddOp(OpInteger, 0, zeroReg, 0)
			c.program.AddOp(OpSubtract, zeroReg, tempReg, destReg)
			return nil
		}
		return fmt.Errorf("unsupported unary operator: %v", e.Op)
	default:
		return fmt.Errorf("unsupported expression type in INSERT: %T", expr)
	}
}

// compileCreateTable compiles a CREATE TABLE statement
func (c *Compiler) compileCreateTable(stmt *parser.CreateTableStmt) (*Program, error) {
	// CREATE TABLE is handled at a higher level (executor)
	// We just generate Init + Halt
	c.program.AddOp(OpInit, 0, 1, 0)
	c.program.AddOp(OpHalt, 0, 0, 0)
	return c.program, nil
}

// compileUpdate compiles an UPDATE statement
// UPDATE table SET col1 = val1, col2 = val2 [WHERE expr]
//
// Generated bytecode:
//
//	Init -> start
//	OpenWrite cursor, rootPage
//	Rewind cursor, end           ; Jump if empty
//
// loop:
//
//	[Load all columns into registers]
//	[WHERE evaluation if present]
//	IfNot condReg, next          ; Skip if WHERE is false
//	[Apply SET assignments - update column registers]
//	Rowid cursor, rowidReg       ; Get current rowid
//	MakeRecord startReg, numCols, recordReg
//	Insert cursor, recordReg, rowidReg  ; Replace row
//
// next:
//
//	Next cursor, loop            ; Continue loop
//
// end:
//
//	Close cursor
//	Halt
func (c *Compiler) compileUpdate(stmt *parser.UpdateStmt) (*Program, error) {
	// Get table
	table := c.catalog.GetTable(stmt.TableName)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", stmt.TableName)
	}

	// Build column map
	colMap := make(map[string]int)
	for i, col := range table.Columns {
		colMap[col.Name] = i
	}

	// Validate assignments
	for _, assign := range stmt.Assignments {
		if _, ok := colMap[assign.Column]; !ok {
			return nil, fmt.Errorf("column %s not found", assign.Column)
		}
	}

	// Allocate cursor for writing
	cursorIdx := c.allocCursor()

	// Allocate registers for all columns
	colRegs := make([]int, len(table.Columns))
	for i := range table.Columns {
		colRegs[i] = c.allocReg()
	}

	// Allocate register for rowid
	rowidReg := c.allocReg()

	// Allocate register for new record
	recordReg := c.allocReg()

	// Init
	addrInit := c.program.AddOp(OpInit, 0, 0, 0)

	// Open for write
	c.program.AddOp(OpOpenWrite, cursorIdx, int(table.RootPage), 0)

	// Rewind - jump to end if empty
	addrRewind := c.program.AddOp(OpRewind, cursorIdx, 0, 0)

	// Loop start
	addrLoopStart := c.program.Len()

	// Load all columns into registers
	for i := range table.Columns {
		c.program.AddOp(OpColumn, cursorIdx, i, colRegs[i])
	}

	// WHERE clause evaluation if present
	var addrSkipUpdate int
	if stmt.Where != nil {
		condReg := c.allocReg()
		if err := c.compileExpr(stmt.Where, colMap, cursorIdx, condReg); err != nil {
			return nil, err
		}
		addrSkipUpdate = c.program.AddOp(OpIfNot, condReg, 0, 0)
	}

	// Apply SET assignments - update column registers
	for _, assign := range stmt.Assignments {
		colIdx := colMap[assign.Column]
		// Compile the assignment value expression
		// Use compileExpr which can reference other columns
		if err := c.compileExpr(assign.Value, colMap, cursorIdx, colRegs[colIdx]); err != nil {
			return nil, err
		}
	}

	// Get current rowid
	c.program.AddOp(OpRowid, cursorIdx, rowidReg, 0)

	// Make record from column registers
	c.program.AddOp(OpMakeRecord, colRegs[0], len(table.Columns), recordReg)

	// Insert (replaces existing row with same rowid)
	c.program.AddOp(OpInsert, cursorIdx, recordReg, rowidReg)

	// Fix skip update jump if there was a WHERE clause
	if stmt.Where != nil {
		addrNext := c.program.Len()
		c.program.ChangeP2(addrSkipUpdate, addrNext)
	}

	// Next - jump back to loop if more rows
	c.program.AddOp(OpNext, cursorIdx, addrLoopStart, 0)

	// End
	addrEnd := c.program.Len()
	c.program.AddOp(OpClose, cursorIdx, 0, 0)
	c.program.AddOp(OpHalt, 0, 0, 0)

	// Fix jump targets
	c.program.ChangeP2(addrInit, 1) // Jump past Init to OpenWrite
	c.program.ChangeP2(addrRewind, addrEnd)

	return c.program, nil
}

// compileDelete compiles a DELETE statement
// DELETE FROM table [WHERE expr]
//
// For DELETE without WHERE (delete all rows):
//
//	Init -> start
//	OpenWrite cursor, rootPage
//
// loop:
//
//	Rewind cursor, end           ; Jump if empty
//	Delete cursor                ; Delete first row
//	Goto loop                    ; Repeat until empty
//
// end:
//
//	Close cursor
//	Halt
//
// For DELETE with WHERE:
//
//	Init -> start
//	OpenWrite cursor, rootPage
//	Rewind cursor, end           ; Jump if empty
//
// loop:
//
//	[Column loads for WHERE clause columns]
//	[WHERE evaluation]
//	IfNot condReg, next          ; Skip if WHERE is false
//	Delete cursor                ; Delete current row
//
// next:
//
//	Next cursor, loop            ; Continue loop
//
// end:
//
//	Close cursor
//	Halt
func (c *Compiler) compileDelete(stmt *parser.DeleteStmt) (*Program, error) {
	// Get table
	table := c.catalog.GetTable(stmt.TableName)
	if table == nil {
		return nil, fmt.Errorf("table %s not found", stmt.TableName)
	}

	// Allocate cursor for writing (delete requires write access)
	cursorIdx := c.allocCursor()

	// Init
	addrInit := c.program.AddOp(OpInit, 0, 0, 0)

	// Open for write
	c.program.AddOp(OpOpenWrite, cursorIdx, int(table.RootPage), 0)

	if stmt.Where == nil {
		// DELETE without WHERE: delete all rows
		// Use a loop that keeps rewinding and deleting the first row
		// until the table is empty

		// Loop start
		addrLoopStart := c.program.Len()

		// Rewind - jump to end if empty
		addrRewind := c.program.AddOp(OpRewind, cursorIdx, 0, 0)

		// Delete first row
		c.program.AddOp(OpDelete, cursorIdx, 0, 0)

		// Goto loop start
		c.program.AddOp(OpGoto, 0, addrLoopStart, 0)

		// End
		addrEnd := c.program.Len()
		c.program.AddOp(OpClose, cursorIdx, 0, 0)
		c.program.AddOp(OpHalt, 0, 0, 0)

		// Fix jump targets
		c.program.ChangeP2(addrInit, 1) // Jump past Init to OpenWrite
		c.program.ChangeP2(addrRewind, addrEnd)
	} else {
		// DELETE with WHERE: iterate and selectively delete
		// Build column map for WHERE clause
		colMap := make(map[string]int)
		for i, col := range table.Columns {
			colMap[col.Name] = i
		}

		// Rewind - jump to end if empty
		addrRewind := c.program.AddOp(OpRewind, cursorIdx, 0, 0)

		// Loop start
		addrLoopStart := c.program.Len()

		// Compile WHERE expression
		condReg := c.allocReg()
		if err := c.compileExpr(stmt.Where, colMap, cursorIdx, condReg); err != nil {
			return nil, err
		}
		// If condition is false, skip to next
		addrSkipDelete := c.program.AddOp(OpIfNot, condReg, 0, 0)

		// Delete current row
		c.program.AddOp(OpDelete, cursorIdx, 0, 0)

		// Jump address for skip
		addrNext := c.program.Len()
		c.program.ChangeP2(addrSkipDelete, addrNext)

		// Next - jump back to loop if more rows
		c.program.AddOp(OpNext, cursorIdx, addrLoopStart, 0)

		// End
		addrEnd := c.program.Len()
		c.program.AddOp(OpClose, cursorIdx, 0, 0)
		c.program.AddOp(OpHalt, 0, 0, 0)

		// Fix jump targets
		c.program.ChangeP2(addrInit, 1) // Jump past Init to OpenWrite
		c.program.ChangeP2(addrRewind, addrEnd)
	}

	return c.program, nil
}
