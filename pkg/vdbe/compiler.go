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
			idx, ok := colMap[col.Name]
			if !ok {
				return nil, fmt.Errorf("column %s not found", col.Name)
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
	case types.TypeInt:
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
	// Get table
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
