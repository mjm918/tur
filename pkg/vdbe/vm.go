// pkg/vdbe/vm.go
package vdbe

import (
	"fmt"

	"tur/pkg/btree"
	"tur/pkg/pager"
	"tur/pkg/record"
	"tur/pkg/types"
)

// VDBECursor represents an open cursor on a B-tree
type VDBECursor struct {
	btree  *btree.BTree
	cursor *btree.Cursor
	isOpen bool
}

// VM is the Virtual Database Engine - a bytecode interpreter for SQL
type VM struct {
	program   *Program
	pager     *pager.Pager
	pc        int             // Program counter
	registers []types.Value   // Register file
	cursors   []*VDBECursor   // Open cursors
	results   [][]types.Value // Result rows
	halted    bool
}

// NewVM creates a new VM with the given program
func NewVM(program *Program, p *pager.Pager) *VM {
	return &VM{
		program:   program,
		pager:     p,
		pc:        0,
		registers: make([]types.Value, 16), // Default 16 registers
		cursors:   make([]*VDBECursor, 8),  // Default 8 cursors
		results:   make([][]types.Value, 0),
		halted:    false,
	}
}

// SetNumRegisters sets the number of registers
func (vm *VM) SetNumRegisters(n int) {
	newRegs := make([]types.Value, n)
	copy(newRegs, vm.registers)
	vm.registers = newRegs
}

// NumRegisters returns the number of registers
func (vm *VM) NumRegisters() int {
	return len(vm.registers)
}

// PC returns the current program counter
func (vm *VM) PC() int {
	return vm.pc
}

// Register returns the value in the given register
func (vm *VM) Register(i int) types.Value {
	if i < 0 || i >= len(vm.registers) {
		return types.NewNull()
	}
	return vm.registers[i]
}

// SetRegister sets a value in the given register
func (vm *VM) SetRegister(i int, val types.Value) {
	if i >= 0 && i < len(vm.registers) {
		vm.registers[i] = val
	}
}

// Results returns the result rows collected during execution
func (vm *VM) Results() [][]types.Value {
	return vm.results
}

// Run executes the program until halt
func (vm *VM) Run() error {
	vm.halted = false
	maxSteps := 1000000 // Safety limit

	for steps := 0; !vm.halted && steps < maxSteps; steps++ {
		if vm.pc < 0 || vm.pc >= vm.program.Len() {
			return fmt.Errorf("program counter out of bounds: %d", vm.pc)
		}

		instr := vm.program.Get(vm.pc)
		if instr == nil {
			return fmt.Errorf("nil instruction at pc=%d", vm.pc)
		}

		if err := vm.step(instr); err != nil {
			return err
		}
	}

	if !vm.halted {
		return fmt.Errorf("program did not halt within %d steps", maxSteps)
	}

	return nil
}

// step executes a single instruction
func (vm *VM) step(instr *Instruction) error {
	switch instr.Op {
	case OpInit:
		// Jump to P2
		vm.pc = instr.P2
		return nil

	case OpHalt:
		vm.halted = true
		return nil

	case OpGoto:
		vm.pc = instr.P2
		return nil

	case OpInteger:
		// P1 = value, P2 = dest register
		vm.registers[instr.P2] = types.NewInt(int64(instr.P1))
		vm.pc++
		return nil

	case OpString:
		// P4 = string, P2 = dest register
		if s, ok := instr.P4.(string); ok {
			vm.registers[instr.P2] = types.NewText(s)
		} else {
			vm.registers[instr.P2] = types.NewNull()
		}
		vm.pc++
		return nil

	case OpNull:
		// P2 = dest register
		vm.registers[instr.P2] = types.NewNull()
		vm.pc++
		return nil

	case OpCopy:
		// P1 = source, P2 = dest
		vm.registers[instr.P2] = vm.registers[instr.P1]
		vm.pc++
		return nil

	case OpAdd:
		return vm.execArithmetic(instr, func(a, b int64) int64 { return a + b })

	case OpSubtract:
		return vm.execArithmetic(instr, func(a, b int64) int64 { return a - b })

	case OpMultiply:
		return vm.execArithmetic(instr, func(a, b int64) int64 { return a * b })

	case OpDivide:
		return vm.execArithmetic(instr, func(a, b int64) int64 {
			if b == 0 {
				return 0
			}
			return a / b
		})

	case OpEq:
		return vm.execComparison(instr, func(cmp int) bool { return cmp == 0 })

	case OpNe:
		return vm.execComparison(instr, func(cmp int) bool { return cmp != 0 })

	case OpLt:
		return vm.execComparison(instr, func(cmp int) bool { return cmp < 0 })

	case OpLe:
		return vm.execComparison(instr, func(cmp int) bool { return cmp <= 0 })

	case OpGt:
		return vm.execComparison(instr, func(cmp int) bool { return cmp > 0 })

	case OpGe:
		return vm.execComparison(instr, func(cmp int) bool { return cmp >= 0 })

	case OpIf:
		// Jump to P2 if r[P1] is true (non-zero, non-null)
		val := vm.registers[instr.P1]
		if vm.isTruthy(val) {
			vm.pc = instr.P2
		} else {
			vm.pc++
		}
		return nil

	case OpIfNot:
		// Jump to P2 if r[P1] is false (zero or null)
		val := vm.registers[instr.P1]
		if !vm.isTruthy(val) {
			vm.pc = instr.P2
		} else {
			vm.pc++
		}
		return nil

	case OpResultRow:
		// Output registers P1 through P1+P2-1
		row := make([]types.Value, instr.P2)
		for i := 0; i < instr.P2; i++ {
			row[i] = vm.registers[instr.P1+i]
		}
		vm.results = append(vm.results, row)
		vm.pc++
		return nil

	case OpOpenRead:
		// Open cursor P1 for reading table with root page P2
		return vm.execOpenCursor(instr, false)

	case OpOpenWrite:
		// Open cursor P1 for writing table with root page P2
		return vm.execOpenCursor(instr, true)

	case OpClose:
		// Close cursor P1
		return vm.execCloseCursor(instr)

	case OpRewind:
		// Move cursor P1 to first row, jump to P2 if empty
		return vm.execRewind(instr)

	case OpNext:
		// Advance cursor P1, jump to P2 if more rows
		return vm.execNext(instr)

	case OpColumn:
		// Read column P2 from cursor P1 into register P3
		return vm.execColumn(instr)

	case OpMakeRecord:
		// Create record from registers P1..P1+P2-1, store in P3
		return vm.execMakeRecord(instr)

	case OpInsert:
		// Insert record r[P2] at rowid r[P3] into cursor P1
		return vm.execInsert(instr)

	default:
		return fmt.Errorf("unimplemented opcode: %s", instr.Op)
	}
}

// execArithmetic executes an arithmetic operation
func (vm *VM) execArithmetic(instr *Instruction, op func(a, b int64) int64) error {
	a := vm.registers[instr.P1]
	b := vm.registers[instr.P2]

	// Handle type coercion
	var result types.Value
	if a.Type() == types.TypeFloat || b.Type() == types.TypeFloat {
		fa := vm.toFloat(a)
		fb := vm.toFloat(b)
		// Use the op on floats
		result = types.NewFloat(float64(op(int64(fa), int64(fb))))
	} else {
		result = types.NewInt(op(a.Int(), b.Int()))
	}

	vm.registers[instr.P3] = result
	vm.pc++
	return nil
}

// execComparison executes a comparison operation
func (vm *VM) execComparison(instr *Instruction, cond func(cmp int) bool) error {
	a := vm.registers[instr.P1]
	b := vm.registers[instr.P3]

	cmp := vm.compare(a, b)
	if cond(cmp) {
		vm.pc = instr.P2 // Jump
	} else {
		vm.pc++ // Fall through
	}
	return nil
}

// compare compares two values, returns -1, 0, or 1
func (vm *VM) compare(a, b types.Value) int {
	// Handle NULL
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1
	}
	if b.IsNull() {
		return 1
	}

	// Same type comparisons
	if a.Type() == b.Type() {
		switch a.Type() {
		case types.TypeInt:
			ai, bi := a.Int(), b.Int()
			if ai < bi {
				return -1
			}
			if ai > bi {
				return 1
			}
			return 0
		case types.TypeFloat:
			af, bf := a.Float(), b.Float()
			if af < bf {
				return -1
			}
			if af > bf {
				return 1
			}
			return 0
		case types.TypeText:
			at, bt := a.Text(), b.Text()
			if at < bt {
				return -1
			}
			if at > bt {
				return 1
			}
			return 0
		}
	}

	// Mixed numeric types
	if (a.Type() == types.TypeInt || a.Type() == types.TypeFloat) &&
		(b.Type() == types.TypeInt || b.Type() == types.TypeFloat) {
		af := vm.toFloat(a)
		bf := vm.toFloat(b)
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	// Default: compare by type order
	if a.Type() < b.Type() {
		return -1
	}
	return 1
}

// toFloat converts a value to float64
func (vm *VM) toFloat(v types.Value) float64 {
	switch v.Type() {
	case types.TypeInt:
		return float64(v.Int())
	case types.TypeFloat:
		return v.Float()
	default:
		return 0
	}
}

// isTruthy returns true if the value is truthy
func (vm *VM) isTruthy(v types.Value) bool {
	if v.IsNull() {
		return false
	}
	switch v.Type() {
	case types.TypeInt:
		return v.Int() != 0
	case types.TypeFloat:
		return v.Float() != 0
	case types.TypeText:
		return v.Text() != ""
	default:
		return false
	}
}

// Cursor operation helpers

// execOpenCursor opens a cursor on a B-tree
func (vm *VM) execOpenCursor(instr *Instruction, forWrite bool) error {
	cursorIdx := instr.P1
	rootPage := uint32(instr.P2)

	// Ensure we have enough cursors
	for len(vm.cursors) <= cursorIdx {
		vm.cursors = append(vm.cursors, nil)
	}

	// Close existing cursor if any
	if vm.cursors[cursorIdx] != nil && vm.cursors[cursorIdx].isOpen {
		if vm.cursors[cursorIdx].cursor != nil {
			vm.cursors[cursorIdx].cursor.Close()
		}
	}

	// Open B-tree
	bt := btree.Open(vm.pager, rootPage)
	cursor := bt.Cursor()

	vm.cursors[cursorIdx] = &VDBECursor{
		btree:  bt,
		cursor: cursor,
		isOpen: true,
	}

	vm.pc++
	return nil
}

// execCloseCursor closes a cursor
func (vm *VM) execCloseCursor(instr *Instruction) error {
	cursorIdx := instr.P1

	if cursorIdx < len(vm.cursors) && vm.cursors[cursorIdx] != nil {
		if vm.cursors[cursorIdx].cursor != nil {
			vm.cursors[cursorIdx].cursor.Close()
		}
		vm.cursors[cursorIdx].isOpen = false
	}

	vm.pc++
	return nil
}

// execRewind moves cursor to first row
func (vm *VM) execRewind(instr *Instruction) error {
	cursorIdx := instr.P1
	jumpAddr := instr.P2

	if cursorIdx >= len(vm.cursors) || vm.cursors[cursorIdx] == nil {
		return fmt.Errorf("cursor %d not open", cursorIdx)
	}

	cursor := vm.cursors[cursorIdx].cursor
	cursor.First()

	if !cursor.Valid() {
		// Table is empty, jump to P2
		vm.pc = jumpAddr
	} else {
		vm.pc++
	}
	return nil
}

// execNext advances cursor to next row
func (vm *VM) execNext(instr *Instruction) error {
	cursorIdx := instr.P1
	jumpAddr := instr.P2

	if cursorIdx >= len(vm.cursors) || vm.cursors[cursorIdx] == nil {
		return fmt.Errorf("cursor %d not open", cursorIdx)
	}

	cursor := vm.cursors[cursorIdx].cursor
	cursor.Next()

	if cursor.Valid() {
		// More rows, jump to P2
		vm.pc = jumpAddr
	} else {
		vm.pc++
	}
	return nil
}

// execColumn reads a column from cursor
func (vm *VM) execColumn(instr *Instruction) error {
	cursorIdx := instr.P1
	columnIdx := instr.P2
	destReg := instr.P3

	if cursorIdx >= len(vm.cursors) || vm.cursors[cursorIdx] == nil {
		return fmt.Errorf("cursor %d not open", cursorIdx)
	}

	cursor := vm.cursors[cursorIdx].cursor
	if !cursor.Valid() {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	// Get the row data and decode it
	data := cursor.Value()
	if data == nil {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	values := record.Decode(data)
	if columnIdx < len(values) {
		vm.registers[destReg] = values[columnIdx]
	} else {
		vm.registers[destReg] = types.NewNull()
	}

	vm.pc++
	return nil
}

// execMakeRecord creates a record from registers
func (vm *VM) execMakeRecord(instr *Instruction) error {
	startReg := instr.P1
	numRegs := instr.P2
	destReg := instr.P3

	values := make([]types.Value, numRegs)
	for i := 0; i < numRegs; i++ {
		values[i] = vm.registers[startReg+i]
	}

	data := record.Encode(values)
	vm.registers[destReg] = types.NewBlob(data)

	vm.pc++
	return nil
}

// execInsert inserts a record into the B-tree
func (vm *VM) execInsert(instr *Instruction) error {
	cursorIdx := instr.P1
	recordReg := instr.P2
	rowidReg := instr.P3

	if cursorIdx >= len(vm.cursors) || vm.cursors[cursorIdx] == nil {
		return fmt.Errorf("cursor %d not open", cursorIdx)
	}

	// Get record data
	recordVal := vm.registers[recordReg]
	if recordVal.Type() != types.TypeBlob {
		return fmt.Errorf("expected blob for record, got %v", recordVal.Type())
	}
	data := recordVal.Blob()

	// Get rowid
	rowidVal := vm.registers[rowidReg]
	rowid := rowidVal.Int()

	// Create key from rowid (big-endian for sorting)
	key := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		key[i] = byte(rowid)
		rowid >>= 8
	}

	// Insert into B-tree
	bt := vm.cursors[cursorIdx].btree
	if err := bt.Insert(key, data); err != nil {
		return fmt.Errorf("insert failed: %w", err)
	}

	vm.pc++
	return nil
}
