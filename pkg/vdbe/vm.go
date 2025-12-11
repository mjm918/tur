// pkg/vdbe/vm.go
package vdbe

import (
	"context"
	"fmt"

	"tur/pkg/btree"
	"tur/pkg/hnsw"
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

// VectorSearchCursor represents a cursor for iterating HNSW search results
type VectorSearchCursor struct {
	index   *hnsw.Index
	k       int                  // Number of results to return
	results []hnsw.SearchResult  // Cached search results
	pos     int                  // Current position in results
}

// NewVectorSearchCursor creates a new vector search cursor
func NewVectorSearchCursor(index *hnsw.Index, k int) *VectorSearchCursor {
	return &VectorSearchCursor{
		index:   index,
		k:       k,
		results: nil,
		pos:     0,
	}
}

// Search executes a KNN search with the given query vector
func (c *VectorSearchCursor) Search(query *types.Vector) error {
	if query == nil {
		return fmt.Errorf("nil query vector")
	}

	results, err := c.index.SearchKNN(query, c.k)
	if err != nil {
		return err
	}

	c.results = results
	c.pos = 0
	return nil
}

// Valid returns true if the cursor is positioned on a valid result
func (c *VectorSearchCursor) Valid() bool {
	return c.results != nil && c.pos < len(c.results)
}

// Current returns the current result (rowID, distance)
func (c *VectorSearchCursor) Current() (int64, float32) {
	if !c.Valid() {
		return 0, 0
	}
	r := c.results[c.pos]
	return r.RowID, r.Distance
}

// Next advances to the next result
func (c *VectorSearchCursor) Next() {
	if c.results != nil && c.pos < len(c.results) {
		c.pos++
	}
}

// Reset moves back to the first result
func (c *VectorSearchCursor) Reset() {
	c.pos = 0
}

// VM is the Virtual Database Engine - a bytecode interpreter for SQL
type VM struct {
	program    *Program
	pager      *pager.Pager
	pc         int              // Program counter
	registers  []types.Value    // Register file
	cursors    []*VDBECursor    // Open cursors
	results    [][]types.Value  // Result rows
	aggregates []AggregateFunc  // Aggregate function contexts
	halted     bool
	profiler   *Profiler        // Optional profiler for timing instrumentation
}

// NewVM creates a new VM with the given program
func NewVM(program *Program, p *pager.Pager) *VM {
	return &VM{
		program:    program,
		pager:      p,
		pc:         0,
		registers:  make([]types.Value, 16), // Default 16 registers
		cursors:    make([]*VDBECursor, 8),  // Default 8 cursors
		results:    make([][]types.Value, 0),
		aggregates: make([]AggregateFunc, 8), // Default 8 aggregate slots
		halted:     false,
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

// CreateVectorSearchCursor creates a new vector search cursor for an HNSW index
func (vm *VM) CreateVectorSearchCursor(index *hnsw.Index, k int) *VectorSearchCursor {
	return NewVectorSearchCursor(index, k)
}

// GetAggregateContext returns the aggregate function at the given index
func (vm *VM) GetAggregateContext(idx int) AggregateFunc {
	if idx < 0 || idx >= len(vm.aggregates) {
		return nil
	}
	return vm.aggregates[idx]
}

// SetProfiler sets the profiler for timing instrumentation.
// If nil, profiling is disabled.
func (vm *VM) SetProfiler(p *Profiler) {
	vm.profiler = p
}

// Profiler returns the current profiler, or nil if not set.
func (vm *VM) Profiler() *Profiler {
	return vm.profiler
}

// Run executes the program until halt
func (vm *VM) Run() error {
	return vm.RunContext(context.Background())
}

// Cleanup releases all resources held by the VM.
// This includes closing all open cursors and clearing aggregate contexts.
// Cleanup is called automatically when RunContext returns due to context cancellation.
func (vm *VM) Cleanup() {
	// Close all open cursors
	for i, cursor := range vm.cursors {
		if cursor != nil && cursor.isOpen {
			if cursor.cursor != nil {
				cursor.cursor.Close()
			}
			cursor.isOpen = false
		}
		vm.cursors[i] = nil
	}

	// Clear aggregate contexts
	for i := range vm.aggregates {
		vm.aggregates[i] = nil
	}

	// Clear results to free memory
	vm.results = nil
}

// RunContext executes the program until halt with context support.
// The context can be used for cancellation and timeout control.
// Context is checked every contextCheckInterval steps to balance
// responsiveness with performance.
// When the context is cancelled, Cleanup is called to release resources.
func (vm *VM) RunContext(ctx context.Context) error {
	vm.halted = false
	maxSteps := 1000000 // Safety limit

	// Check context every N steps to balance responsiveness with performance
	const contextCheckInterval = 100

	for steps := 0; !vm.halted && steps < maxSteps; steps++ {
		// Check context periodically (every contextCheckInterval steps)
		if steps%contextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				// Clean up resources on context cancellation
				vm.Cleanup()
				return err
			}
		}

		if vm.pc < 0 || vm.pc >= vm.program.Len() {
			return fmt.Errorf("program counter out of bounds: %d", vm.pc)
		}

		instr := vm.program.Get(vm.pc)
		if instr == nil {
			return fmt.Errorf("nil instruction at pc=%d", vm.pc)
		}

		// Profile the opcode execution if profiler is enabled
		if vm.profiler != nil {
			startTime := vm.profiler.BeforeOpcode(instr.Op)
			if err := vm.step(instr); err != nil {
				return err
			}
			vm.profiler.AfterOpcode(instr.Op, startTime)
		} else {
			if err := vm.step(instr); err != nil {
				return err
			}
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

	case OpRowid:
		// Store rowid from cursor P1 into register P2
		return vm.execRowid(instr)

	case OpSeek:
		// Seek cursor P1 to rowid in register P3, jump to P2 if not found
		return vm.execSeek(instr)

	case OpMakeRecord:
		// Create record from registers P1..P1+P2-1, store in P3
		return vm.execMakeRecord(instr)

	case OpInsert:
		// Insert record r[P2] at rowid r[P3] into cursor P1
		return vm.execInsert(instr)

	case OpDelete:
		// Delete current row in cursor P1
		return vm.execDelete(instr)

	case OpAggInit:
		// Initialize aggregate: P1=aggIdx, P4=name (string)
		return vm.execAggInit(instr)

	case OpAggStep:
		// Step aggregate: P1=aggIdx, P2=valueReg
		return vm.execAggStep(instr)

	case OpAggFinal:
		// Finalize aggregate: P1=aggIdx, P2=destReg
		return vm.execAggFinal(instr)

	case OpVectorDistance:
		// r[P3] = cosine_distance(r[P1], r[P2])
		return vm.ExecuteVectorDistance(instr)

	case OpVectorNormalize:
		// r[P2] = normalize(r[P1])
		return vm.executeVectorNormalize(instr)

	case OpVectorToBlob:
		// r[P2] = vector_to_blob(r[P1])
		return vm.executeVectorToBlob(instr)

	case OpVectorFromBlob:
		// r[P2] = vector_from_blob(r[P1])
		return vm.executeVectorFromBlob(instr)

	case OpVectorDot:
		// r[P3] = dot_product(r[P1], r[P2])
		return vm.executeVectorDot(instr)

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
		case types.TypeSmallInt, types.TypeInt32, types.TypeBigInt, types.TypeSerial, types.TypeBigSerial:
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
	if (types.IsIntegerType(a.Type()) || a.Type() == types.TypeFloat) &&
		(types.IsIntegerType(b.Type()) || b.Type() == types.TypeFloat) {
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
	case types.TypeSmallInt, types.TypeInt32, types.TypeBigInt, types.TypeSerial, types.TypeBigSerial:
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
	case types.TypeSmallInt, types.TypeInt32, types.TypeBigInt, types.TypeSerial, types.TypeBigSerial:
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

// execRowid extracts the rowid from the current cursor position
func (vm *VM) execRowid(instr *Instruction) error {
	cursorIdx := instr.P1
	destReg := instr.P2

	if cursorIdx >= len(vm.cursors) || vm.cursors[cursorIdx] == nil {
		return fmt.Errorf("cursor %d not open", cursorIdx)
	}

	cursor := vm.cursors[cursorIdx].cursor
	if !cursor.Valid() {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	// Extract rowid from key (big-endian 8-byte integer)
	key := cursor.Key()
	if len(key) < 8 {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	var rowid int64
	for i := 0; i < 8; i++ {
		rowid = (rowid << 8) | int64(key[i])
	}

	vm.registers[destReg] = types.NewInt(rowid)
	vm.pc++
	return nil
}

// execSeek seeks the cursor to a specific rowid
func (vm *VM) execSeek(instr *Instruction) error {
	cursorIdx := instr.P1
	jumpAddr := instr.P2
	rowidReg := instr.P3

	if cursorIdx >= len(vm.cursors) || vm.cursors[cursorIdx] == nil {
		return fmt.Errorf("cursor %d not open", cursorIdx)
	}

	// Get rowid from register
	rowidVal := vm.registers[rowidReg]
	rowid := rowidVal.Int()

	// Create key from rowid (big-endian)
	key := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		key[i] = byte(rowid)
		rowid >>= 8
	}

	// Seek to the key
	cursor := vm.cursors[cursorIdx].cursor
	cursor.Seek(key)

	// Check if we found the exact key
	if !cursor.Valid() {
		// Not found, jump to P2
		vm.pc = jumpAddr
		return nil
	}

	// Verify the key matches exactly
	foundKey := cursor.Key()
	for i := 0; i < 8; i++ {
		if foundKey[i] != key[i] {
			// Key doesn't match, jump to P2
			vm.pc = jumpAddr
			return nil
		}
	}

	// Found the row, continue to next instruction
	vm.pc++
	return nil
}

// execDelete deletes the current row from the cursor
func (vm *VM) execDelete(instr *Instruction) error {
	cursorIdx := instr.P1

	if cursorIdx >= len(vm.cursors) || vm.cursors[cursorIdx] == nil {
		return fmt.Errorf("cursor %d not open", cursorIdx)
	}

	vdbeCursor := vm.cursors[cursorIdx]
	cursor := vdbeCursor.cursor

	if !cursor.Valid() {
		// No current row, nothing to delete
		vm.pc++
		return nil
	}

	// Get the current key
	key := cursor.Key()
	if key == nil {
		vm.pc++
		return nil
	}

	// Make a copy of the key since the cursor may become invalid after delete
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	// Delete from B-tree
	bt := vdbeCursor.btree
	if err := bt.Delete(keyCopy); err != nil {
		return fmt.Errorf("delete failed: %w", err)
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

// Aggregate operation helpers

// execAggInit initializes an aggregate function
func (vm *VM) execAggInit(instr *Instruction) error {
	aggIdx := instr.P1
	aggName, ok := instr.P4.(string)
	if !ok {
		return fmt.Errorf("OpAggInit requires aggregate name in P4")
	}

	// Ensure we have enough aggregate slots
	for len(vm.aggregates) <= aggIdx {
		vm.aggregates = append(vm.aggregates, nil)
	}

	// Create and initialize the aggregate
	agg := GetAggregate(aggName)
	if agg == nil {
		return fmt.Errorf("unknown aggregate function: %s", aggName)
	}
	agg.Init()
	vm.aggregates[aggIdx] = agg

	vm.pc++
	return nil
}

// execAggStep steps an aggregate with a value
func (vm *VM) execAggStep(instr *Instruction) error {
	aggIdx := instr.P1
	valueReg := instr.P2

	if aggIdx >= len(vm.aggregates) || vm.aggregates[aggIdx] == nil {
		return fmt.Errorf("aggregate %d not initialized", aggIdx)
	}

	value := vm.registers[valueReg]
	vm.aggregates[aggIdx].Step(value)

	vm.pc++
	return nil
}

// execAggFinal finalizes an aggregate and stores result
func (vm *VM) execAggFinal(instr *Instruction) error {
	aggIdx := instr.P1
	destReg := instr.P2

	if aggIdx >= len(vm.aggregates) || vm.aggregates[aggIdx] == nil {
		return fmt.Errorf("aggregate %d not initialized", aggIdx)
	}

	result := vm.aggregates[aggIdx].Finalize()
	vm.registers[destReg] = result

	vm.pc++
	return nil
}

// executeVectorDot computes dot product of two vectors
// r[P3] = dot_product(r[P1], r[P2])
func (vm *VM) executeVectorDot(instr *Instruction) error {
	v1Reg := instr.P1
	v2Reg := instr.P2
	destReg := instr.P3

	v1Val := vm.registers[v1Reg]
	v2Val := vm.registers[v2Reg]

	if v1Val.Type() != types.TypeVector || v2Val.Type() != types.TypeVector {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	vec1 := v1Val.Vector()
	vec2 := v2Val.Vector()

	if vec1 == nil || vec2 == nil {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	dot := vec1.DotProduct(vec2)
	vm.registers[destReg] = types.NewFloat(float64(dot))

	vm.pc++
	return nil
}

// executeVectorToBlob serializes a vector to a blob
// r[P2] = vector_to_blob(r[P1])
func (vm *VM) executeVectorToBlob(instr *Instruction) error {
	srcReg := instr.P1
	destReg := instr.P2

	srcVal := vm.registers[srcReg]

	if srcVal.Type() != types.TypeVector {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	vec := srcVal.Vector()
	if vec == nil {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	blob := vec.ToBytes()
	vm.registers[destReg] = types.NewBlob(blob)

	vm.pc++
	return nil
}

// executeVectorFromBlob deserializes a blob to a vector
// r[P2] = vector_from_blob(r[P1])
func (vm *VM) executeVectorFromBlob(instr *Instruction) error {
	srcReg := instr.P1
	destReg := instr.P2

	srcVal := vm.registers[srcReg]

	if srcVal.Type() != types.TypeBlob {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	blob := srcVal.Blob()
	if blob == nil {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	vec, err := types.VectorFromBytes(blob)
	if err != nil {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	vm.registers[destReg] = types.NewVectorValue(vec)

	vm.pc++
	return nil
}

// executeVectorNormalize normalizes a vector to unit length
// r[P2] = normalize(r[P1])
func (vm *VM) executeVectorNormalize(instr *Instruction) error {
	srcReg := instr.P1
	destReg := instr.P2

	srcVal := vm.registers[srcReg]

	if srcVal.Type() != types.TypeVector {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	srcVec := srcVal.Vector()
	if srcVec == nil {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	// Create a copy and normalize it
	normalized := srcVec.NormalizedCopy()
	vm.registers[destReg] = types.NewVectorValue(normalized)

	vm.pc++
	return nil
}

// ExecuteVectorDistance computes cosine distance between two vectors
// r[P3] = cosine_distance(r[P1], r[P2])
func (vm *VM) ExecuteVectorDistance(instr *Instruction) error {
	v1Reg := instr.P1
	v2Reg := instr.P2
	destReg := instr.P3

	v1Val := vm.registers[v1Reg]
	v2Val := vm.registers[v2Reg]

	// Check if both values are vectors
	if v1Val.Type() != types.TypeVector || v2Val.Type() != types.TypeVector {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	vec1 := v1Val.Vector()
	vec2 := v2Val.Vector()

	if vec1 == nil || vec2 == nil {
		vm.registers[destReg] = types.NewNull()
		vm.pc++
		return nil
	}

	// Compute cosine distance
	dist := vec1.CosineDistance(vec2)
	vm.registers[destReg] = types.NewFloat(float64(dist))

	vm.pc++
	return nil
}
