// pkg/vdbe/opcode.go
// Package vdbe implements the Virtual Database Engine (VDBE) for TurDB.
// The VDBE is a bytecode interpreter that executes compiled SQL statements.
package vdbe

// Opcode represents a VDBE operation
type Opcode uint8

// VDBE opcodes - following SQLite's design
const (
	// Control flow
	OpInit  Opcode = iota // Initialize program, jump to P2
	OpHalt                // Terminate execution
	OpGoto                // Jump to P2
	OpIf                  // Jump to P2 if r[P1] is true
	OpIfNot               // Jump to P2 if r[P1] is false

	// Literals and registers
	OpInteger // Store integer P1 in register P2
	OpString  // Store string P4 in register P2
	OpNull    // Store NULL in register P2
	OpCopy    // Copy value from register P1 to P2

	// Comparison (result in P3, jump to P2 if condition met)
	OpEq // Jump to P2 if r[P1] == r[P3]
	OpNe // Jump to P2 if r[P1] != r[P3]
	OpLt // Jump to P2 if r[P1] < r[P3]
	OpLe // Jump to P2 if r[P1] <= r[P3]
	OpGt // Jump to P2 if r[P1] > r[P3]
	OpGe // Jump to P2 if r[P1] >= r[P3]

	// Arithmetic (P1 and P2 are source registers, P3 is dest)
	OpAdd      // r[P3] = r[P1] + r[P2]
	OpSubtract // r[P3] = r[P1] - r[P2]
	OpMultiply // r[P3] = r[P1] * r[P2]
	OpDivide   // r[P3] = r[P1] / r[P2]
	OpNegate   // r[P2] = -r[P1]

	// Cursor operations
	OpOpenRead  // Open cursor P1 for reading table with root page P2
	OpOpenWrite // Open cursor P1 for writing table with root page P2
	OpClose     // Close cursor P1
	OpRewind    // Move cursor P1 to first row, jump to P2 if empty
	OpNext      // Advance cursor P1, jump to P2 if more rows
	OpPrev      // Move cursor P1 backward, jump to P2 if more rows
	OpColumn    // Read column P2 from cursor P1 into register P3
	OpRowid     // Store rowid from cursor P1 into register P2
	OpSeek      // Seek cursor P1 to rowid in register P3

	// Record operations
	OpMakeRecord // Create record from registers P1..P1+P2-1, store in P3
	OpInsert     // Insert record r[P2] at rowid r[P3] into cursor P1
	OpDelete     // Delete current row in cursor P1

	// Result operations
	OpResultRow // Output registers P1 through P1+P2-1 as a result row

	// Transaction control
	OpTransaction // Begin transaction
	OpCommit      // Commit transaction
	OpRollback    // Rollback transaction

	// Vector operations (TurDB extension)
	OpVectorSearch // Search HNSW index for K nearest neighbors

	// Aggregation
	OpAggInit  // Initialize aggregate: P1=aggIdx, P4=name (string)
	OpAggStep  // Step aggregate: P1=aggIdx, P2=valueReg
	OpAggFinal // Finalize aggregate: P1=aggIdx, P2=destReg

	// Vector operations (TurDB extension)
	OpVectorDistance  // r[P3] = cosine_distance(r[P1], r[P2])
	OpVectorNormalize // r[P2] = normalize(r[P1])
	OpVectorDot       // r[P3] = dot_product(r[P1], r[P2])
	OpVectorFromBlob  // r[P2] = vector_from_blob(r[P1])
	OpVectorToBlob    // r[P2] = vector_to_blob(r[P1])
)

// String returns the name of the opcode
func (op Opcode) String() string {
	switch op {
	case OpInit:
		return "Init"
	case OpHalt:
		return "Halt"
	case OpGoto:
		return "Goto"
	case OpIf:
		return "If"
	case OpIfNot:
		return "IfNot"
	case OpInteger:
		return "Integer"
	case OpString:
		return "String"
	case OpNull:
		return "Null"
	case OpCopy:
		return "Copy"
	case OpEq:
		return "Eq"
	case OpNe:
		return "Ne"
	case OpLt:
		return "Lt"
	case OpLe:
		return "Le"
	case OpGt:
		return "Gt"
	case OpGe:
		return "Ge"
	case OpAdd:
		return "Add"
	case OpSubtract:
		return "Subtract"
	case OpMultiply:
		return "Multiply"
	case OpDivide:
		return "Divide"
	case OpNegate:
		return "Negate"
	case OpOpenRead:
		return "OpenRead"
	case OpOpenWrite:
		return "OpenWrite"
	case OpClose:
		return "Close"
	case OpRewind:
		return "Rewind"
	case OpNext:
		return "Next"
	case OpPrev:
		return "Prev"
	case OpColumn:
		return "Column"
	case OpRowid:
		return "Rowid"
	case OpSeek:
		return "Seek"
	case OpMakeRecord:
		return "MakeRecord"
	case OpInsert:
		return "Insert"
	case OpDelete:
		return "Delete"
	case OpResultRow:
		return "ResultRow"
	case OpTransaction:
		return "Transaction"
	case OpCommit:
		return "Commit"
	case OpRollback:
		return "Rollback"
	case OpVectorSearch:
		return "VectorSearch"
	case OpAggInit:
		return "AggInit"
	case OpAggStep:
		return "AggStep"
	case OpAggFinal:
		return "AggFinal"
	case OpVectorDistance:
		return "VectorDistance"
	case OpVectorNormalize:
		return "VectorNormalize"
	case OpVectorDot:
		return "VectorDot"
	case OpVectorFromBlob:
		return "VectorFromBlob"
	case OpVectorToBlob:
		return "VectorToBlob"
	default:
		return "Unknown"
	}
}

// Instruction represents a single VDBE instruction
type Instruction struct {
	Op Opcode      // The operation to perform
	P1 int         // First operand (often a register number)
	P2 int         // Second operand (often a jump destination)
	P3 int         // Third operand
	P4 interface{} // Fourth operand (string, pointer, etc.)
	P5 uint16      // Fifth operand (flags)
}

// Program holds a sequence of VDBE instructions
type Program struct {
	instructions []Instruction
}

// NewProgram creates a new empty program
func NewProgram() *Program {
	return &Program{
		instructions: make([]Instruction, 0, 16),
	}
}

// AddOp adds an instruction with up to 3 operands, returns address
func (p *Program) AddOp(op Opcode, p1, p2, p3 int) int {
	addr := len(p.instructions)
	p.instructions = append(p.instructions, Instruction{
		Op: op,
		P1: p1,
		P2: p2,
		P3: p3,
	})
	return addr
}

// AddOp4 adds an instruction with P4 operand
func (p *Program) AddOp4(op Opcode, p1, p2, p3 int, p4 interface{}) int {
	addr := len(p.instructions)
	p.instructions = append(p.instructions, Instruction{
		Op: op,
		P1: p1,
		P2: p2,
		P3: p3,
		P4: p4,
	})
	return addr
}

// Len returns the number of instructions
func (p *Program) Len() int {
	return len(p.instructions)
}

// Get returns the instruction at the given address
func (p *Program) Get(addr int) *Instruction {
	if addr < 0 || addr >= len(p.instructions) {
		return nil
	}
	return &p.instructions[addr]
}

// ChangeP2 changes the P2 operand of instruction at addr
func (p *Program) ChangeP2(addr, newP2 int) {
	if addr >= 0 && addr < len(p.instructions) {
		p.instructions[addr].P2 = newP2
	}
}
