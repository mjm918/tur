// pkg/vdbe/opcode_test.go
package vdbe

import "testing"

func TestOpcodeString(t *testing.T) {
	tests := []struct {
		op   Opcode
		want string
	}{
		{OpInit, "Init"},
		{OpHalt, "Halt"},
		{OpInteger, "Integer"},
		{OpString, "String"},
		{OpNull, "Null"},
		{OpGoto, "Goto"},
		{OpIf, "If"},
		{OpIfNot, "IfNot"},
		{OpEq, "Eq"},
		{OpNe, "Ne"},
		{OpLt, "Lt"},
		{OpLe, "Le"},
		{OpGt, "Gt"},
		{OpGe, "Ge"},
		{OpAdd, "Add"},
		{OpSubtract, "Subtract"},
		{OpMultiply, "Multiply"},
		{OpDivide, "Divide"},
		{OpOpenRead, "OpenRead"},
		{OpOpenWrite, "OpenWrite"},
		{OpClose, "Close"},
		{OpRewind, "Rewind"},
		{OpNext, "Next"},
		{OpColumn, "Column"},
		{OpResultRow, "ResultRow"},
		{OpInsert, "Insert"},
		{OpMakeRecord, "MakeRecord"},
	}

	for _, tt := range tests {
		if got := tt.op.String(); got != tt.want {
			t.Errorf("Opcode(%d).String() = %q, want %q", tt.op, got, tt.want)
		}
	}
}

func TestInstructionCreate(t *testing.T) {
	// Test creating an instruction with operands
	instr := Instruction{
		Op: OpInteger,
		P1: 42, // value
		P2: 1,  // destination register
		P3: 0,
	}

	if instr.Op != OpInteger {
		t.Errorf("expected OpInteger, got %v", instr.Op)
	}
	if instr.P1 != 42 {
		t.Errorf("expected P1=42, got %d", instr.P1)
	}
	if instr.P2 != 1 {
		t.Errorf("expected P2=1, got %d", instr.P2)
	}
}

func TestInstructionWithP4(t *testing.T) {
	// Test instruction with P4 (string data)
	instr := Instruction{
		Op: OpString,
		P1: 5, // length
		P2: 2, // destination register
		P4: "hello",
	}

	if instr.P4 != "hello" {
		t.Errorf("expected P4='hello', got %v", instr.P4)
	}
}

func TestProgramAppend(t *testing.T) {
	prog := NewProgram()

	addr1 := prog.AddOp(OpInit, 0, 1, 0)
	addr2 := prog.AddOp(OpInteger, 42, 1, 0)
	addr3 := prog.AddOp(OpHalt, 0, 0, 0)

	if addr1 != 0 {
		t.Errorf("first instruction should be at address 0, got %d", addr1)
	}
	if addr2 != 1 {
		t.Errorf("second instruction should be at address 1, got %d", addr2)
	}
	if addr3 != 2 {
		t.Errorf("third instruction should be at address 2, got %d", addr3)
	}

	if prog.Len() != 3 {
		t.Errorf("expected 3 instructions, got %d", prog.Len())
	}
}

func TestProgramGet(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInteger, 100, 5, 0)

	instr := prog.Get(0)
	if instr.Op != OpInteger {
		t.Errorf("expected OpInteger, got %v", instr.Op)
	}
	if instr.P1 != 100 {
		t.Errorf("expected P1=100, got %d", instr.P1)
	}
}
