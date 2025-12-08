// pkg/vdbe/vm_test.go
package vdbe

import (
	"testing"

	"tur/pkg/types"
)

func TestVMCreate(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	if vm == nil {
		t.Fatal("expected non-nil VM")
	}
}

func TestVMRegisterCount(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(10)

	if vm.NumRegisters() != 10 {
		t.Errorf("expected 10 registers, got %d", vm.NumRegisters())
	}
}

func TestVMRunHalt(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if vm.PC() != 0 {
		t.Errorf("expected PC=0 after halt, got %d", vm.PC())
	}
}

func TestVMRunInteger(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInteger, 42, 1, 0) // Store 42 in register 1
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(1)
	if val.Type() != types.TypeInt {
		t.Errorf("expected TypeInt, got %v", val.Type())
	}
	if val.Int() != 42 {
		t.Errorf("expected 42, got %d", val.Int())
	}
}

func TestVMRunString(t *testing.T) {
	prog := NewProgram()
	prog.AddOp4(OpString, 5, 2, 0, "hello") // Store "hello" in register 2
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(2)
	if val.Type() != types.TypeText {
		t.Errorf("expected TypeText, got %v", val.Type())
	}
	if val.Text() != "hello" {
		t.Errorf("expected 'hello', got '%s'", val.Text())
	}
}

func TestVMRunNull(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpNull, 0, 3, 0) // Store NULL in register 3
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(3)
	if !val.IsNull() {
		t.Error("expected null value")
	}
}

func TestVMRunInit(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 2, 0)     // Jump to instruction 2
	prog.AddOp(OpInteger, 99, 1, 0) // Should be skipped
	prog.AddOp(OpInteger, 42, 1, 0) // This should execute
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(1)
	if val.Int() != 42 {
		t.Errorf("expected 42 (Init should have jumped), got %d", val.Int())
	}
}

func TestVMRunGoto(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpGoto, 0, 2, 0)     // Jump to instruction 2
	prog.AddOp(OpInteger, 99, 1, 0) // Should be skipped
	prog.AddOp(OpInteger, 42, 1, 0) // This should execute
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(1)
	if val.Int() != 42 {
		t.Errorf("expected 42 (Goto should have jumped), got %d", val.Int())
	}
}

func TestVMRunAdd(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInteger, 10, 1, 0) // r[1] = 10
	prog.AddOp(OpInteger, 32, 2, 0) // r[2] = 32
	prog.AddOp(OpAdd, 1, 2, 3)      // r[3] = r[1] + r[2]
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(3)
	if val.Int() != 42 {
		t.Errorf("expected 42, got %d", val.Int())
	}
}

func TestVMRunSubtract(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInteger, 50, 1, 0) // r[1] = 50
	prog.AddOp(OpInteger, 8, 2, 0)  // r[2] = 8
	prog.AddOp(OpSubtract, 1, 2, 3) // r[3] = r[1] - r[2]
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(3)
	if val.Int() != 42 {
		t.Errorf("expected 42, got %d", val.Int())
	}
}

func TestVMRunMultiply(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInteger, 6, 1, 0)  // r[1] = 6
	prog.AddOp(OpInteger, 7, 2, 0)  // r[2] = 7
	prog.AddOp(OpMultiply, 1, 2, 3) // r[3] = r[1] * r[2]
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(3)
	if val.Int() != 42 {
		t.Errorf("expected 42, got %d", val.Int())
	}
}

func TestVMRunDivide(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInteger, 84, 1, 0) // r[1] = 84
	prog.AddOp(OpInteger, 2, 2, 0)  // r[2] = 2
	prog.AddOp(OpDivide, 1, 2, 3)   // r[3] = r[1] / r[2]
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(3)
	if val.Int() != 42 {
		t.Errorf("expected 42, got %d", val.Int())
	}
}

func TestVMRunEq(t *testing.T) {
	// Test equal values - should jump
	prog := NewProgram()
	prog.AddOp(OpInteger, 42, 1, 0) // r[1] = 42
	prog.AddOp(OpInteger, 42, 2, 0) // r[2] = 42
	prog.AddOp(OpEq, 1, 4, 2)       // If r[1] == r[2], jump to 4
	prog.AddOp(OpInteger, 0, 3, 0)  // r[3] = 0 (should be skipped)
	prog.AddOp(OpInteger, 1, 3, 0)  // r[3] = 1 (should execute)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(3)
	if val.Int() != 1 {
		t.Errorf("expected 1 (Eq should have jumped), got %d", val.Int())
	}
}

func TestVMRunLt(t *testing.T) {
	// Test less than - should jump
	prog := NewProgram()
	prog.AddOp(OpInteger, 10, 1, 0) // r[1] = 10
	prog.AddOp(OpInteger, 20, 2, 0) // r[2] = 20
	prog.AddOp(OpLt, 1, 4, 2)       // If r[1] < r[2], jump to 4
	prog.AddOp(OpInteger, 0, 3, 0)  // r[3] = 0 (should be skipped)
	prog.AddOp(OpInteger, 1, 3, 0)  // r[3] = 1 (should execute)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(3)
	if val.Int() != 1 {
		t.Errorf("expected 1 (Lt should have jumped), got %d", val.Int())
	}
}

func TestVMRunIf(t *testing.T) {
	// Test If with true value - should jump
	prog := NewProgram()
	prog.AddOp(OpInteger, 1, 1, 0)  // r[1] = 1 (true)
	prog.AddOp(OpIf, 1, 3, 0)       // If r[1], jump to 3
	prog.AddOp(OpInteger, 0, 2, 0)  // r[2] = 0 (should be skipped)
	prog.AddOp(OpInteger, 42, 2, 0) // r[2] = 42 (should execute)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(2)
	if val.Int() != 42 {
		t.Errorf("expected 42 (If should have jumped), got %d", val.Int())
	}
}

func TestVMRunIfNot(t *testing.T) {
	// Test IfNot with false value - should jump
	prog := NewProgram()
	prog.AddOp(OpInteger, 0, 1, 0)  // r[1] = 0 (false)
	prog.AddOp(OpIfNot, 1, 3, 0)    // If !r[1], jump to 3
	prog.AddOp(OpInteger, 0, 2, 0)  // r[2] = 0 (should be skipped)
	prog.AddOp(OpInteger, 42, 2, 0) // r[2] = 42 (should execute)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(2)
	if val.Int() != 42 {
		t.Errorf("expected 42 (IfNot should have jumped), got %d", val.Int())
	}
}

func TestVMRunCopy(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInteger, 42, 1, 0) // r[1] = 42
	prog.AddOp(OpCopy, 1, 2, 0)     // r[2] = r[1]
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(2)
	if val.Int() != 42 {
		t.Errorf("expected 42, got %d", val.Int())
	}
}

func TestVMResultRow(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInteger, 1, 1, 0)          // r[1] = 1
	prog.AddOp4(OpString, 5, 2, 0, "hello") // r[2] = "hello"
	prog.AddOp(OpResultRow, 1, 2, 0)        // Output r[1], r[2]
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rows := vm.Results()
	if len(rows) != 1 {
		t.Fatalf("expected 1 result row, got %d", len(rows))
	}
	if len(rows[0]) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(rows[0]))
	}
	if rows[0][0].Int() != 1 {
		t.Errorf("expected first column = 1, got %d", rows[0][0].Int())
	}
	if rows[0][1].Text() != "hello" {
		t.Errorf("expected second column = 'hello', got '%s'", rows[0][1].Text())
	}
}

func TestVMRunVectorDistance(t *testing.T) {
	// Create two vectors and compute cosine distance
	// v1 = [1, 0, 0] (normalized)
	// v2 = [1, 0, 0] (normalized) - same vector, distance should be 0
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{1.0, 0.0, 0.0})

	prog := NewProgram()
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	// Set vectors in registers (using P4 would be for blobs, we use SetRegister for vectors)
	vm.SetRegister(1, types.NewVectorValue(v1))
	vm.SetRegister(2, types.NewVectorValue(v2))

	// Manually invoke distance computation
	distInstr := &Instruction{Op: OpVectorDistance, P1: 1, P2: 2, P3: 3}
	err := vm.ExecuteVectorDistance(distInstr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(3)
	if val.Type() != types.TypeFloat {
		t.Errorf("expected TypeFloat, got %v", val.Type())
	}
	dist := val.Float()
	if dist < -0.001 || dist > 0.001 {
		t.Errorf("expected distance ~0 for identical vectors, got %f", dist)
	}
}

func TestVMRunVectorDistance_Orthogonal(t *testing.T) {
	// v1 = [1, 0, 0], v2 = [0, 1, 0] - orthogonal, cosine distance = 1
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})

	prog := NewProgram()
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)

	vm.SetRegister(1, types.NewVectorValue(v1))
	vm.SetRegister(2, types.NewVectorValue(v2))

	distInstr := &Instruction{Op: OpVectorDistance, P1: 1, P2: 2, P3: 3}
	err := vm.ExecuteVectorDistance(distInstr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(3)
	dist := val.Float()
	// Cosine distance for orthogonal vectors: 1 - 0 = 1
	if dist < 0.999 || dist > 1.001 {
		t.Errorf("expected distance ~1 for orthogonal vectors, got %f", dist)
	}
}

func TestVMRunVectorDistance_ViaProgram(t *testing.T) {
	// Test OpVectorDistance through normal VM execution
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})

	prog := NewProgram()
	prog.AddOp(OpVectorDistance, 1, 2, 3) // r[3] = distance(r[1], r[2])
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)
	vm.SetRegister(1, types.NewVectorValue(v1))
	vm.SetRegister(2, types.NewVectorValue(v2))

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(3)
	if val.Type() != types.TypeFloat {
		t.Errorf("expected TypeFloat, got %v", val.Type())
	}
	dist := val.Float()
	if dist < 0.999 || dist > 1.001 {
		t.Errorf("expected distance ~1 for orthogonal vectors, got %f", dist)
	}
}
