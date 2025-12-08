// pkg/vdbe/vm_test.go
package vdbe

import (
	"path/filepath"
	"testing"

	"tur/pkg/btree"
	"tur/pkg/hnsw"
	"tur/pkg/pager"
	"tur/pkg/record"
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

func TestVMRunVectorNormalize(t *testing.T) {
	// Create an unnormalized vector [3, 4, 0] - magnitude = 5
	v := types.NewVector([]float32{3.0, 4.0, 0.0})

	prog := NewProgram()
	prog.AddOp(OpVectorNormalize, 1, 2, 0) // r[2] = normalize(r[1])
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)
	vm.SetRegister(1, types.NewVectorValue(v))

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(2)
	if val.Type() != types.TypeVector {
		t.Errorf("expected TypeVector, got %v", val.Type())
	}

	normalized := val.Vector()
	if normalized == nil {
		t.Fatal("expected non-nil vector")
	}

	// After normalization: [0.6, 0.8, 0.0]
	data := normalized.Data()
	if len(data) != 3 {
		t.Errorf("expected 3 dimensions, got %d", len(data))
	}

	// Check normalized values (3/5 = 0.6, 4/5 = 0.8)
	epsilon := float32(0.001)
	if data[0] < 0.6-epsilon || data[0] > 0.6+epsilon {
		t.Errorf("expected data[0] ~0.6, got %f", data[0])
	}
	if data[1] < 0.8-epsilon || data[1] > 0.8+epsilon {
		t.Errorf("expected data[1] ~0.8, got %f", data[1])
	}
	if data[2] < -epsilon || data[2] > epsilon {
		t.Errorf("expected data[2] ~0, got %f", data[2])
	}
}

func TestVMRunVectorNormalize_AlreadyNormalized(t *testing.T) {
	// Already normalized vector
	v := types.NewVector([]float32{1.0, 0.0, 0.0})

	prog := NewProgram()
	prog.AddOp(OpVectorNormalize, 1, 2, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)
	vm.SetRegister(1, types.NewVectorValue(v))

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(2)
	normalized := val.Vector()
	data := normalized.Data()

	epsilon := float32(0.001)
	if data[0] < 1.0-epsilon || data[0] > 1.0+epsilon {
		t.Errorf("expected data[0] ~1.0, got %f", data[0])
	}
}

func TestVMVectorSearchCursor(t *testing.T) {
	// Create a simple HNSW index with a few vectors
	cfg := hnsw.DefaultConfig(3)
	idx := hnsw.NewIndex(cfg)

	// Insert some test vectors
	idx.Insert(1, types.NewVector([]float32{1.0, 0.0, 0.0}))
	idx.Insert(2, types.NewVector([]float32{0.0, 1.0, 0.0}))
	idx.Insert(3, types.NewVector([]float32{0.0, 0.0, 1.0}))
	idx.Insert(4, types.NewVector([]float32{0.9, 0.1, 0.0})) // Closest to [1,0,0]

	// Query vector closest to [1, 0, 0]
	queryVec := types.NewVector([]float32{1.0, 0.0, 0.0})

	prog := NewProgram()
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(10)

	// Set up query vector in register 1
	vm.SetRegister(1, types.NewVectorValue(queryVec))

	// Create vector search cursor
	cursor := vm.CreateVectorSearchCursor(idx, 2) // k=2

	// Execute search with query from register 1
	err := cursor.Search(vm.Register(1).Vector())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Get first result
	if !cursor.Valid() {
		t.Fatal("expected cursor to be valid after search")
	}

	rowID1, dist1 := cursor.Current()
	// First result should be rowID 1 or 4 (both very close to query)
	if rowID1 != 1 && rowID1 != 4 {
		t.Errorf("expected first result rowID 1 or 4, got %d", rowID1)
	}
	if dist1 > 0.2 {
		t.Errorf("expected distance < 0.2 for closest vector, got %f", dist1)
	}

	// Move to second result
	cursor.Next()
	if !cursor.Valid() {
		t.Fatal("expected cursor to be valid for second result")
	}

	rowID2, _ := cursor.Current()
	if rowID2 == rowID1 {
		t.Error("second result should be different from first")
	}

	// Move past results
	cursor.Next()
	if cursor.Valid() {
		t.Error("expected cursor to be invalid after k results")
	}
}

func TestVMRunVectorToBlob(t *testing.T) {
	// Create a vector and serialize to blob
	v := types.NewVector([]float32{1.0, 2.0, 3.0})

	prog := NewProgram()
	prog.AddOp(OpVectorToBlob, 1, 2, 0) // r[2] = vector_to_blob(r[1])
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)
	vm.SetRegister(1, types.NewVectorValue(v))

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(2)
	if val.Type() != types.TypeBlob {
		t.Errorf("expected TypeBlob, got %v", val.Type())
	}

	blob := val.Blob()
	// 4 bytes dimension + 3*4 bytes data = 16 bytes
	if len(blob) != 16 {
		t.Errorf("expected 16 bytes, got %d", len(blob))
	}
}

func TestVMRunVectorFromBlob(t *testing.T) {
	// Create a blob from a vector, then deserialize it back
	v := types.NewVector([]float32{1.0, 2.0, 3.0})
	blob := v.ToBytes()

	prog := NewProgram()
	prog.AddOp(OpVectorFromBlob, 1, 2, 0) // r[2] = vector_from_blob(r[1])
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)
	vm.SetRegister(1, types.NewBlob(blob))

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(2)
	if val.Type() != types.TypeVector {
		t.Errorf("expected TypeVector, got %v", val.Type())
	}

	vec := val.Vector()
	if vec == nil {
		t.Fatal("expected non-nil vector")
	}

	data := vec.Data()
	if len(data) != 3 {
		t.Errorf("expected 3 dimensions, got %d", len(data))
	}

	epsilon := float32(0.001)
	if data[0] < 1.0-epsilon || data[0] > 1.0+epsilon {
		t.Errorf("expected data[0] ~1.0, got %f", data[0])
	}
	if data[1] < 2.0-epsilon || data[1] > 2.0+epsilon {
		t.Errorf("expected data[1] ~2.0, got %f", data[1])
	}
	if data[2] < 3.0-epsilon || data[2] > 3.0+epsilon {
		t.Errorf("expected data[2] ~3.0, got %f", data[2])
	}
}

func TestVMRunVectorRoundtrip(t *testing.T) {
	// Test complete roundtrip: vector -> blob -> vector
	v := types.NewVector([]float32{0.5, 0.5, 0.5, 0.5})

	prog := NewProgram()
	prog.AddOp(OpVectorToBlob, 1, 2, 0)   // r[2] = blob(r[1])
	prog.AddOp(OpVectorFromBlob, 2, 3, 0) // r[3] = vector(r[2])
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	vm.SetNumRegisters(5)
	vm.SetRegister(1, types.NewVectorValue(v))

	err := vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Compare original and roundtripped vectors
	originalVec := vm.Register(1).Vector()
	roundtrippedVec := vm.Register(3).Vector()

	if roundtrippedVec == nil {
		t.Fatal("expected non-nil roundtripped vector")
	}

	origData := originalVec.Data()
	rtData := roundtrippedVec.Data()

	if len(origData) != len(rtData) {
		t.Errorf("dimension mismatch: %d vs %d", len(origData), len(rtData))
	}

	epsilon := float32(0.001)
	for i := range origData {
		diff := origData[i] - rtData[i]
		if diff < -epsilon || diff > epsilon {
			t.Errorf("mismatch at index %d: %f vs %f", i, origData[i], rtData[i])
		}
	}
}

func TestVMRunVectorDot(t *testing.T) {
	// Test dot product: [1, 2, 3] · [4, 5, 6] = 4 + 10 + 18 = 32
	v1 := types.NewVector([]float32{1.0, 2.0, 3.0})
	v2 := types.NewVector([]float32{4.0, 5.0, 6.0})

	prog := NewProgram()
	prog.AddOp(OpVectorDot, 1, 2, 3) // r[3] = dot(r[1], r[2])
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

	dot := val.Float()
	if dot < 31.99 || dot > 32.01 {
		t.Errorf("expected dot product ~32, got %f", dot)
	}
}

func TestVMRunVectorDot_Orthogonal(t *testing.T) {
	// Orthogonal vectors: [1, 0, 0] · [0, 1, 0] = 0
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})

	prog := NewProgram()
	prog.AddOp(OpVectorDot, 1, 2, 3)
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
	dot := val.Float()
	if dot < -0.001 || dot > 0.001 {
		t.Errorf("expected dot product ~0 for orthogonal vectors, got %f", dot)
	}
}

// TestVMRunRowid tests OpRowid: extract rowid from cursor into register
func TestVMRunRowid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Create B-tree and insert a row with rowid=42
	bt, _ := btree.Create(p)
	key := make([]byte, 8)
	rowid := int64(42)
	for i := 7; i >= 0; i-- {
		key[i] = byte(rowid)
		rowid >>= 8
	}
	values := []types.Value{types.NewInt(42), types.NewText("test")}
	bt.Insert(key, record.Encode(values))

	// Program: OpenRead -> Rewind -> Rowid -> Halt
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpOpenRead, 0, int(bt.RootPage()), 0) // cursor 0, rootPage
	prog.AddOp(OpRewind, 0, 6, 0)                    // cursor 0, jump to halt if empty
	prog.AddOp(OpRowid, 0, 1, 0)                     // cursor 0, dest reg 1
	prog.AddOp(OpClose, 0, 0, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, p)
	vm.SetNumRegisters(5)

	err = vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(1)
	if val.Type() != types.TypeInt {
		t.Errorf("expected TypeInt, got %v", val.Type())
	}
	if val.Int() != 42 {
		t.Errorf("expected rowid 42, got %d", val.Int())
	}
}

// TestVMRunSeek tests OpSeek: seek cursor to specific rowid
func TestVMRunSeek(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Create B-tree and insert multiple rows
	bt, _ := btree.Create(p)
	for _, rowid := range []int64{10, 20, 30} {
		key := make([]byte, 8)
		r := rowid
		for i := 7; i >= 0; i-- {
			key[i] = byte(r)
			r >>= 8
		}
		values := []types.Value{types.NewInt(rowid), types.NewText("value")}
		bt.Insert(key, record.Encode(values))
	}

	// Program: OpenRead -> Integer 20 (rowid to seek) -> Seek -> Column -> Halt
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpOpenRead, 0, int(bt.RootPage()), 0) // cursor 0
	prog.AddOp(OpInteger, 20, 1, 0)                  // r[1] = 20 (rowid to seek)
	prog.AddOp(OpSeek, 0, 7, 1)                      // cursor 0, jump to halt if not found, rowid in r[1]
	prog.AddOp(OpColumn, 0, 0, 2)                    // cursor 0, col 0, dest r[2]
	prog.AddOp(OpClose, 0, 0, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, p)
	vm.SetNumRegisters(5)

	err = vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := vm.Register(2)
	if val.Type() != types.TypeInt {
		t.Errorf("expected TypeInt, got %v", val.Type())
	}
	if val.Int() != 20 {
		t.Errorf("expected value 20, got %d", val.Int())
	}
}

// TestVMRunSeek_NotFound tests OpSeek when rowid doesn't exist
func TestVMRunSeek_NotFound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Create B-tree with one row
	bt, _ := btree.Create(p)
	key := make([]byte, 8)
	key[7] = 10
	values := []types.Value{types.NewInt(10)}
	bt.Insert(key, record.Encode(values))

	// Program: seek to non-existent rowid 99
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpOpenRead, 0, int(bt.RootPage()), 0)
	prog.AddOp(OpInteger, 99, 1, 0)  // r[1] = 99 (rowid that doesn't exist)
	prog.AddOp(OpSeek, 0, 6, 1)      // cursor 0, jump to end if not found
	prog.AddOp(OpInteger, 1, 2, 0)   // r[2] = 1 (should be skipped)
	prog.AddOp(OpGoto, 0, 7, 0)      // skip over the "not found" marker
	prog.AddOp(OpInteger, 99, 2, 0)  // r[2] = 99 (marker for not found)
	prog.AddOp(OpClose, 0, 0, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, p)
	vm.SetNumRegisters(5)

	err = vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// If seek failed, r[2] should be 99
	val := vm.Register(2)
	if val.Int() != 99 {
		t.Errorf("expected 99 (not found marker), got %d", val.Int())
	}
}

// TestVMRunDelete tests OpDelete: delete current row from cursor
func TestVMRunDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Create B-tree and insert rows
	bt, _ := btree.Create(p)
	for _, rowid := range []int64{10, 20, 30} {
		key := make([]byte, 8)
		r := rowid
		for i := 7; i >= 0; i-- {
			key[i] = byte(r)
			r >>= 8
		}
		values := []types.Value{types.NewInt(rowid)}
		bt.Insert(key, record.Encode(values))
	}

	// Verify initial count is 3
	cursor := bt.Cursor()
	count := 0
	for cursor.First(); cursor.Valid(); cursor.Next() {
		count++
	}
	cursor.Close()
	if count != 3 {
		t.Fatalf("expected 3 rows before delete, got %d", count)
	}

	// Program: seek to rowid 20 and delete it
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpOpenWrite, 0, int(bt.RootPage()), 0) // cursor 0, must use OpenWrite for delete
	prog.AddOp(OpInteger, 20, 1, 0)                   // r[1] = 20
	prog.AddOp(OpSeek, 0, 6, 1)                       // cursor 0, jump to close if not found
	prog.AddOp(OpDelete, 0, 0, 0)                     // delete current row from cursor 0
	prog.AddOp(OpClose, 0, 0, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, p)
	vm.SetNumRegisters(5)

	err = vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify row was deleted (count should be 2)
	cursor = bt.Cursor()
	count = 0
	for cursor.First(); cursor.Valid(); cursor.Next() {
		count++
	}
	cursor.Close()
	if count != 2 {
		t.Errorf("expected 2 rows after delete, got %d", count)
	}

	// Verify rowid 20 is gone
	seekKey := make([]byte, 8)
	seekKey[7] = 20
	cursor = bt.Cursor()
	cursor.Seek(seekKey)
	// After seek, cursor should be at 30, not 20
	if cursor.Valid() {
		key := cursor.Key()
		rowid := int64(0)
		for i := 0; i < 8; i++ {
			rowid = (rowid << 8) | int64(key[i])
		}
		if rowid == 20 {
			t.Error("rowid 20 should have been deleted")
		}
	}
	cursor.Close()
}
