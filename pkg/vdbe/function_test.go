// pkg/vdbe/function_test.go
package vdbe

import (
	"testing"

	"tur/pkg/types"
)

func TestFunctionRegistry_Register(t *testing.T) {
	registry := NewFunctionRegistry()

	// Define a simple test function
	testFunc := &ScalarFunction{
		Name:     "test_func",
		NumArgs:  1,
		Function: func(args []types.Value) types.Value { return types.NewInt(42) },
	}

	// Register the function
	registry.Register(testFunc)

	// Lookup should find it
	found := registry.Lookup("test_func")
	if found == nil {
		t.Fatal("expected to find registered function")
	}
	if found.Name != "test_func" {
		t.Errorf("expected name 'test_func', got %q", found.Name)
	}
	if found.NumArgs != 1 {
		t.Errorf("expected 1 arg, got %d", found.NumArgs)
	}
}

func TestFunctionRegistry_LookupNotFound(t *testing.T) {
	registry := NewFunctionRegistry()

	// Lookup unregistered function should return nil
	found := registry.Lookup("nonexistent")
	if found != nil {
		t.Error("expected nil for unregistered function")
	}
}

func TestFunctionRegistry_CaseInsensitive(t *testing.T) {
	registry := NewFunctionRegistry()

	testFunc := &ScalarFunction{
		Name:     "UPPER",
		NumArgs:  1,
		Function: func(args []types.Value) types.Value { return types.NewNull() },
	}
	registry.Register(testFunc)

	// Should find regardless of case
	tests := []string{"UPPER", "upper", "Upper", "uPpEr"}
	for _, name := range tests {
		found := registry.Lookup(name)
		if found == nil {
			t.Errorf("expected to find function with name %q", name)
		}
	}
}

func TestScalarFunction_Call(t *testing.T) {
	// Create a function that adds two integers
	addFunc := &ScalarFunction{
		Name:    "add",
		NumArgs: 2,
		Function: func(args []types.Value) types.Value {
			a := args[0].Int()
			b := args[1].Int()
			return types.NewInt(a + b)
		},
	}

	args := []types.Value{types.NewInt(5), types.NewInt(3)}
	result := addFunc.Call(args)

	if result.Type() != types.TypeInt {
		t.Fatalf("expected int result, got %v", result.Type())
	}
	if result.Int() != 8 {
		t.Errorf("expected 8, got %d", result.Int())
	}
}

func TestFunctionRegistry_VariadicFunction(t *testing.T) {
	registry := NewFunctionRegistry()

	// A variadic function (like COALESCE) has NumArgs = -1
	variadicFunc := &ScalarFunction{
		Name:    "coalesce",
		NumArgs: -1, // Variadic
		Function: func(args []types.Value) types.Value {
			for _, arg := range args {
				if !arg.IsNull() {
					return arg
				}
			}
			return types.NewNull()
		},
	}
	registry.Register(variadicFunc)

	found := registry.Lookup("coalesce")
	if found == nil {
		t.Fatal("expected to find variadic function")
	}
	if found.NumArgs != -1 {
		t.Errorf("expected NumArgs=-1 for variadic, got %d", found.NumArgs)
	}
}

// Tests for built-in scalar functions

func TestSubstr_TwoArgs(t *testing.T) {
	// SUBSTR(string, start) - returns substring from start to end
	// SQLite uses 1-based indexing
	tests := []struct {
		str    string
		start  int64
		expect string
	}{
		{"Hello, World!", 1, "Hello, World!"},
		{"Hello, World!", 8, "World!"},
		{"Hello, World!", 7, " World!"},
		{"Hello", 3, "llo"},
		{"Hello", 6, ""},  // past end
		{"Hello", 0, "Hello"}, // 0 is treated as 1 in SQLite
		{"Hello", -2, "lo"},   // negative counts from end
	}

	registry := DefaultFunctionRegistry()
	substr := registry.Lookup("SUBSTR")
	if substr == nil {
		t.Fatal("SUBSTR function not found in default registry")
	}

	for _, tc := range tests {
		args := []types.Value{types.NewText(tc.str), types.NewInt(tc.start)}
		result := substr.Call(args)

		if result.Type() != types.TypeText {
			t.Errorf("SUBSTR(%q, %d): expected text, got %v", tc.str, tc.start, result.Type())
			continue
		}
		if result.Text() != tc.expect {
			t.Errorf("SUBSTR(%q, %d): expected %q, got %q", tc.str, tc.start, tc.expect, result.Text())
		}
	}
}

func TestSubstr_ThreeArgs(t *testing.T) {
	// SUBSTR(string, start, length) - returns substring
	tests := []struct {
		str    string
		start  int64
		length int64
		expect string
	}{
		{"Hello, World!", 1, 5, "Hello"},
		{"Hello, World!", 8, 5, "World"},
		{"Hello, World!", 1, 0, ""},
		{"Hello", 3, 2, "ll"},
		{"Hello", 1, 100, "Hello"}, // length beyond string
		{"Hello", -2, 2, "lo"},     // negative start
	}

	registry := DefaultFunctionRegistry()
	substr := registry.Lookup("SUBSTR")

	for _, tc := range tests {
		args := []types.Value{types.NewText(tc.str), types.NewInt(tc.start), types.NewInt(tc.length)}
		result := substr.Call(args)

		if result.Type() != types.TypeText {
			t.Errorf("SUBSTR(%q, %d, %d): expected text, got %v", tc.str, tc.start, tc.length, result.Type())
			continue
		}
		if result.Text() != tc.expect {
			t.Errorf("SUBSTR(%q, %d, %d): expected %q, got %q", tc.str, tc.start, tc.length, tc.expect, result.Text())
		}
	}
}

func TestSubstr_NullHandling(t *testing.T) {
	registry := DefaultFunctionRegistry()
	substr := registry.Lookup("SUBSTR")

	// If any argument is NULL, result is NULL
	tests := [][]types.Value{
		{types.NewNull(), types.NewInt(1)},
		{types.NewText("Hello"), types.NewNull()},
		{types.NewNull(), types.NewNull()},
		{types.NewText("Hello"), types.NewInt(1), types.NewNull()},
	}

	for i, args := range tests {
		result := substr.Call(args)
		if !result.IsNull() {
			t.Errorf("test %d: expected NULL result when any arg is NULL", i)
		}
	}
}

func TestLength_String(t *testing.T) {
	registry := DefaultFunctionRegistry()
	length := registry.Lookup("LENGTH")
	if length == nil {
		t.Fatal("LENGTH function not found in default registry")
	}

	tests := []struct {
		input  string
		expect int64
	}{
		{"", 0},
		{"Hello", 5},
		{"Hello, World!", 13},
		{"日本語", 3},                // Unicode characters
		{"Hello 世界", 8},          // Mixed ASCII and Unicode
		{"👋🌍", 2},                // Emoji (counted as characters)
	}

	for _, tc := range tests {
		args := []types.Value{types.NewText(tc.input)}
		result := length.Call(args)

		if result.Type() != types.TypeInt {
			t.Errorf("LENGTH(%q): expected int, got %v", tc.input, result.Type())
			continue
		}
		if result.Int() != tc.expect {
			t.Errorf("LENGTH(%q): expected %d, got %d", tc.input, tc.expect, result.Int())
		}
	}
}

func TestLength_Blob(t *testing.T) {
	registry := DefaultFunctionRegistry()
	length := registry.Lookup("LENGTH")

	// For blobs, length returns byte count
	tests := []struct {
		input  []byte
		expect int64
	}{
		{[]byte{}, 0},
		{[]byte{1, 2, 3}, 3},
		{[]byte{0, 0, 0, 0, 0}, 5},
	}

	for _, tc := range tests {
		args := []types.Value{types.NewBlob(tc.input)}
		result := length.Call(args)

		if result.Type() != types.TypeInt {
			t.Errorf("LENGTH(blob): expected int, got %v", result.Type())
			continue
		}
		if result.Int() != tc.expect {
			t.Errorf("LENGTH(blob len %d): expected %d, got %d", len(tc.input), tc.expect, result.Int())
		}
	}
}

func TestLength_Null(t *testing.T) {
	registry := DefaultFunctionRegistry()
	length := registry.Lookup("LENGTH")

	args := []types.Value{types.NewNull()}
	result := length.Call(args)

	if !result.IsNull() {
		t.Error("LENGTH(NULL) should return NULL")
	}
}

func TestLength_Numeric(t *testing.T) {
	registry := DefaultFunctionRegistry()
	length := registry.Lookup("LENGTH")

	// For numbers, SQLite converts to string first
	tests := []struct {
		input  types.Value
		expect int64
	}{
		{types.NewInt(12345), 5},
		{types.NewInt(-123), 4},
		{types.NewFloat(3.14), 4},
	}

	for _, tc := range tests {
		args := []types.Value{tc.input}
		result := length.Call(args)

		if result.Type() != types.TypeInt {
			t.Errorf("LENGTH(number): expected int, got %v", result.Type())
			continue
		}
		if result.Int() != tc.expect {
			t.Errorf("LENGTH(%v): expected %d, got %d", tc.input, tc.expect, result.Int())
		}
	}
}

func TestUpper(t *testing.T) {
	registry := DefaultFunctionRegistry()
	upper := registry.Lookup("UPPER")
	if upper == nil {
		t.Fatal("UPPER function not found in default registry")
	}

	tests := []struct {
		input  string
		expect string
	}{
		{"hello", "HELLO"},
		{"Hello World", "HELLO WORLD"},
		{"ALREADY UPPER", "ALREADY UPPER"},
		{"", ""},
		{"123abc", "123ABC"},
		{"hélló", "HÉLLÓ"}, // Unicode
		{"日本語", "日本語"},   // Non-Latin (no case change)
	}

	for _, tc := range tests {
		args := []types.Value{types.NewText(tc.input)}
		result := upper.Call(args)

		if result.Type() != types.TypeText {
			t.Errorf("UPPER(%q): expected text, got %v", tc.input, result.Type())
			continue
		}
		if result.Text() != tc.expect {
			t.Errorf("UPPER(%q): expected %q, got %q", tc.input, tc.expect, result.Text())
		}
	}
}

func TestUpper_Null(t *testing.T) {
	registry := DefaultFunctionRegistry()
	upper := registry.Lookup("UPPER")

	args := []types.Value{types.NewNull()}
	result := upper.Call(args)

	if !result.IsNull() {
		t.Error("UPPER(NULL) should return NULL")
	}
}

func TestLower(t *testing.T) {
	registry := DefaultFunctionRegistry()
	lower := registry.Lookup("LOWER")
	if lower == nil {
		t.Fatal("LOWER function not found in default registry")
	}

	tests := []struct {
		input  string
		expect string
	}{
		{"HELLO", "hello"},
		{"Hello World", "hello world"},
		{"already lower", "already lower"},
		{"", ""},
		{"123ABC", "123abc"},
		{"HÉLLÓ", "hélló"}, // Unicode
		{"日本語", "日本語"},   // Non-Latin (no case change)
	}

	for _, tc := range tests {
		args := []types.Value{types.NewText(tc.input)}
		result := lower.Call(args)

		if result.Type() != types.TypeText {
			t.Errorf("LOWER(%q): expected text, got %v", tc.input, result.Type())
			continue
		}
		if result.Text() != tc.expect {
			t.Errorf("LOWER(%q): expected %q, got %q", tc.input, tc.expect, result.Text())
		}
	}
}

func TestLower_Null(t *testing.T) {
	registry := DefaultFunctionRegistry()
	lower := registry.Lookup("LOWER")

	args := []types.Value{types.NewNull()}
	result := lower.Call(args)

	if !result.IsNull() {
		t.Error("LOWER(NULL) should return NULL")
	}
}

func TestCoalesce(t *testing.T) {
	registry := DefaultFunctionRegistry()
	coalesce := registry.Lookup("COALESCE")
	if coalesce == nil {
		t.Fatal("COALESCE function not found in default registry")
	}

	tests := []struct {
		args   []types.Value
		expect types.Value
	}{
		// First non-null value
		{[]types.Value{types.NewNull(), types.NewInt(1)}, types.NewInt(1)},
		{[]types.Value{types.NewNull(), types.NewNull(), types.NewText("hello")}, types.NewText("hello")},
		// First value is non-null
		{[]types.Value{types.NewInt(42), types.NewNull()}, types.NewInt(42)},
		{[]types.Value{types.NewText("first"), types.NewText("second")}, types.NewText("first")},
		// All null
		{[]types.Value{types.NewNull(), types.NewNull()}, types.NewNull()},
		// Single value
		{[]types.Value{types.NewInt(5)}, types.NewInt(5)},
		{[]types.Value{types.NewNull()}, types.NewNull()},
		// Mixed types
		{[]types.Value{types.NewNull(), types.NewFloat(3.14)}, types.NewFloat(3.14)},
	}

	for i, tc := range tests {
		result := coalesce.Call(tc.args)

		if tc.expect.IsNull() {
			if !result.IsNull() {
				t.Errorf("test %d: expected NULL, got %v", i, result)
			}
		} else {
			if result.Type() != tc.expect.Type() {
				t.Errorf("test %d: expected type %v, got %v", i, tc.expect.Type(), result.Type())
				continue
			}
			switch result.Type() {
			case types.TypeInt:
				if result.Int() != tc.expect.Int() {
					t.Errorf("test %d: expected %d, got %d", i, tc.expect.Int(), result.Int())
				}
			case types.TypeText:
				if result.Text() != tc.expect.Text() {
					t.Errorf("test %d: expected %q, got %q", i, tc.expect.Text(), result.Text())
				}
			case types.TypeFloat:
				if result.Float() != tc.expect.Float() {
					t.Errorf("test %d: expected %f, got %f", i, tc.expect.Float(), result.Float())
				}
			}
		}
	}
}

func TestCoalesce_NoArgs(t *testing.T) {
	registry := DefaultFunctionRegistry()
	coalesce := registry.Lookup("COALESCE")

	// Empty args should return NULL
	result := coalesce.Call([]types.Value{})
	if !result.IsNull() {
		t.Error("COALESCE() with no args should return NULL")
	}
}

func TestAbs_Integer(t *testing.T) {
	registry := DefaultFunctionRegistry()
	abs := registry.Lookup("ABS")
	if abs == nil {
		t.Fatal("ABS function not found in default registry")
	}

	tests := []struct {
		input  int64
		expect int64
	}{
		{5, 5},
		{-5, 5},
		{0, 0},
		{-100, 100},
		{-9223372036854775807, 9223372036854775807}, // Large negative
	}

	for _, tc := range tests {
		args := []types.Value{types.NewInt(tc.input)}
		result := abs.Call(args)

		if result.Type() != types.TypeInt {
			t.Errorf("ABS(%d): expected int, got %v", tc.input, result.Type())
			continue
		}
		if result.Int() != tc.expect {
			t.Errorf("ABS(%d): expected %d, got %d", tc.input, tc.expect, result.Int())
		}
	}
}

func TestAbs_Float(t *testing.T) {
	registry := DefaultFunctionRegistry()
	abs := registry.Lookup("ABS")

	tests := []struct {
		input  float64
		expect float64
	}{
		{5.5, 5.5},
		{-5.5, 5.5},
		{0.0, 0.0},
		{-3.14159, 3.14159},
	}

	for _, tc := range tests {
		args := []types.Value{types.NewFloat(tc.input)}
		result := abs.Call(args)

		if result.Type() != types.TypeFloat {
			t.Errorf("ABS(%f): expected float, got %v", tc.input, result.Type())
			continue
		}
		if result.Float() != tc.expect {
			t.Errorf("ABS(%f): expected %f, got %f", tc.input, tc.expect, result.Float())
		}
	}
}

func TestAbs_Null(t *testing.T) {
	registry := DefaultFunctionRegistry()
	abs := registry.Lookup("ABS")

	args := []types.Value{types.NewNull()}
	result := abs.Call(args)

	if !result.IsNull() {
		t.Error("ABS(NULL) should return NULL")
	}
}

func TestRound_OneArg(t *testing.T) {
	registry := DefaultFunctionRegistry()
	round := registry.Lookup("ROUND")
	if round == nil {
		t.Fatal("ROUND function not found in default registry")
	}

	tests := []struct {
		input  float64
		expect float64
	}{
		{3.14159, 3.0},
		{3.5, 4.0},
		{3.4, 3.0},
		{-2.5, -3.0}, // SQLite rounds away from zero
		{-2.4, -2.0},
		{0.0, 0.0},
	}

	for _, tc := range tests {
		args := []types.Value{types.NewFloat(tc.input)}
		result := round.Call(args)

		if result.Type() != types.TypeFloat {
			t.Errorf("ROUND(%f): expected float, got %v", tc.input, result.Type())
			continue
		}
		if result.Float() != tc.expect {
			t.Errorf("ROUND(%f): expected %f, got %f", tc.input, tc.expect, result.Float())
		}
	}
}

func TestRound_TwoArgs(t *testing.T) {
	registry := DefaultFunctionRegistry()
	round := registry.Lookup("ROUND")

	tests := []struct {
		input    float64
		decimals int64
		expect   float64
	}{
		{3.14159, 2, 3.14},
		{3.14159, 3, 3.142},
		{3.145, 2, 3.15}, // Rounding
		{1234.5678, 1, 1234.6},
		{1234.5678, 0, 1235.0},
		{1234.5678, -1, 1230.0}, // Negative decimals
		{1234.5678, -2, 1200.0},
	}

	for _, tc := range tests {
		args := []types.Value{types.NewFloat(tc.input), types.NewInt(tc.decimals)}
		result := round.Call(args)

		if result.Type() != types.TypeFloat {
			t.Errorf("ROUND(%f, %d): expected float, got %v", tc.input, tc.decimals, result.Type())
			continue
		}
		if result.Float() != tc.expect {
			t.Errorf("ROUND(%f, %d): expected %f, got %f", tc.input, tc.decimals, tc.expect, result.Float())
		}
	}
}

func TestRound_Integer(t *testing.T) {
	registry := DefaultFunctionRegistry()
	round := registry.Lookup("ROUND")

	// Integers should work too
	args := []types.Value{types.NewInt(42)}
	result := round.Call(args)

	if result.Type() != types.TypeFloat {
		t.Errorf("ROUND(42): expected float, got %v", result.Type())
	}
	if result.Float() != 42.0 {
		t.Errorf("ROUND(42): expected 42.0, got %f", result.Float())
	}
}

func TestRound_Null(t *testing.T) {
	registry := DefaultFunctionRegistry()
	round := registry.Lookup("ROUND")

	// NULL input
	args := []types.Value{types.NewNull()}
	result := round.Call(args)
	if !result.IsNull() {
		t.Error("ROUND(NULL) should return NULL")
	}

	// NULL decimals
	args = []types.Value{types.NewFloat(3.14), types.NewNull()}
	result = round.Call(args)
	if !result.IsNull() {
		t.Error("ROUND(3.14, NULL) should return NULL")
	}
}

// Tests for VECTOR_DISTANCE function

func TestVectorDistance_Registered(t *testing.T) {
	// Task 1: Register vector_distance as scalar function
	registry := DefaultFunctionRegistry()
	vectorDistance := registry.Lookup("VECTOR_DISTANCE")
	if vectorDistance == nil {
		t.Fatal("VECTOR_DISTANCE function not found in default registry")
	}
	if vectorDistance.NumArgs != 2 {
		t.Errorf("expected NumArgs=2, got %d", vectorDistance.NumArgs)
	}
}

func TestVectorDistance_WithVectorValues(t *testing.T) {
	// Tasks 2-5: Parse vectors, compute cosine distance, return REAL
	registry := DefaultFunctionRegistry()
	vectorDistance := registry.Lookup("VECTOR_DISTANCE")
	if vectorDistance == nil {
		t.Fatal("VECTOR_DISTANCE function not found in default registry")
	}

	// Create two normalized vectors for testing
	// Using simple normalized vectors: [1, 0, 0] and [1, 0, 0] should have distance 0
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{1.0, 0.0, 0.0})

	args := []types.Value{types.NewVectorValue(v1), types.NewVectorValue(v2)}
	result := vectorDistance.Call(args)

	if result.Type() != types.TypeFloat {
		t.Fatalf("expected REAL result, got %v", result.Type())
	}
	// Identical normalized vectors have distance 0
	if result.Float() != 0.0 {
		t.Errorf("expected distance 0.0 for identical vectors, got %f", result.Float())
	}
}

func TestVectorDistance_OrthogonalVectors(t *testing.T) {
	// Orthogonal normalized vectors should have distance 1.0
	registry := DefaultFunctionRegistry()
	vectorDistance := registry.Lookup("VECTOR_DISTANCE")
	if vectorDistance == nil {
		t.Fatal("VECTOR_DISTANCE function not found in default registry")
	}

	// [1, 0, 0] and [0, 1, 0] are orthogonal - cosine similarity is 0, distance is 1
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})

	args := []types.Value{types.NewVectorValue(v1), types.NewVectorValue(v2)}
	result := vectorDistance.Call(args)

	if result.Type() != types.TypeFloat {
		t.Fatalf("expected REAL result, got %v", result.Type())
	}
	// Orthogonal vectors have distance 1.0 (cosine distance = 1 - 0 = 1)
	if result.Float() != 1.0 {
		t.Errorf("expected distance 1.0 for orthogonal vectors, got %f", result.Float())
	}
}

func TestVectorDistance_OppositeVectors(t *testing.T) {
	// Opposite normalized vectors should have distance 2.0
	registry := DefaultFunctionRegistry()
	vectorDistance := registry.Lookup("VECTOR_DISTANCE")
	if vectorDistance == nil {
		t.Fatal("VECTOR_DISTANCE function not found in default registry")
	}

	// [1, 0, 0] and [-1, 0, 0] are opposite - cosine similarity is -1, distance is 2
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{-1.0, 0.0, 0.0})

	args := []types.Value{types.NewVectorValue(v1), types.NewVectorValue(v2)}
	result := vectorDistance.Call(args)

	if result.Type() != types.TypeFloat {
		t.Fatalf("expected REAL result, got %v", result.Type())
	}
	// Opposite vectors have distance 2.0 (cosine distance = 1 - (-1) = 2)
	if result.Float() != 2.0 {
		t.Errorf("expected distance 2.0 for opposite vectors, got %f", result.Float())
	}
}

func TestVectorDistance_WithBlobs(t *testing.T) {
	// Tasks 2-3: Parse and deserialize vectors from blobs
	registry := DefaultFunctionRegistry()
	vectorDistance := registry.Lookup("VECTOR_DISTANCE")
	if vectorDistance == nil {
		t.Fatal("VECTOR_DISTANCE function not found in default registry")
	}

	// Create vectors and convert to blobs
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{1.0, 0.0, 0.0})

	blob1 := v1.ToBytes()
	blob2 := v2.ToBytes()

	args := []types.Value{types.NewBlob(blob1), types.NewBlob(blob2)}
	result := vectorDistance.Call(args)

	if result.Type() != types.TypeFloat {
		t.Fatalf("expected REAL result from blob vectors, got %v", result.Type())
	}
	if result.Float() != 0.0 {
		t.Errorf("expected distance 0.0 for identical blob vectors, got %f", result.Float())
	}
}

func TestVectorDistance_MixedVectorAndBlob(t *testing.T) {
	// Test with one Vector and one Blob
	registry := DefaultFunctionRegistry()
	vectorDistance := registry.Lookup("VECTOR_DISTANCE")
	if vectorDistance == nil {
		t.Fatal("VECTOR_DISTANCE function not found in default registry")
	}

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{0.0, 1.0, 0.0})
	blob2 := v2.ToBytes()

	args := []types.Value{types.NewVectorValue(v1), types.NewBlob(blob2)}
	result := vectorDistance.Call(args)

	if result.Type() != types.TypeFloat {
		t.Fatalf("expected REAL result from mixed types, got %v", result.Type())
	}
	if result.Float() != 1.0 {
		t.Errorf("expected distance 1.0 for orthogonal mixed types, got %f", result.Float())
	}
}

func TestVectorDistance_NullHandling(t *testing.T) {
	registry := DefaultFunctionRegistry()
	vectorDistance := registry.Lookup("VECTOR_DISTANCE")
	if vectorDistance == nil {
		t.Fatal("VECTOR_DISTANCE function not found in default registry")
	}

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})

	// NULL first argument
	args := []types.Value{types.NewNull(), types.NewVectorValue(v1)}
	result := vectorDistance.Call(args)
	if !result.IsNull() {
		t.Error("VECTOR_DISTANCE(NULL, vec) should return NULL")
	}

	// NULL second argument
	args = []types.Value{types.NewVectorValue(v1), types.NewNull()}
	result = vectorDistance.Call(args)
	if !result.IsNull() {
		t.Error("VECTOR_DISTANCE(vec, NULL) should return NULL")
	}

	// Both NULL
	args = []types.Value{types.NewNull(), types.NewNull()}
	result = vectorDistance.Call(args)
	if !result.IsNull() {
		t.Error("VECTOR_DISTANCE(NULL, NULL) should return NULL")
	}
}

func TestVectorDistance_DimensionMismatch(t *testing.T) {
	registry := DefaultFunctionRegistry()
	vectorDistance := registry.Lookup("VECTOR_DISTANCE")
	if vectorDistance == nil {
		t.Fatal("VECTOR_DISTANCE function not found in default registry")
	}

	// Vectors with different dimensions
	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})
	v2 := types.NewVector([]float32{1.0, 0.0}) // 2D vs 3D

	args := []types.Value{types.NewVectorValue(v1), types.NewVectorValue(v2)}
	result := vectorDistance.Call(args)

	if result.Type() != types.TypeFloat {
		t.Fatalf("expected REAL result for dimension mismatch, got %v", result.Type())
	}
	// Dimension mismatch returns 2.0 (max distance) as per Vector.CosineDistance behavior
	if result.Float() != 2.0 {
		t.Errorf("expected distance 2.0 for dimension mismatch, got %f", result.Float())
	}
}

func TestVectorDistance_InvalidArgs(t *testing.T) {
	registry := DefaultFunctionRegistry()
	vectorDistance := registry.Lookup("VECTOR_DISTANCE")
	if vectorDistance == nil {
		t.Fatal("VECTOR_DISTANCE function not found in default registry")
	}

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})

	// Wrong number of arguments
	args := []types.Value{types.NewVectorValue(v1)}
	result := vectorDistance.Call(args)
	if !result.IsNull() {
		t.Error("VECTOR_DISTANCE with 1 arg should return NULL")
	}

	// Too many arguments
	args = []types.Value{types.NewVectorValue(v1), types.NewVectorValue(v1), types.NewVectorValue(v1)}
	result = vectorDistance.Call(args)
	if !result.IsNull() {
		t.Error("VECTOR_DISTANCE with 3 args should return NULL")
	}

	// Invalid types (not vector or blob)
	args = []types.Value{types.NewInt(42), types.NewVectorValue(v1)}
	result = vectorDistance.Call(args)
	if !result.IsNull() {
		t.Error("VECTOR_DISTANCE(int, vec) should return NULL")
	}

	args = []types.Value{types.NewText("hello"), types.NewVectorValue(v1)}
	result = vectorDistance.Call(args)
	if !result.IsNull() {
		t.Error("VECTOR_DISTANCE(text, vec) should return NULL")
	}
}

func TestVectorDistance_InvalidBlob(t *testing.T) {
	registry := DefaultFunctionRegistry()
	vectorDistance := registry.Lookup("VECTOR_DISTANCE")
	if vectorDistance == nil {
		t.Fatal("VECTOR_DISTANCE function not found in default registry")
	}

	v1 := types.NewVector([]float32{1.0, 0.0, 0.0})

	// Invalid blob (not a valid vector serialization)
	invalidBlob := []byte{0, 1, 2, 3}
	args := []types.Value{types.NewBlob(invalidBlob), types.NewVectorValue(v1)}
	result := vectorDistance.Call(args)
	if !result.IsNull() {
		t.Error("VECTOR_DISTANCE with invalid blob should return NULL")
	}
}

func TestConcat(t *testing.T) {
	registry := DefaultFunctionRegistry()
	concat := registry.Lookup("CONCAT")
	if concat == nil {
		t.Fatal("CONCAT function not found")
	}

	tests := []struct {
		args   []types.Value
		expect string
	}{
		{[]types.Value{types.NewText("Hello"), types.NewText(" "), types.NewText("World")}, "Hello World"},
		{[]types.Value{types.NewText("A"), types.NewText("B")}, "AB"},
		{[]types.Value{types.NewText("Hello"), types.NewNull(), types.NewText("World")}, "HelloWorld"}, // NULL skipped
		{[]types.Value{types.NewInt(42), types.NewText(" items")}, "42 items"}, // Number coercion
		{[]types.Value{}, ""}, // Empty
	}

	for i, tc := range tests {
		result := concat.Call(tc.args)
		if result.Type() != types.TypeText {
			t.Errorf("test %d: expected text, got %v", i, result.Type())
			continue
		}
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q, got %q", i, tc.expect, result.Text())
		}
	}
}

func TestConcatWS(t *testing.T) {
	registry := DefaultFunctionRegistry()
	concatWS := registry.Lookup("CONCAT_WS")
	if concatWS == nil {
		t.Fatal("CONCAT_WS function not found")
	}

	tests := []struct {
		args   []types.Value
		expect string
		isNull bool
	}{
		{[]types.Value{types.NewText(","), types.NewText("a"), types.NewText("b"), types.NewText("c")}, "a,b,c", false},
		{[]types.Value{types.NewText("-"), types.NewText("Hello"), types.NewText("World")}, "Hello-World", false},
		{[]types.Value{types.NewText(","), types.NewText("a"), types.NewNull(), types.NewText("c")}, "a,c", false}, // NULL skipped
		{[]types.Value{types.NewNull(), types.NewText("a"), types.NewText("b")}, "", true}, // NULL separator = NULL result
	}

	for i, tc := range tests {
		result := concatWS.Call(tc.args)
		if tc.isNull {
			if !result.IsNull() {
				t.Errorf("test %d: expected NULL", i)
			}
			continue
		}
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q, got %q", i, tc.expect, result.Text())
		}
	}
}

func TestTrim(t *testing.T) {
	registry := DefaultFunctionRegistry()
	trim := registry.Lookup("TRIM")
	if trim == nil {
		t.Fatal("TRIM function not found")
	}

	tests := []struct {
		args   []types.Value
		expect string
	}{
		{[]types.Value{types.NewText("  hello  ")}, "hello"},
		{[]types.Value{types.NewText("\t\nhello\n\t")}, "hello"},
		{[]types.Value{types.NewText("xxxhelloxxx"), types.NewText("x")}, "hello"},
		{[]types.Value{types.NewText("hello")}, "hello"},
		{[]types.Value{types.NewText("")}, ""},
	}

	for i, tc := range tests {
		result := trim.Call(tc.args)
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q, got %q", i, tc.expect, result.Text())
		}
	}
}

func TestLTrim(t *testing.T) {
	registry := DefaultFunctionRegistry()
	ltrim := registry.Lookup("LTRIM")
	if ltrim == nil {
		t.Fatal("LTRIM function not found")
	}

	tests := []struct {
		args   []types.Value
		expect string
	}{
		{[]types.Value{types.NewText("  hello  ")}, "hello  "},
		{[]types.Value{types.NewText("xxxhello"), types.NewText("x")}, "hello"},
	}

	for i, tc := range tests {
		result := ltrim.Call(tc.args)
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q, got %q", i, tc.expect, result.Text())
		}
	}
}

func TestRTrim(t *testing.T) {
	registry := DefaultFunctionRegistry()
	rtrim := registry.Lookup("RTRIM")
	if rtrim == nil {
		t.Fatal("RTRIM function not found")
	}

	tests := []struct {
		args   []types.Value
		expect string
	}{
		{[]types.Value{types.NewText("  hello  ")}, "  hello"},
		{[]types.Value{types.NewText("helloxxx"), types.NewText("x")}, "hello"},
	}

	for i, tc := range tests {
		result := rtrim.Call(tc.args)
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q, got %q", i, tc.expect, result.Text())
		}
	}
}

func TestLeft(t *testing.T) {
	registry := DefaultFunctionRegistry()
	left := registry.Lookup("LEFT")
	if left == nil {
		t.Fatal("LEFT function not found")
	}

	tests := []struct {
		str    string
		n      int64
		expect string
	}{
		{"hello", 2, "he"},
		{"hello", 10, "hello"},
		{"hello", 0, ""},
		{"日本語", 2, "日本"},
	}

	for i, tc := range tests {
		result := left.Call([]types.Value{types.NewText(tc.str), types.NewInt(tc.n)})
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q, got %q", i, tc.expect, result.Text())
		}
	}
}

func TestRight(t *testing.T) {
	registry := DefaultFunctionRegistry()
	right := registry.Lookup("RIGHT")
	if right == nil {
		t.Fatal("RIGHT function not found")
	}

	tests := []struct {
		str    string
		n      int64
		expect string
	}{
		{"hello", 2, "lo"},
		{"hello", 10, "hello"},
		{"hello", 0, ""},
		{"日本語", 2, "本語"},
	}

	for i, tc := range tests {
		result := right.Call([]types.Value{types.NewText(tc.str), types.NewInt(tc.n)})
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q, got %q", i, tc.expect, result.Text())
		}
	}
}

func TestRepeat(t *testing.T) {
	registry := DefaultFunctionRegistry()
	repeat := registry.Lookup("REPEAT")
	if repeat == nil {
		t.Fatal("REPEAT function not found")
	}

	tests := []struct {
		str    string
		n      int64
		expect string
	}{
		{"ab", 3, "ababab"},
		{"x", 5, "xxxxx"},
		{"hello", 0, ""},
		{"", 5, ""},
	}

	for i, tc := range tests {
		result := repeat.Call([]types.Value{types.NewText(tc.str), types.NewInt(tc.n)})
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q, got %q", i, tc.expect, result.Text())
		}
	}
}

func TestSpace(t *testing.T) {
	registry := DefaultFunctionRegistry()
	space := registry.Lookup("SPACE")
	if space == nil {
		t.Fatal("SPACE function not found")
	}

	tests := []struct {
		n      int64
		expect string
	}{
		{5, "     "},
		{0, ""},
		{1, " "},
	}

	for i, tc := range tests {
		result := space.Call([]types.Value{types.NewInt(tc.n)})
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q (len %d), got %q (len %d)", i, tc.expect, len(tc.expect), result.Text(), len(result.Text()))
		}
	}
}

func TestReplace(t *testing.T) {
	registry := DefaultFunctionRegistry()
	replace := registry.Lookup("REPLACE")
	if replace == nil {
		t.Fatal("REPLACE function not found")
	}

	tests := []struct {
		str, from, to string
		expect        string
	}{
		{"hello world", "world", "Go", "hello Go"},
		{"aaa", "a", "b", "bbb"},
		{"hello", "x", "y", "hello"},
		{"", "a", "b", ""},
	}

	for i, tc := range tests {
		args := []types.Value{types.NewText(tc.str), types.NewText(tc.from), types.NewText(tc.to)}
		result := replace.Call(args)
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q, got %q", i, tc.expect, result.Text())
		}
	}
}

func TestReverse(t *testing.T) {
	registry := DefaultFunctionRegistry()
	reverse := registry.Lookup("REVERSE")
	if reverse == nil {
		t.Fatal("REVERSE function not found")
	}

	tests := []struct {
		input  string
		expect string
	}{
		{"hello", "olleh"},
		{"", ""},
		{"a", "a"},
		{"日本語", "語本日"},
	}

	for i, tc := range tests {
		result := reverse.Call([]types.Value{types.NewText(tc.input)})
		if result.Text() != tc.expect {
			t.Errorf("test %d: expected %q, got %q", i, tc.expect, result.Text())
		}
	}
}
