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
