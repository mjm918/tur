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
