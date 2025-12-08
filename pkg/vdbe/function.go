// pkg/vdbe/function.go
// Scalar function registry for the VDBE.
package vdbe

import (
	"strings"

	"tur/pkg/types"
)

// ScalarFunc is the signature for scalar function implementations.
type ScalarFunc func(args []types.Value) types.Value

// ScalarFunction represents a registered scalar function.
type ScalarFunction struct {
	Name     string     // Function name (stored in uppercase)
	NumArgs  int        // Number of expected arguments (-1 for variadic)
	Function ScalarFunc // The actual implementation
}

// Call invokes the scalar function with the given arguments.
func (sf *ScalarFunction) Call(args []types.Value) types.Value {
	return sf.Function(args)
}

// FunctionRegistry holds registered scalar functions.
type FunctionRegistry struct {
	functions map[string]*ScalarFunction
}

// NewFunctionRegistry creates a new empty function registry.
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		functions: make(map[string]*ScalarFunction),
	}
}

// Register adds a scalar function to the registry.
func (r *FunctionRegistry) Register(fn *ScalarFunction) {
	// Store with uppercase name for case-insensitive lookup
	name := strings.ToUpper(fn.Name)
	r.functions[name] = fn
}

// Lookup finds a function by name (case-insensitive).
func (r *FunctionRegistry) Lookup(name string) *ScalarFunction {
	return r.functions[strings.ToUpper(name)]
}
