// pkg/vdbe/function.go
// Scalar function registry for the VDBE.
package vdbe

import (
	"fmt"
	"math"
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

// DefaultFunctionRegistry returns a registry with all built-in scalar functions.
func DefaultFunctionRegistry() *FunctionRegistry {
	r := NewFunctionRegistry()

	// Register SUBSTR function
	r.Register(&ScalarFunction{
		Name:     "SUBSTR",
		NumArgs:  -1, // 2 or 3 arguments
		Function: builtinSubstr,
	})

	// Also register as SUBSTRING (alias)
	r.Register(&ScalarFunction{
		Name:     "SUBSTRING",
		NumArgs:  -1,
		Function: builtinSubstr,
	})

	// Register LENGTH function
	r.Register(&ScalarFunction{
		Name:     "LENGTH",
		NumArgs:  1,
		Function: builtinLength,
	})

	// Register UPPER function
	r.Register(&ScalarFunction{
		Name:     "UPPER",
		NumArgs:  1,
		Function: builtinUpper,
	})

	// Register LOWER function
	r.Register(&ScalarFunction{
		Name:     "LOWER",
		NumArgs:  1,
		Function: builtinLower,
	})

	// Register COALESCE function (variadic)
	r.Register(&ScalarFunction{
		Name:     "COALESCE",
		NumArgs:  -1,
		Function: builtinCoalesce,
	})

	// Register ABS function
	r.Register(&ScalarFunction{
		Name:     "ABS",
		NumArgs:  1,
		Function: builtinAbs,
	})

	// Register ROUND function
	r.Register(&ScalarFunction{
		Name:     "ROUND",
		NumArgs:  -1, // 1 or 2 arguments
		Function: builtinRound,
	})

	// Register VECTOR_DISTANCE function
	r.Register(&ScalarFunction{
		Name:     "VECTOR_DISTANCE",
		NumArgs:  2,
		Function: builtinVectorDistance,
	})

	return r
}

// builtinSubstr implements SUBSTR(string, start[, length])
// SQLite uses 1-based indexing. Negative start counts from the end.
func builtinSubstr(args []types.Value) types.Value {
	if len(args) < 2 || len(args) > 3 {
		return types.NewNull()
	}

	// If any argument is NULL, return NULL
	for _, arg := range args {
		if arg.IsNull() {
			return types.NewNull()
		}
	}

	// Get the string (coerce to text if needed)
	var str string
	if args[0].Type() == types.TypeText {
		str = args[0].Text()
	} else {
		// For non-text types, return NULL for now
		return types.NewNull()
	}

	// Get start position (1-based in SQLite)
	start := args[0].Int()
	if args[1].Type() == types.TypeInt {
		start = args[1].Int()
	} else if args[1].Type() == types.TypeFloat {
		start = int64(args[1].Float())
	} else {
		return types.NewNull()
	}

	// Convert to runes for proper Unicode handling
	runes := []rune(str)
	strLen := int64(len(runes))

	// Handle special case: 0 is treated as 1 in SQLite
	if start == 0 {
		start = 1
	}

	// Handle negative start (count from end)
	// In SQLite, -1 means last character, -2 means second to last, etc.
	// SUBSTR("Hello", -2) should return "lo" (last 2 characters)
	var startIdx int64
	if start < 0 {
		startIdx = strLen + start
		if startIdx < 0 {
			startIdx = 0
		}
	} else {
		startIdx = start - 1 // Convert from 1-based to 0-based
	}

	// If start is past the end, return empty string
	if startIdx >= strLen {
		return types.NewText("")
	}
	if startIdx < 0 {
		startIdx = 0
	}

	// Determine length
	var length int64
	if len(args) == 3 {
		if args[2].Type() == types.TypeInt {
			length = args[2].Int()
		} else if args[2].Type() == types.TypeFloat {
			length = int64(args[2].Float())
		} else {
			return types.NewNull()
		}
		if length < 0 {
			length = 0
		}
	} else {
		// No length specified, go to end of string
		length = strLen - startIdx
	}

	// Calculate end index
	endIdx := startIdx + length
	if endIdx > strLen {
		endIdx = strLen
	}

	return types.NewText(string(runes[startIdx:endIdx]))
}

// builtinLength implements LENGTH(value)
// Returns the length of a string in characters, or blob in bytes.
// For numbers, converts to string first.
func builtinLength(args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewNull()
	}

	val := args[0]
	if val.IsNull() {
		return types.NewNull()
	}

	switch val.Type() {
	case types.TypeText:
		// For text, count Unicode characters (runes)
		runes := []rune(val.Text())
		return types.NewInt(int64(len(runes)))

	case types.TypeBlob:
		// For blobs, count bytes
		return types.NewInt(int64(len(val.Blob())))

	case types.TypeInt:
		// Convert to string and count characters
		s := fmt.Sprintf("%d", val.Int())
		return types.NewInt(int64(len(s)))

	case types.TypeFloat:
		// Convert to string and count characters
		s := fmt.Sprintf("%g", val.Float())
		return types.NewInt(int64(len(s)))

	default:
		return types.NewNull()
	}
}

// builtinUpper implements UPPER(string)
// Converts string to uppercase using Unicode case folding.
func builtinUpper(args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewNull()
	}

	val := args[0]
	if val.IsNull() {
		return types.NewNull()
	}

	if val.Type() != types.TypeText {
		return types.NewNull()
	}

	return types.NewText(strings.ToUpper(val.Text()))
}

// builtinLower implements LOWER(string)
// Converts string to lowercase using Unicode case folding.
func builtinLower(args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewNull()
	}

	val := args[0]
	if val.IsNull() {
		return types.NewNull()
	}

	if val.Type() != types.TypeText {
		return types.NewNull()
	}

	return types.NewText(strings.ToLower(val.Text()))
}

// builtinCoalesce implements COALESCE(val1, val2, ...)
// Returns the first non-NULL argument, or NULL if all arguments are NULL.
func builtinCoalesce(args []types.Value) types.Value {
	for _, arg := range args {
		if !arg.IsNull() {
			return arg
		}
	}
	return types.NewNull()
}

// builtinAbs implements ABS(value)
// Returns the absolute value of a number.
func builtinAbs(args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewNull()
	}

	val := args[0]
	if val.IsNull() {
		return types.NewNull()
	}

	switch val.Type() {
	case types.TypeInt:
		i := val.Int()
		if i < 0 {
			i = -i
		}
		return types.NewInt(i)

	case types.TypeFloat:
		return types.NewFloat(math.Abs(val.Float()))

	default:
		return types.NewNull()
	}
}

// builtinRound implements ROUND(value[, decimals])
// Rounds a number to the specified number of decimal places.
// Uses banker's rounding (round half away from zero for SQLite compatibility).
func builtinRound(args []types.Value) types.Value {
	if len(args) < 1 || len(args) > 2 {
		return types.NewNull()
	}

	// Check for NULL in any argument
	for _, arg := range args {
		if arg.IsNull() {
			return types.NewNull()
		}
	}

	// Get the value to round
	var val float64
	switch args[0].Type() {
	case types.TypeInt:
		val = float64(args[0].Int())
	case types.TypeFloat:
		val = args[0].Float()
	default:
		return types.NewNull()
	}

	// Get number of decimal places (default 0)
	decimals := int64(0)
	if len(args) == 2 {
		switch args[1].Type() {
		case types.TypeInt:
			decimals = args[1].Int()
		case types.TypeFloat:
			decimals = int64(args[1].Float())
		default:
			return types.NewNull()
		}
	}

	// Calculate multiplier
	multiplier := math.Pow(10, float64(decimals))

	// Round using SQLite-style rounding (away from zero for .5)
	rounded := math.Round(val * multiplier) / multiplier

	return types.NewFloat(rounded)
}

// builtinVectorDistance implements VECTOR_DISTANCE(vec1, vec2)
// Computes the cosine distance between two vectors.
// Accepts Vector values or Blob values containing serialized vectors.
// Returns REAL (float64) distance value.
func builtinVectorDistance(args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewNull()
	}

	// Check for NULL arguments
	if args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	// Extract vectors from arguments (either Vector type or Blob)
	vec1, err := extractVector(args[0])
	if err != nil {
		return types.NewNull()
	}

	vec2, err := extractVector(args[1])
	if err != nil {
		return types.NewNull()
	}

	// Compute cosine distance and return as REAL
	distance := vec1.CosineDistance(vec2)
	return types.NewFloat(float64(distance))
}

// extractVector extracts a Vector from a Value.
// Supports TypeVector and TypeBlob (deserializes from blob).
func extractVector(val types.Value) (*types.Vector, error) {
	switch val.Type() {
	case types.TypeVector:
		return val.Vector(), nil
	case types.TypeBlob:
		return types.VectorFromBytes(val.Blob())
	default:
		return nil, fmt.Errorf("unsupported type for vector: %v", val.Type())
	}
}
