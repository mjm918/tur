// pkg/vdbe/function.go
// Scalar function registry for the VDBE.
package vdbe

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
	"unicode"

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

	// Register CONCAT function (variadic)
	r.Register(&ScalarFunction{
		Name:     "CONCAT",
		NumArgs:  -1,
		Function: builtinConcat,
	})

	// Register CONCAT_WS function (variadic)
	r.Register(&ScalarFunction{
		Name:     "CONCAT_WS",
		NumArgs:  -1,
		Function: builtinConcatWS,
	})

	// Register TRIM function (1 or 2 arguments)
	r.Register(&ScalarFunction{
		Name:     "TRIM",
		NumArgs:  -1,
		Function: builtinTrim,
	})

	// Register LTRIM function (1 or 2 arguments)
	r.Register(&ScalarFunction{
		Name:     "LTRIM",
		NumArgs:  -1,
		Function: builtinLTrim,
	})

	// Register RTRIM function (1 or 2 arguments)
	r.Register(&ScalarFunction{
		Name:     "RTRIM",
		NumArgs:  -1,
		Function: builtinRTrim,
	})

	// Register LEFT function
	r.Register(&ScalarFunction{
		Name:     "LEFT",
		NumArgs:  2,
		Function: builtinLeft,
	})

	// Register RIGHT function
	r.Register(&ScalarFunction{
		Name:     "RIGHT",
		NumArgs:  2,
		Function: builtinRight,
	})

	// Register REPEAT function
	r.Register(&ScalarFunction{
		Name:     "REPEAT",
		NumArgs:  2,
		Function: builtinRepeat,
	})

	// Register SPACE function
	r.Register(&ScalarFunction{
		Name:     "SPACE",
		NumArgs:  1,
		Function: builtinSpace,
	})

	// Register REPLACE function
	r.Register(&ScalarFunction{
		Name:     "REPLACE",
		NumArgs:  3,
		Function: builtinReplace,
	})

	// Register REVERSE function
	r.Register(&ScalarFunction{
		Name:     "REVERSE",
		NumArgs:  1,
		Function: builtinReverse,
	})

	// Register INITCAP function
	r.Register(&ScalarFunction{
		Name:     "INITCAP",
		NumArgs:  1,
		Function: builtinInitcap,
	})

	// Register QUOTE function
	r.Register(&ScalarFunction{
		Name:     "QUOTE",
		NumArgs:  1,
		Function: builtinQuote,
	})

	// Register LPAD function
	r.Register(&ScalarFunction{
		Name:     "LPAD",
		NumArgs:  3,
		Function: builtinLPad,
	})

	// Register RPAD function
	r.Register(&ScalarFunction{
		Name:     "RPAD",
		NumArgs:  3,
		Function: builtinRPad,
	})

	// Register POSITION function
	r.Register(&ScalarFunction{
		Name:     "POSITION",
		NumArgs:  2,
		Function: builtinPosition,
	})

	// Register INSTR function (alias for POSITION)
	r.Register(&ScalarFunction{
		Name:     "INSTR",
		NumArgs:  2,
		Function: builtinPosition,
	})

	// Register ASCII function
	r.Register(&ScalarFunction{
		Name:     "ASCII",
		NumArgs:  1,
		Function: builtinASCII,
	})

	// Register CHR function
	r.Register(&ScalarFunction{
		Name:     "CHR",
		NumArgs:  1,
		Function: builtinCHR,
	})

	// Register CHAR function (alias for CHR)
	r.Register(&ScalarFunction{
		Name:     "CHAR",
		NumArgs:  1,
		Function: builtinCHR,
	})

	// Register MOD function
	r.Register(&ScalarFunction{
		Name:     "MOD",
		NumArgs:  2,
		Function: builtinMod,
	})

	// Register POWER function
	r.Register(&ScalarFunction{
		Name:     "POWER",
		NumArgs:  2,
		Function: builtinPower,
	})

	// Register POW function (alias for POWER)
	r.Register(&ScalarFunction{
		Name:     "POW",
		NumArgs:  2,
		Function: builtinPower,
	})

	// Register SQRT function
	r.Register(&ScalarFunction{
		Name:     "SQRT",
		NumArgs:  1,
		Function: builtinSqrt,
	})

	// Register EXP function
	r.Register(&ScalarFunction{
		Name:     "EXP",
		NumArgs:  1,
		Function: builtinExp,
	})

	// Register LN function
	r.Register(&ScalarFunction{
		Name:     "LN",
		NumArgs:  1,
		Function: builtinLn,
	})

	// Register LOG10 function
	r.Register(&ScalarFunction{
		Name:     "LOG10",
		NumArgs:  1,
		Function: builtinLog10,
	})

	// Register LOG function (1 or 2 arguments)
	r.Register(&ScalarFunction{
		Name:     "LOG",
		NumArgs:  -1, // Variadic: 1 or 2 arguments
		Function: builtinLog,
	})

	// Register CEIL function
	r.Register(&ScalarFunction{
		Name:     "CEIL",
		NumArgs:  1,
		Function: builtinCeil,
	})

	// Register CEILING function (alias for CEIL)
	r.Register(&ScalarFunction{
		Name:     "CEILING",
		NumArgs:  1,
		Function: builtinCeil,
	})

	// Register FLOOR function
	r.Register(&ScalarFunction{
		Name:     "FLOOR",
		NumArgs:  1,
		Function: builtinFloor,
	})

	// Register TRUNC function
	r.Register(&ScalarFunction{
		Name:     "TRUNC",
		NumArgs:  -1, // 1 or 2 arguments
		Function: builtinTrunc,
	})

	// Register TRUNCATE function (alias for TRUNC)
	r.Register(&ScalarFunction{
		Name:     "TRUNCATE",
		NumArgs:  -1, // 1 or 2 arguments
		Function: builtinTrunc,
	})

	// Register FORMAT function
	r.Register(&ScalarFunction{
		Name:     "FORMAT",
		NumArgs:  -1, // 2 or 3 arguments
		Function: builtinFormat,
	})

	// Register NOW function
	r.Register(&ScalarFunction{
		Name:     "NOW",
		NumArgs:  0,
		Function: builtinNow,
	})

	// Register CURRENT_TIMESTAMP function (alias for NOW)
	r.Register(&ScalarFunction{
		Name:     "CURRENT_TIMESTAMP",
		NumArgs:  0,
		Function: builtinNow,
	})

	// Register CURRENT_DATE function
	r.Register(&ScalarFunction{
		Name:     "CURRENT_DATE",
		NumArgs:  0,
		Function: builtinCurrentDate,
	})

	// Register CURRENT_TIME function
	r.Register(&ScalarFunction{
		Name:     "CURRENT_TIME",
		NumArgs:  0,
		Function: builtinCurrentTime,
	})

	// Register LOCALTIME function
	r.Register(&ScalarFunction{
		Name:     "LOCALTIME",
		NumArgs:  0,
		Function: builtinLocaltime,
	})

	// Register LOCALTIMESTAMP function
	r.Register(&ScalarFunction{
		Name:     "LOCALTIMESTAMP",
		NumArgs:  0,
		Function: builtinLocaltimestamp,
	})

	// Register SIGN function
	r.Register(&ScalarFunction{
		Name:     "SIGN",
		NumArgs:  1,
		Function: builtinSign,
	})

	// Register GREATEST function (variadic)
	r.Register(&ScalarFunction{
		Name:     "GREATEST",
		NumArgs:  -1,
		Function: builtinGreatest,
	})

	// Register LEAST function (variadic)
	r.Register(&ScalarFunction{
		Name:     "LEAST",
		NumArgs:  -1,
		Function: builtinLeast,
	})

	// Register RANDOM function
	r.Register(&ScalarFunction{
		Name:     "RANDOM",
		NumArgs:  0,
		Function: builtinRandom,
	})

	// Register TO_CHAR function
	r.Register(&ScalarFunction{
		Name:     "TO_CHAR",
		NumArgs:  2,
		Function: builtinToChar,
	})

	// Register DATE_ADD function
	r.Register(&ScalarFunction{
		Name:     "DATE_ADD",
		NumArgs:  2,
		Function: builtinDateAdd,
	})

	// Register DATE_SUB function
	r.Register(&ScalarFunction{
		Name:     "DATE_SUB",
		NumArgs:  2,
		Function: builtinDateSub,
	})

	// Register DATEDIFF function
	r.Register(&ScalarFunction{
		Name:     "DATEDIFF",
		NumArgs:  2,
		Function: builtinDateDiff,
	})

	// Register YEAR function
	r.Register(&ScalarFunction{
		Name:     "YEAR",
		NumArgs:  1,
		Function: builtinYear,
	})

	// Register MONTH function
	r.Register(&ScalarFunction{
		Name:     "MONTH",
		NumArgs:  1,
		Function: builtinMonth,
	})

	// Register DAY function
	r.Register(&ScalarFunction{
		Name:     "DAY",
		NumArgs:  1,
		Function: builtinDay,
	})

	// Register HOUR function
	r.Register(&ScalarFunction{
		Name:     "HOUR",
		NumArgs:  1,
		Function: builtinHour,
	})

	// Register MINUTE function
	r.Register(&ScalarFunction{
		Name:     "MINUTE",
		NumArgs:  1,
		Function: builtinMinute,
	})

	// Register SECOND function
	r.Register(&ScalarFunction{
		Name:     "SECOND",
		NumArgs:  1,
		Function: builtinSecond,
	})

	// Register TO_DATE function
	r.Register(&ScalarFunction{
		Name:     "TO_DATE",
		NumArgs:  2,
		Function: builtinToDate,
	})

	// Register TO_TIMESTAMP function
	r.Register(&ScalarFunction{
		Name:     "TO_TIMESTAMP",
		NumArgs:  2,
		Function: builtinToTimestamp,
	})

	// Register AGE function (variadic: 1 or 2 arguments)
	r.Register(&ScalarFunction{
		Name:     "AGE",
		NumArgs:  -1,
		Function: builtinAge,
	})

	// Register DATE_TRUNC function
	r.Register(&ScalarFunction{
		Name:     "DATE_TRUNC",
		NumArgs:  2,
		Function: builtinDateTrunc,
	})

	// Register EXTRACT function
	r.Register(&ScalarFunction{
		Name:     "EXTRACT",
		NumArgs:  2,
		Function: builtinExtract,
	})

	// Register DATE_PART function (alias for EXTRACT)
	r.Register(&ScalarFunction{
		Name:     "DATE_PART",
		NumArgs:  2,
		Function: builtinDatePart,
	})

	// Register IF function (conditional)
	r.Register(&ScalarFunction{
		Name:     "IF",
		NumArgs:  3,
		Function: builtinIf,
	})

	// Register IFNULL function
	r.Register(&ScalarFunction{
		Name:     "IFNULL",
		NumArgs:  2,
		Function: builtinIfNull,
	})

	// Register NULLIF function
	r.Register(&ScalarFunction{
		Name:     "NULLIF",
		NumArgs:  2,
		Function: builtinNullIf,
	})

	// Register JSON functions
	RegisterJSONFunctions(r)

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

// valueToString converts a Value to its string representation.
// Used by CONCAT and CONCAT_WS functions.
func valueToString(v types.Value) string {
	switch v.Type() {
	case types.TypeText:
		return v.Text()
	case types.TypeInt:
		return fmt.Sprintf("%d", v.Int())
	case types.TypeFloat:
		return fmt.Sprintf("%g", v.Float())
	default:
		return ""
	}
}

// builtinConcat implements CONCAT(val1, val2, ...)
// Concatenates all non-NULL arguments into a single string.
// NULL values are skipped.
func builtinConcat(args []types.Value) types.Value {
	var sb strings.Builder
	for _, arg := range args {
		if arg.IsNull() {
			continue
		}
		sb.WriteString(valueToString(arg))
	}
	return types.NewText(sb.String())
}

// builtinConcatWS implements CONCAT_WS(separator, val1, val2, ...)
// Concatenates all non-NULL arguments with the given separator.
// If separator is NULL, returns NULL.
// NULL values in the arguments are skipped.
func builtinConcatWS(args []types.Value) types.Value {
	if len(args) < 1 {
		return types.NewNull()
	}
	if args[0].IsNull() {
		return types.NewNull()
	}
	sep := args[0].Text()

	var parts []string
	for _, arg := range args[1:] {
		if arg.IsNull() {
			continue
		}
		parts = append(parts, valueToString(arg))
	}
	return types.NewText(strings.Join(parts, sep))
}

// builtinTrim implements TRIM(string[, chars])
// Removes leading and trailing characters from a string.
// If chars is not specified, removes whitespace (space, tab, newline, carriage return).
// If chars is specified, removes those characters.
func builtinTrim(args []types.Value) types.Value {
	if len(args) < 1 || args[0].IsNull() {
		return types.NewNull()
	}
	str := args[0].Text()
	if len(args) >= 2 && !args[1].IsNull() {
		return types.NewText(strings.Trim(str, args[1].Text()))
	}
	return types.NewText(strings.TrimSpace(str))
}

// builtinLTrim implements LTRIM(string[, chars])
// Removes leading characters from a string.
// If chars is not specified, removes whitespace (space, tab, newline, carriage return).
// If chars is specified, removes those characters.
func builtinLTrim(args []types.Value) types.Value {
	if len(args) < 1 || args[0].IsNull() {
		return types.NewNull()
	}
	str := args[0].Text()
	if len(args) >= 2 && !args[1].IsNull() {
		return types.NewText(strings.TrimLeft(str, args[1].Text()))
	}
	return types.NewText(strings.TrimLeft(str, " \t\n\r"))
}

// builtinRTrim implements RTRIM(string[, chars])
// Removes trailing characters from a string.
// If chars is not specified, removes whitespace (space, tab, newline, carriage return).
// If chars is specified, removes those characters.
func builtinRTrim(args []types.Value) types.Value {
	if len(args) < 1 || args[0].IsNull() {
		return types.NewNull()
	}
	str := args[0].Text()
	if len(args) >= 2 && !args[1].IsNull() {
		return types.NewText(strings.TrimRight(str, args[1].Text()))
	}
	return types.NewText(strings.TrimRight(str, " \t\n\r"))
}

// builtinLeft implements LEFT(string, n)
// Returns the leftmost n characters from string.
func builtinLeft(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}
	runes := []rune(args[0].Text())
	n := int(args[1].Int())
	if n < 0 {
		n = 0
	}
	if n > len(runes) {
		n = len(runes)
	}
	return types.NewText(string(runes[:n]))
}

// builtinRight implements RIGHT(string, n)
// Returns the rightmost n characters from string.
func builtinRight(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}
	runes := []rune(args[0].Text())
	n := int(args[1].Int())
	if n < 0 {
		n = 0
	}
	if n > len(runes) {
		n = len(runes)
	}
	return types.NewText(string(runes[len(runes)-n:]))
}

// builtinRepeat implements REPEAT(string, n)
// Returns a string consisting of string repeated n times.
func builtinRepeat(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}
	n := int(args[1].Int())
	if n < 0 {
		n = 0
	}
	return types.NewText(strings.Repeat(args[0].Text(), n))
}

// builtinSpace implements SPACE(n)
// Returns a string consisting of n space characters.
func builtinSpace(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}
	n := int(args[0].Int())
	if n < 0 {
		n = 0
	}
	return types.NewText(strings.Repeat(" ", n))
}

// builtinReplace implements REPLACE(string, from, to)
// Replaces all occurrences of 'from' substring with 'to' substring.
// If any argument is NULL, returns NULL.
func builtinReplace(args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewNull()
	}
	for _, arg := range args {
		if arg.IsNull() {
			return types.NewNull()
		}
	}
	return types.NewText(strings.ReplaceAll(args[0].Text(), args[1].Text(), args[2].Text()))
}

// builtinReverse implements REVERSE(string)
// Reverses the characters in a string.
// Properly handles Unicode characters (runes).
// If argument is NULL, returns NULL.
func builtinReverse(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}
	runes := []rune(args[0].Text())
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return types.NewText(string(runes))
}

// builtinInitcap implements INITCAP(string)
// Converts the first letter of each word to uppercase and the rest to lowercase.
// A word is defined as a sequence of letters or numbers.
// If argument is NULL, returns NULL.
func builtinInitcap(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}
	str := args[0].Text()
	runes := []rune(strings.ToLower(str))
	inWord := false
	for i, r := range runes {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			if !inWord {
				runes[i] = unicode.ToUpper(r)
				inWord = true
			}
		} else {
			inWord = false
		}
	}
	return types.NewText(string(runes))
}

// builtinQuote implements QUOTE(value)
// Returns a string that is the value of the argument enclosed in single quotes.
// Single quotes within the string are escaped by doubling them.
// If argument is NULL, returns the string "NULL" (without quotes).
func builtinQuote(args []types.Value) types.Value {
	if len(args) != 1 {
		return types.NewNull()
	}
	if args[0].IsNull() {
		return types.NewText("NULL")
	}
	str := args[0].Text()
	escaped := strings.ReplaceAll(str, "'", "''")
	return types.NewText("'" + escaped + "'")
}

// builtinLPad implements LPAD(string, length, pad)
// Pads the left side of a string with a specified pad string until it reaches the desired length.
// If the string is already longer than the desired length, it is truncated.
// If pad is empty, returns the string as-is (potentially truncated).
// If any argument is NULL, returns NULL.
func builtinLPad(args []types.Value) types.Value {
	if len(args) < 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}
	runes := []rune(args[0].Text())
	length := int(args[1].Int())
	pad := " "
	if len(args) >= 3 && !args[2].IsNull() {
		pad = args[2].Text()
	}
	if pad == "" {
		return types.NewText(string(runes))
	}
	if length <= len(runes) {
		return types.NewText(string(runes[:length]))
	}
	padRunes := []rune(pad)
	needed := length - len(runes)
	var result []rune
	for i := 0; i < needed; i++ {
		result = append(result, padRunes[i%len(padRunes)])
	}
	result = append(result, runes...)
	return types.NewText(string(result))
}

// builtinRPad implements RPAD(string, length, pad)
// Pads the right side of a string with a specified pad string until it reaches the desired length.
// If the string is already longer than the desired length, it is truncated.
// If pad is empty, returns the string as-is (potentially truncated).
// If any argument is NULL, returns NULL.
func builtinRPad(args []types.Value) types.Value {
	if len(args) < 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}
	runes := []rune(args[0].Text())
	length := int(args[1].Int())
	pad := " "
	if len(args) >= 3 && !args[2].IsNull() {
		pad = args[2].Text()
	}
	if pad == "" {
		return types.NewText(string(runes))
	}
	if length <= len(runes) {
		return types.NewText(string(runes[:length]))
	}
	padRunes := []rune(pad)
	needed := length - len(runes)
	result := append([]rune{}, runes...)
	for i := 0; i < needed; i++ {
		result = append(result, padRunes[i%len(padRunes)])
	}
	return types.NewText(string(result))
}

// builtinPosition implements POSITION(substr, str)
// Returns the 1-based position of the first occurrence of substr in str.
// Returns 0 if substr is not found.
// Returns 1 if substr is empty (SQLite behavior).
// If any argument is NULL, returns NULL.
func builtinPosition(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}
	substr := args[0].Text()
	str := args[1].Text()
	
	// Empty substring returns 1 (SQLite behavior)
	if substr == "" {
		return types.NewInt(1)
	}
	
	idx := strings.Index(str, substr)
	if idx < 0 {
		return types.NewInt(0)
	}
	// Convert byte index to rune index (1-based)
	runeIdx := len([]rune(str[:idx])) + 1
	return types.NewInt(int64(runeIdx))
}

// builtinASCII implements ASCII(str)
// Returns the ASCII code of the first character in str.
// Returns 0 if str is empty.
// If argument is NULL, returns NULL.
func builtinASCII(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}
	str := args[0].Text()
	if len(str) == 0 {
		return types.NewInt(0)
	}
	return types.NewInt(int64(str[0]))
}

// builtinCHR implements CHR(code)
// Returns the character with the given ASCII/Unicode code.
// If argument is NULL, returns NULL.
func builtinCHR(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}
	code := args[0].Int()
	return types.NewText(string(rune(code)))
}

// builtinMod implements MOD(x, y)
// Returns the remainder of x divided by y.
// If any argument is NULL, returns NULL.
func builtinMod(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	var x, y float64
	xIsInt := types.IsIntegerType(args[0].Type())
	yIsInt := types.IsIntegerType(args[1].Type())

	// Convert first argument to float
	if xIsInt {
		x = float64(args[0].Int())
	} else if args[0].Type() == types.TypeFloat {
		x = args[0].Float()
	} else {
		return types.NewNull()
	}

	// Convert second argument to float
	if yIsInt {
		y = float64(args[1].Int())
	} else if args[1].Type() == types.TypeFloat {
		y = args[1].Float()
	} else {
		return types.NewNull()
	}

	// Check for division by zero
	if y == 0 {
		return types.NewNull()
	}

	// For integers, return integer result
	if xIsInt && yIsInt {
		result := int64(x) % int64(y)
		return types.NewInt32(int32(result))
	}

	// For floats, use math.Mod
	return types.NewFloat(math.Mod(x, y))
}

// builtinPower implements POWER(x, y)
// Returns x raised to the power of y.
// If any argument is NULL, returns NULL.
func builtinPower(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	var x, y float64

	// Convert first argument to float
	if types.IsIntegerType(args[0].Type()) {
		x = float64(args[0].Int())
	} else if args[0].Type() == types.TypeFloat {
		x = args[0].Float()
	} else {
		return types.NewNull()
	}

	// Convert second argument to float
	if types.IsIntegerType(args[1].Type()) {
		y = float64(args[1].Int())
	} else if args[1].Type() == types.TypeFloat {
		y = args[1].Float()
	} else {
		return types.NewNull()
	}

	return types.NewFloat(math.Pow(x, y))
}

// builtinSqrt implements SQRT(x)
// Returns the square root of x.
// If argument is NULL or negative, returns NULL.
func builtinSqrt(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}

	var x float64

	if types.IsIntegerType(args[0].Type()) {
		x = float64(args[0].Int())
	} else if args[0].Type() == types.TypeFloat {
		x = args[0].Float()
	} else {
		return types.NewNull()
	}

	// SQRT of negative number is NULL
	if x < 0 {
		return types.NewNull()
	}

	return types.NewFloat(math.Sqrt(x))
}

// builtinExp implements EXP(x)
// Returns e raised to the power of x (e^x).
// If argument is NULL, returns NULL.
func builtinExp(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}
	
	var x float64
	
	switch args[0].Type() {
	case types.TypeInt:
		x = float64(args[0].Int())
	case types.TypeFloat:
		x = args[0].Float()
	default:
		return types.NewNull()
	}
	
	return types.NewFloat(math.Exp(x))
}

// builtinLn implements LN(x)
// Returns the natural logarithm (base e) of x.
// If argument is NULL or x <= 0, returns NULL.
func builtinLn(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}
	
	var x float64
	
	switch args[0].Type() {
	case types.TypeInt:
		x = float64(args[0].Int())
	case types.TypeFloat:
		x = args[0].Float()
	default:
		return types.NewNull()
	}
	
	// LN of non-positive number is NULL
	if x <= 0 {
		return types.NewNull()
	}
	
	return types.NewFloat(math.Log(x))
}

// builtinLog10 implements LOG10(x)
// Returns the base-10 logarithm of x.
// If argument is NULL or x <= 0, returns NULL.
func builtinLog10(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}
	
	var x float64
	
	switch args[0].Type() {
	case types.TypeInt:
		x = float64(args[0].Int())
	case types.TypeFloat:
		x = args[0].Float()
	default:
		return types.NewNull()
	}
	
	// LOG10 of non-positive number is NULL
	if x <= 0 {
		return types.NewNull()
	}
	
	return types.NewFloat(math.Log10(x))
}

// builtinLog implements LOG(base, value) or LOG(value)
// With 2 args: Returns log_base(value) = ln(value) / ln(base)
// With 1 arg: Returns natural log (same as LN)
// If any argument is NULL, or base/value <= 0, or base = 1, returns NULL.
func builtinLog(args []types.Value) types.Value {
	if len(args) < 1 || len(args) > 2 {
		return types.NewNull()
	}
	
	// Check for NULL arguments
	for _, arg := range args {
		if arg.IsNull() {
			return types.NewNull()
		}
	}
	
	// LOG with 1 arg: natural log
	if len(args) == 1 {
		return builtinLn(args)
	}
	
	// LOG with 2 args: LOG(base, value)
	var base, value float64
	
	// Convert base to float
	switch args[0].Type() {
	case types.TypeInt:
		base = float64(args[0].Int())
	case types.TypeFloat:
		base = args[0].Float()
	default:
		return types.NewNull()
	}
	
	// Convert value to float
	switch args[1].Type() {
	case types.TypeInt:
		value = float64(args[1].Int())
	case types.TypeFloat:
		value = args[1].Float()
	default:
		return types.NewNull()
	}
	
	// Check for invalid inputs
	if base <= 0 || base == 1 || value <= 0 {
		return types.NewNull()
	}
	
	// log_base(value) = ln(value) / ln(base)
	return types.NewFloat(math.Log(value) / math.Log(base))
}

// builtinCeil implements CEIL(value)
// Returns the smallest integer value greater than or equal to value.
// Uses math.Ceil for the calculation.
// If argument is NULL, returns NULL.
func builtinCeil(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}

	// Get the value, converting to float64
	var val float64
	switch args[0].Type() {
	case types.TypeInt:
		val = float64(args[0].Int())
	case types.TypeFloat:
		val = args[0].Float()
	default:
		return types.NewNull()
	}

	// Calculate ceiling and return as float
	result := math.Ceil(val)
	return types.NewFloat(result)
}

// builtinFloor implements FLOOR(value)
// Returns the largest integer value less than or equal to value.
// Uses math.Floor for the calculation.
// If argument is NULL, returns NULL.
func builtinFloor(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}

	// Get the value, converting to float64
	var val float64
	switch args[0].Type() {
	case types.TypeInt:
		val = float64(args[0].Int())
	case types.TypeFloat:
		val = args[0].Float()
	default:
		return types.NewNull()
	}

	// Calculate floor and return as float
	result := math.Floor(val)
	return types.NewFloat(result)
}

// builtinTrunc implements TRUNC(value[, decimals])
// Truncates a number to the specified number of decimal places.
// If decimals is not specified, truncates to 0 decimal places (integer part).
// Negative decimals truncate to the left of the decimal point.
// Uses math.Trunc for the basic truncation.
// If any argument is NULL, returns NULL.
func builtinTrunc(args []types.Value) types.Value {
	if len(args) < 1 || len(args) > 2 {
		return types.NewNull()
	}

	// Check for NULL arguments
	for _, arg := range args {
		if arg.IsNull() {
			return types.NewNull()
		}
	}

	// Get the value to truncate
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

	// Truncate using math.Trunc
	truncated := math.Trunc(val*multiplier) / multiplier

	return types.NewFloat(truncated)
}

// localeFormat defines the thousand separator and decimal point for a locale
type localeFormat struct {
	thousand string
	decimal  string
}

// localeFormats maps locale identifiers to their formatting rules
var localeFormats = map[string]localeFormat{
	"":      {",", "."},     // Default (en_US)
	"en_US": {",", "."},     // English (US)
	"de_DE": {".", ","},     // German (Germany)
	"fr_FR": {" ", ","},     // French (France)
	"es_ES": {".", ","},     // Spanish (Spain)
}

// builtinFormat implements FORMAT(number, decimals[, locale])
// Formats a number with thousand separators and specified decimal places.
// If locale is not specified or not recognized, uses default (en_US) formatting.
// If any required argument is NULL, returns NULL.
func builtinFormat(args []types.Value) types.Value {
	if len(args) < 2 || len(args) > 3 {
		return types.NewNull()
	}

	// Check for NULL in required arguments
	if args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	// Get the number to format
	var number float64
	switch args[0].Type() {
	case types.TypeInt:
		number = float64(args[0].Int())
	case types.TypeFloat:
		number = args[0].Float()
	default:
		return types.NewNull()
	}

	// Get number of decimal places
	var decimals int64
	switch args[1].Type() {
	case types.TypeInt:
		decimals = args[1].Int()
	case types.TypeFloat:
		decimals = int64(args[1].Float())
	default:
		return types.NewNull()
	}

	// Ensure decimals is not negative
	if decimals < 0 {
		decimals = 0
	}

	// Get locale format (default to en_US)
	locale := ""
	if len(args) == 3 && !args[2].IsNull() {
		locale = args[2].Text()
	}

	format, ok := localeFormats[locale]
	if !ok {
		// Unknown locale, use default
		format = localeFormats[""]
	}

	// Round the number to the specified decimal places
	multiplier := math.Pow(10, float64(decimals))
	rounded := math.Round(number * multiplier) / multiplier

	// Handle sign
	sign := ""
	if rounded < 0 {
		sign = "-"
		rounded = -rounded
	}

	// Split into integer and fractional parts
	intPart := int64(rounded)
	fracPart := rounded - float64(intPart)

	// Format integer part with thousand separators
	intStr := fmt.Sprintf("%d", intPart)
	var intFormatted strings.Builder

	// Add thousand separators from right to left
	for i, digit := range intStr {
		if i > 0 && (len(intStr)-i)%3 == 0 {
			intFormatted.WriteString(format.thousand)
		}
		intFormatted.WriteRune(digit)
	}

	// Format fractional part
	var result strings.Builder
	result.WriteString(sign)
	result.WriteString(intFormatted.String())

	if decimals > 0 {
		result.WriteString(format.decimal)
		// Format fractional part with specified decimal places
		fracStr := fmt.Sprintf("%0*d", int(decimals), int64(fracPart*multiplier+0.5))
		result.WriteString(fracStr)
	}

	return types.NewText(result.String())
}

// builtinNow implements NOW() and CURRENT_TIMESTAMP
// Returns the current date and time as TIMESTAMPTZ (in UTC).
func builtinNow(args []types.Value) types.Value {
	return types.NewTimestampTZ(time.Now())
}

// builtinCurrentDate implements CURRENT_DATE
// Returns the current date as DATE type.
func builtinCurrentDate(args []types.Value) types.Value {
	now := time.Now()
	return types.NewDate(now.Year(), int(now.Month()), now.Day())
}

// builtinCurrentTime implements CURRENT_TIME
// Returns the current time with timezone as TIMETZ type.
func builtinCurrentTime(args []types.Value) types.Value {
	now := time.Now()
	_, offset := now.Zone()
	return types.NewTimeTZ(now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1000, offset)
}

// builtinLocaltime implements LOCALTIME
// Returns the current local date and time as TIMESTAMP (without timezone).
// The local time components are preserved even though stored in UTC internally.
func builtinLocaltime(args []types.Value) types.Value {
	now := time.Now()
	// Get the local time and store its components as-is
	// Even though NewTimestamp stores in UTC, the time components represent local time
	local := now.Local()
	return types.NewTimestamp(local.Year(), int(local.Month()), local.Day(), local.Hour(), local.Minute(), local.Second(), local.Nanosecond()/1000)
}

// builtinLocaltimestamp implements LOCALTIMESTAMP
// Returns the current local date and time as TIMESTAMP (without timezone).
// The local time components are preserved even though stored in UTC internally.
func builtinLocaltimestamp(args []types.Value) types.Value {
	now := time.Now()
	// Get the local time and store its components as-is
	// Even though NewTimestamp stores in UTC, the time components represent local time
	local := now.Local()
	return types.NewTimestamp(local.Year(), int(local.Month()), local.Day(), local.Hour(), local.Minute(), local.Second(), local.Nanosecond()/1000)
}

// builtinSign implements SIGN(x)
// Returns the sign of a number:
// -1 if x < 0
//  0 if x = 0
//  1 if x > 0
// If argument is NULL, returns NULL.
func builtinSign(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}

	var x float64

	if types.IsIntegerType(args[0].Type()) {
		x = float64(args[0].Int())
	} else if args[0].Type() == types.TypeFloat {
		x = args[0].Float()
	} else {
		return types.NewNull()
	}

	if x < 0 {
		return types.NewInt32(-1)
	} else if x > 0 {
		return types.NewInt32(1)
	}
	return types.NewInt32(0)
}

// builtinGreatest implements GREATEST(val1, val2, ...)
// Returns the greatest (maximum) value from the arguments.
// NULL values are skipped. If all arguments are NULL, returns NULL.
// Uses types.Compare for comparison.
func builtinGreatest(args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewNull()
	}

	var greatest types.Value
	found := false

	for _, arg := range args {
		if arg.IsNull() {
			continue
		}
		if !found {
			greatest = arg
			found = true
		} else {
			if types.Compare(arg, greatest) > 0 {
				greatest = arg
			}
		}
	}

	if !found {
		return types.NewNull()
	}
	return greatest
}

// builtinLeast implements LEAST(val1, val2, ...)
// Returns the least (minimum) value from the arguments.
// NULL values are skipped. If all arguments are NULL, returns NULL.
// Uses types.Compare for comparison.
func builtinLeast(args []types.Value) types.Value {
	if len(args) == 0 {
		return types.NewNull()
	}

	var least types.Value
	found := false

	for _, arg := range args {
		if arg.IsNull() {
			continue
		}
		if !found {
			least = arg
			found = true
		} else {
			if types.Compare(arg, least) < 0 {
				least = arg
			}
		}
	}

	if !found {
		return types.NewNull()
	}
	return least
}

// builtinRandom implements RANDOM()
// Returns a random floating-point number in the range [0, 1).
// Uses math/rand for random number generation.
func builtinRandom(args []types.Value) types.Value {
	return types.NewFloat(rand.Float64())
}

// builtinToChar implements TO_CHAR(value, format)
// Formats date/time values according to PostgreSQL-style format codes.
// Supported format codes:
// - YYYY: 4-digit year
// - MM: 2-digit month (01-12)
// - DD: 2-digit day of month (01-31)
// - HH24: 24-hour format (00-23)
// - HH12, HH: 12-hour format (01-12)
// - MI: minutes (00-59)
// - SS: seconds (00-59)
// - Mon: abbreviated month name (Jan, Feb, etc.)
// - Day: weekday name (Monday, Tuesday, etc.)
// If any argument is NULL, returns NULL.
func builtinToChar(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	format := args[1].Text()
	val := args[0]

	var t time.Time

	// Extract time.Time from the value based on its type
	switch val.Type() {
	case types.TypeDate:
		year, month, day := val.DateValue()
		t = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	case types.TypeTimestamp:
		t = val.TimestampValue()
	case types.TypeTimestampTZ:
		t = val.TimestampTZValue()
	case types.TypeTime:
		hour, minute, second, microsecond := val.TimeValue()
		// For TIME, use a reference date (2000-01-01)
		t = time.Date(2000, 1, 1, hour, minute, second, microsecond*1000, time.UTC)
	case types.TypeTimeTZ:
		hour, minute, second, microsecond, _ := val.TimeTZValue()
		// For TIMETZ, use a reference date (2000-01-01)
		t = time.Date(2000, 1, 1, hour, minute, second, microsecond*1000, time.UTC)
	default:
		return types.NewNull()
	}

	// Format the value according to the format string
	result := formatDateTime(t, format)
	return types.NewText(result)
}

// formatDateTime formats a time.Time according to PostgreSQL-style format codes
func formatDateTime(t time.Time, format string) string {
	var result strings.Builder
	i := 0
	for i < len(format) {
		// Check for format codes (longest match first)
		matched := false

		// 4-character patterns
		if i+4 <= len(format) {
			code := format[i : i+4]
			switch code {
			case "YYYY":
				result.WriteString(fmt.Sprintf("%04d", t.Year()))
				i += 4
				matched = true
			case "HH24":
				result.WriteString(fmt.Sprintf("%02d", t.Hour()))
				i += 4
				matched = true
			case "HH12":
				hour := t.Hour() % 12
				if hour == 0 {
					hour = 12
				}
				result.WriteString(fmt.Sprintf("%02d", hour))
				i += 4
				matched = true
			}
		}

		if !matched && i+3 <= len(format) {
			code := format[i : i+3]
			switch code {
			case "Mon":
				monthNames := []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
				result.WriteString(monthNames[t.Month()-1])
				i += 3
				matched = true
			case "Day":
				result.WriteString(t.Weekday().String())
				i += 3
				matched = true
			}
		}

		if !matched && i+2 <= len(format) {
			code := format[i : i+2]
			switch code {
			case "MM":
				result.WriteString(fmt.Sprintf("%02d", t.Month()))
				i += 2
				matched = true
			case "DD":
				result.WriteString(fmt.Sprintf("%02d", t.Day()))
				i += 2
				matched = true
			case "HH":
				hour := t.Hour() % 12
				if hour == 0 {
					hour = 12
				}
				result.WriteString(fmt.Sprintf("%02d", hour))
				i += 2
				matched = true
			case "MI":
				result.WriteString(fmt.Sprintf("%02d", t.Minute()))
				i += 2
				matched = true
			case "SS":
				result.WriteString(fmt.Sprintf("%02d", t.Second()))
				i += 2
				matched = true
			}
		}

		// If no pattern matched, copy the character as-is
		if !matched {
			result.WriteByte(format[i])
			i++
		}
	}

	return result.String()
}

// builtinDateAdd implements DATE_ADD(date, interval)
// Adds an interval to a date value.
// Returns a DATE value representing the result of date + interval.
// If any argument is NULL, returns NULL.
func builtinDateAdd(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	// First argument must be a DATE
	if args[0].Type() != types.TypeDate {
		return types.NewNull()
	}

	// Second argument must be an INTERVAL
	if args[1].Type() != types.TypeInterval {
		return types.NewNull()
	}

	// Get the date components
	year, month, day := args[0].DateValue()

	// Get the interval components
	months, microseconds := args[1].IntervalValue()

	// Create a time.Time for date arithmetic
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)

	// Add months using AddDate
	if months != 0 {
		t = t.AddDate(0, int(months), 0)
	}

	// Add microseconds (days + time) using Add
	if microseconds != 0 {
		t = t.Add(time.Duration(microseconds) * time.Microsecond)
	}

	// Return the result as a DATE
	return types.NewDate(t.Year(), int(t.Month()), t.Day())
}

// builtinDateSub implements DATE_SUB(date, interval)
// Subtracts an interval from a date value.
// Returns a DATE value representing the result of date - interval.
// If any argument is NULL, returns NULL.
func builtinDateSub(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	// First argument must be a DATE
	if args[0].Type() != types.TypeDate {
		return types.NewNull()
	}

	// Second argument must be an INTERVAL
	if args[1].Type() != types.TypeInterval {
		return types.NewNull()
	}

	// Get the date components
	year, month, day := args[0].DateValue()

	// Get the interval components
	months, microseconds := args[1].IntervalValue()

	// Create a time.Time for date arithmetic
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)

	// Subtract months using AddDate with negative values
	if months != 0 {
		t = t.AddDate(0, -int(months), 0)
	}

	// Subtract microseconds (days + time) using Add with negative duration
	if microseconds != 0 {
		t = t.Add(-time.Duration(microseconds) * time.Microsecond)
	}

	// Return the result as a DATE
	return types.NewDate(t.Year(), int(t.Month()), t.Day())
}

// builtinDateDiff implements DATEDIFF(date1, date2)
// Returns the number of days between date1 and date2 (date1 - date2).
// Returns a positive number if date1 > date2, negative if date1 < date2.
// If any argument is NULL, returns NULL.
func builtinDateDiff(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	// Both arguments must be DATE
	if args[0].Type() != types.TypeDate || args[1].Type() != types.TypeDate {
		return types.NewNull()
	}

	// Get the date components for both dates
	year1, month1, day1 := args[0].DateValue()
	year2, month2, day2 := args[1].DateValue()

	// Create time.Time values for both dates
	t1 := time.Date(year1, time.Month(month1), day1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(year2, time.Month(month2), day2, 0, 0, 0, 0, time.UTC)

	// Calculate the difference in days
	diff := t1.Sub(t2)
	days := int64(diff.Hours() / 24)

	return types.NewInt(days)
}

// builtinYear implements YEAR(date_value)
// Extracts the year from a DATE, TIMESTAMP, or TIMESTAMPTZ value.
// Returns the year as an INTEGER.
// If argument is NULL or not a date/timestamp type, returns NULL.
func builtinYear(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}

	val := args[0]
	switch val.Type() {
	case types.TypeDate:
		year, _, _ := val.DateValue()
		return types.NewInt(int64(year))
	case types.TypeTimestamp:
		t := val.TimestampValue()
		return types.NewInt(int64(t.Year()))
	case types.TypeTimestampTZ:
		t := val.TimestampTZValue()
		return types.NewInt(int64(t.Year()))
	default:
		return types.NewNull()
	}
}

// builtinMonth implements MONTH(date_value)
// Extracts the month from a DATE, TIMESTAMP, or TIMESTAMPTZ value.
// Returns the month as an INTEGER (1-12).
// If argument is NULL or not a date/timestamp type, returns NULL.
func builtinMonth(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}

	val := args[0]
	switch val.Type() {
	case types.TypeDate:
		_, month, _ := val.DateValue()
		return types.NewInt(int64(month))
	case types.TypeTimestamp:
		t := val.TimestampValue()
		return types.NewInt(int64(t.Month()))
	case types.TypeTimestampTZ:
		t := val.TimestampTZValue()
		return types.NewInt(int64(t.Month()))
	default:
		return types.NewNull()
	}
}

// builtinDay implements DAY(date_value)
// Extracts the day of month from a DATE, TIMESTAMP, or TIMESTAMPTZ value.
// Returns the day as an INTEGER (1-31).
// If argument is NULL or not a date/timestamp type, returns NULL.
func builtinDay(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}

	val := args[0]
	switch val.Type() {
	case types.TypeDate:
		_, _, day := val.DateValue()
		return types.NewInt(int64(day))
	case types.TypeTimestamp:
		t := val.TimestampValue()
		return types.NewInt(int64(t.Day()))
	case types.TypeTimestampTZ:
		t := val.TimestampTZValue()
		return types.NewInt(int64(t.Day()))
	default:
		return types.NewNull()
	}
}

// builtinHour implements HOUR(time_value)
// Extracts the hour from a TIME, TIMETZ, TIMESTAMP, or TIMESTAMPTZ value.
// Returns the hour as an INTEGER (0-23).
// If argument is NULL or not a time/timestamp type, returns NULL.
func builtinHour(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}

	val := args[0]
	switch val.Type() {
	case types.TypeTime:
		hour, _, _, _ := val.TimeValue()
		return types.NewInt(int64(hour))
	case types.TypeTimeTZ:
		hour, _, _, _, _ := val.TimeTZValue()
		return types.NewInt(int64(hour))
	case types.TypeTimestamp:
		t := val.TimestampValue()
		return types.NewInt(int64(t.Hour()))
	case types.TypeTimestampTZ:
		t := val.TimestampTZValue()
		return types.NewInt(int64(t.Hour()))
	default:
		return types.NewNull()
	}
}

// builtinMinute implements MINUTE(time_value)
// Extracts the minute from a TIME, TIMETZ, TIMESTAMP, or TIMESTAMPTZ value.
// Returns the minute as an INTEGER (0-59).
// If argument is NULL or not a time/timestamp type, returns NULL.
func builtinMinute(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}

	val := args[0]
	switch val.Type() {
	case types.TypeTime:
		_, minute, _, _ := val.TimeValue()
		return types.NewInt(int64(minute))
	case types.TypeTimeTZ:
		_, minute, _, _, _ := val.TimeTZValue()
		return types.NewInt(int64(minute))
	case types.TypeTimestamp:
		t := val.TimestampValue()
		return types.NewInt(int64(t.Minute()))
	case types.TypeTimestampTZ:
		t := val.TimestampTZValue()
		return types.NewInt(int64(t.Minute()))
	default:
		return types.NewNull()
	}
}

// builtinSecond implements SECOND(time_value)
// Extracts the second from a TIME, TIMETZ, TIMESTAMP, or TIMESTAMPTZ value.
// Returns the second as a REAL (FLOAT) to include fractional seconds.
// If argument is NULL or not a time/timestamp type, returns NULL.
func builtinSecond(args []types.Value) types.Value {
	if len(args) != 1 || args[0].IsNull() {
		return types.NewNull()
	}

	val := args[0]
	switch val.Type() {
	case types.TypeTime:
		_, _, second, microsecond := val.TimeValue()
		// Convert to float with fractional part
		seconds := float64(second) + float64(microsecond)/1000000.0
		return types.NewFloat(seconds)
	case types.TypeTimeTZ:
		_, _, second, microsecond, _ := val.TimeTZValue()
		// Convert to float with fractional part
		seconds := float64(second) + float64(microsecond)/1000000.0
		return types.NewFloat(seconds)
	case types.TypeTimestamp:
		t := val.TimestampValue()
		// Get second and nanosecond components
		seconds := float64(t.Second()) + float64(t.Nanosecond())/1000000000.0
		return types.NewFloat(seconds)
	case types.TypeTimestampTZ:
		t := val.TimestampTZValue()
		// Get second and nanosecond components
		seconds := float64(t.Second()) + float64(t.Nanosecond())/1000000000.0
		return types.NewFloat(seconds)
	default:
		return types.NewNull()
	}
}

// parseDateTime parses a string using PostgreSQL-style format patterns
func parseDateTime(text, format string) (year, month, day, hour, minute, second int, err error) {
	// Default values
	year, month, day = 1, 1, 1
	hour, minute, second = 0, 0, 0

	isPM := false
	is12Hour := false

	textIdx := 0
	formatIdx := 0

	for formatIdx < len(format) && textIdx < len(text) {
		remaining := format[formatIdx:]

		switch {
		case strings.HasPrefix(remaining, "YYYY"):
			if textIdx+4 <= len(text) {
				year, err = strconv.Atoi(text[textIdx : textIdx+4])
				if err != nil {
					return 0, 0, 0, 0, 0, 0, err
				}
				textIdx += 4
			}
			formatIdx += 4

		case strings.HasPrefix(remaining, "YY"):
			if textIdx+2 <= len(text) {
				y, err := strconv.Atoi(text[textIdx : textIdx+2])
				if err != nil {
					return 0, 0, 0, 0, 0, 0, err
				}
				year = 2000 + y
				textIdx += 2
			}
			formatIdx += 2

		case strings.HasPrefix(remaining, "MM"):
			if textIdx+2 <= len(text) {
				month, err = strconv.Atoi(text[textIdx : textIdx+2])
				if err != nil {
					return 0, 0, 0, 0, 0, 0, err
				}
				textIdx += 2
			}
			formatIdx += 2

		case strings.HasPrefix(remaining, "DD"):
			if textIdx+2 <= len(text) {
				day, err = strconv.Atoi(text[textIdx : textIdx+2])
				if err != nil {
					return 0, 0, 0, 0, 0, 0, err
				}
				textIdx += 2
			}
			formatIdx += 2

		case strings.HasPrefix(remaining, "HH24"):
			if textIdx+2 <= len(text) {
				hour, err = strconv.Atoi(text[textIdx : textIdx+2])
				if err != nil {
					return 0, 0, 0, 0, 0, 0, err
				}
				textIdx += 2
			}
			formatIdx += 4

		case strings.HasPrefix(remaining, "HH12"), strings.HasPrefix(remaining, "HH"):
			is12Hour = true
			if textIdx+2 <= len(text) {
				hour, err = strconv.Atoi(text[textIdx : textIdx+2])
				if err != nil {
					return 0, 0, 0, 0, 0, 0, err
				}
				textIdx += 2
			}
			if strings.HasPrefix(remaining, "HH12") {
				formatIdx += 4
			} else {
				formatIdx += 2
			}

		case strings.HasPrefix(remaining, "MI"):
			if textIdx+2 <= len(text) {
				minute, err = strconv.Atoi(text[textIdx : textIdx+2])
				if err != nil {
					return 0, 0, 0, 0, 0, 0, err
				}
				textIdx += 2
			}
			formatIdx += 2

		case strings.HasPrefix(remaining, "SS"):
			if textIdx+2 <= len(text) {
				second, err = strconv.Atoi(text[textIdx : textIdx+2])
				if err != nil {
					return 0, 0, 0, 0, 0, 0, err
				}
				textIdx += 2
			}
			formatIdx += 2

		case strings.HasPrefix(remaining, "PM"), strings.HasPrefix(remaining, "AM"):
			textRemaining := strings.ToUpper(text[textIdx:])
			if strings.HasPrefix(textRemaining, "PM") {
				isPM = true
				textIdx += 2
			} else if strings.HasPrefix(textRemaining, "AM") {
				textIdx += 2
			}
			formatIdx += 2

		default:
			// Literal character - must match
			if textIdx < len(text) && text[textIdx] == format[formatIdx] {
				textIdx++
			}
			formatIdx++
		}
	}

	// Convert 12-hour to 24-hour
	if is12Hour {
		if isPM && hour < 12 {
			hour += 12
		} else if !isPM && hour == 12 {
			hour = 0
		}
	}

	return year, month, day, hour, minute, second, nil
}

// builtinToDate implements TO_DATE(text, format)
// Parses a string using PostgreSQL-style format patterns and returns a DATE.
// If any argument is NULL or parsing fails, returns NULL.
func builtinToDate(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	text := args[0].Text()
	format := args[1].Text()

	year, month, day, _, _, _, err := parseDateTime(text, format)
	if err != nil {
		return types.NewNull()
	}

	return types.NewDate(year, month, day)
}

// builtinToTimestamp implements TO_TIMESTAMP(text, format)
// Parses a string using PostgreSQL-style format patterns and returns a TIMESTAMP.
// If any argument is NULL or parsing fails, returns NULL.
func builtinToTimestamp(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	text := args[0].Text()
	format := args[1].Text()

	year, month, day, hour, minute, second, err := parseDateTime(text, format)
	if err != nil {
		return types.NewNull()
	}

	return types.NewTimestamp(year, month, day, hour, minute, second, 0)
}

// builtinDateTrunc implements DATE_TRUNC(field, source)
// Truncates a date/timestamp to the specified precision.
// Returns the same type as the input (DATE, TIMESTAMP, or TIMESTAMPTZ).
// If any argument is NULL, returns NULL.
func builtinDateTrunc(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	field := strings.ToLower(args[0].Text())

	var t time.Time
	inputType := args[1].Type()

	switch inputType {
	case types.TypeDate:
		year, month, day := args[1].DateValue()
		t = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	case types.TypeTimestamp:
		t = args[1].TimestampValue()
	case types.TypeTimestampTZ:
		t = args[1].TimestampTZValue()
	default:
		return types.NewNull()
	}

	var truncated time.Time
	switch field {
	case "year":
		truncated = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	case "quarter":
		q := (int(t.Month())-1)/3*3 + 1
		truncated = time.Date(t.Year(), time.Month(q), 1, 0, 0, 0, 0, time.UTC)
	case "month":
		truncated = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	case "week":
		// Truncate to Monday of the current week (ISO week)
		weekday := int(t.Weekday())
		if weekday == 0 {
			weekday = 7 // Sunday = 7 in ISO
		}
		truncated = time.Date(t.Year(), t.Month(), t.Day()-(weekday-1), 0, 0, 0, 0, time.UTC)
	case "day":
		truncated = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case "hour":
		truncated = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	case "minute":
		truncated = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, time.UTC)
	case "second":
		truncated = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
	default:
		return types.NewNull()
	}

	// Return same type as input
	switch inputType {
	case types.TypeDate:
		return types.NewDate(truncated.Year(), int(truncated.Month()), truncated.Day())
	case types.TypeTimestamp:
		return types.NewTimestamp(truncated.Year(), int(truncated.Month()), truncated.Day(),
			truncated.Hour(), truncated.Minute(), truncated.Second(), 0)
	case types.TypeTimestampTZ:
		return types.NewTimestampTZ(truncated)
	default:
		return types.NewNull()
	}
}

// builtinExtract implements EXTRACT(field FROM source)
// Extracts a field from a date/time value and returns it as a float.
// Supports: year, month, day, hour, minute, second, quarter, week, dow, doy, epoch.
// If any argument is NULL, returns NULL.
func builtinExtract(args []types.Value) types.Value {
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return types.NewNull()
	}

	field := strings.ToLower(args[0].Text())

	var t time.Time
	var microseconds int

	switch args[1].Type() {
	case types.TypeDate:
		year, month, day := args[1].DateValue()
		t = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	case types.TypeTime:
		hour, min, sec, usec := args[1].TimeValue()
		t = time.Date(0, 1, 1, hour, min, sec, usec*1000, time.UTC)
		microseconds = usec
	case types.TypeTimestamp:
		t = args[1].TimestampValue()
		microseconds = t.Nanosecond() / 1000
	case types.TypeTimestampTZ:
		t = args[1].TimestampTZValue()
		microseconds = t.Nanosecond() / 1000
	default:
		return types.NewNull()
	}

	var result float64
	switch field {
	case "year":
		result = float64(t.Year())
	case "month":
		result = float64(t.Month())
	case "day":
		result = float64(t.Day())
	case "hour":
		result = float64(t.Hour())
	case "minute":
		result = float64(t.Minute())
	case "second":
		result = float64(t.Second()) + float64(microseconds)/1000000.0
	case "millisecond":
		result = float64(t.Second()*1000) + float64(microseconds)/1000.0
	case "microsecond":
		result = float64(t.Second()*1000000) + float64(microseconds)
	case "quarter":
		result = float64((int(t.Month())-1)/3 + 1)
	case "week":
		_, week := t.ISOWeek()
		result = float64(week)
	case "dow", "dayofweek":
		result = float64(t.Weekday()) // 0=Sunday
	case "doy", "dayofyear":
		result = float64(t.YearDay())
	case "epoch":
		result = float64(t.Unix()) + float64(t.Nanosecond())/1e9
	case "timezone":
		_, offset := t.Zone()
		result = float64(offset)
	default:
		return types.NewNull()
	}

	return types.NewFloat(result)
}

// builtinDatePart implements DATE_PART(field, source)
// Alias for EXTRACT - extracts a field from a date/time value.
// If any argument is NULL, returns NULL.
func builtinDatePart(args []types.Value) types.Value {
	return builtinExtract(args)
}

// builtinAge implements AGE(timestamp1[, timestamp2])
// Returns INTERVAL representing the difference between timestamps.
// AGE(ts1, ts2) returns ts1 - ts2
// AGE(ts) returns current_date - ts
// Properly handles variable-length months and month boundaries.
// Supports DATE, TIMESTAMP, and TIMESTAMPTZ inputs.
// Returns NULL for NULL inputs.
func builtinAge(args []types.Value) types.Value {
	if len(args) == 0 || args[0].IsNull() {
		return types.NewNull()
	}

	var t1, t2 time.Time

	// Get first timestamp
	switch args[0].Type() {
	case types.TypeDate:
		year, month, day := args[0].DateValue()
		t1 = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	case types.TypeTimestamp:
		t1 = args[0].TimestampValue()
	case types.TypeTimestampTZ:
		t1 = args[0].TimestampTZValue()
	default:
		return types.NewNull()
	}

	// Get second timestamp (or use current date for single-arg form)
	if len(args) == 1 {
		// Single argument: AGE(timestamp) = current_date - timestamp
		now := time.Now().UTC()
		t2 = t1
		t1 = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	} else {
		if args[1].IsNull() {
			return types.NewNull()
		}
		switch args[1].Type() {
		case types.TypeDate:
			year, month, day := args[1].DateValue()
			t2 = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		case types.TypeTimestamp:
			t2 = args[1].TimestampValue()
		case types.TypeTimestampTZ:
			t2 = args[1].TimestampTZValue()
		default:
			return types.NewNull()
		}
	}

	// Calculate the difference
	// Approach: Calculate years and months first, then remaining days/time
	negative := false
	if t1.Before(t2) {
		t1, t2 = t2, t1
		negative = true
	}

	years := t1.Year() - t2.Year()
	months := int(t1.Month()) - int(t2.Month())
	days := t1.Day() - t2.Day()

	// Handle day underflow
	if days < 0 {
		months--
		// Add days from the previous month
		prevMonth := t1.AddDate(0, 0, -t1.Day())
		days += prevMonth.Day()
	}

	// Handle month underflow
	if months < 0 {
		years--
		months += 12
	}

	totalMonths := int64(years*12 + months)

	// Calculate time difference within the day
	hourDiff := t1.Hour() - t2.Hour()
	minDiff := t1.Minute() - t2.Minute()
	secDiff := t1.Second() - t2.Second()
	nsecDiff := t1.Nanosecond() - t2.Nanosecond()

	// Convert to microseconds
	totalMicroseconds := int64(days)*24*3600*1000000 +
		int64(hourDiff)*3600*1000000 +
		int64(minDiff)*60*1000000 +
		int64(secDiff)*1000000 +
		int64(nsecDiff)/1000

	if negative {
		totalMonths = -totalMonths
		totalMicroseconds = -totalMicroseconds
	}

	return types.NewInterval(totalMonths, totalMicroseconds)
}

// builtinIf implements IF(condition, true_value, false_value)
// Returns true_value if condition is truthy, else returns false_value.
// Truthy values: non-zero numbers, non-empty strings
// Falsy values: 0, 0.0, empty string, NULL
func builtinIf(args []types.Value) types.Value {
	if len(args) != 3 {
		return types.NewNull()
	}

	condition := args[0]
	trueVal := args[1]
	falseVal := args[2]

	// Evaluate the condition
	if isTruthy(condition) {
		return trueVal
	}
	return falseVal
}

// isTruthy determines if a value is truthy (non-zero, non-empty, non-null)
func isTruthy(v types.Value) bool {
	if v.IsNull() {
		return false
	}

	switch v.Type() {
	case types.TypeInt:
		return v.Int() != 0
	case types.TypeFloat:
		return v.Float() != 0.0
	case types.TypeText:
		return v.Text() != ""
	default:
		// Other types (blob, vector, etc.) are considered truthy if not null
		return true
	}
}

// builtinIfNull implements IFNULL(expr, alt_value)
// Returns expr if it is not NULL, otherwise returns alt_value.
// This is equivalent to COALESCE with exactly 2 arguments.
func builtinIfNull(args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewNull()
	}

	if args[0].IsNull() {
		return args[1]
	}
	return args[0]
}

// builtinNullIf implements NULLIF(expr1, expr2)
// Returns NULL if expr1 equals expr2, otherwise returns expr1.
// Uses types.Compare for comparison.
func builtinNullIf(args []types.Value) types.Value {
	if len(args) != 2 {
		return types.NewNull()
	}

	// If first arg is NULL, return NULL
	if args[0].IsNull() {
		return types.NewNull()
	}

	// If second arg is NULL, return first arg (NULL != anything non-NULL)
	if args[1].IsNull() {
		return args[0]
	}

	// Compare the values - if equal, return NULL
	if types.Compare(args[0], args[1]) == 0 {
		return types.NewNull()
	}

	return args[0]
}
