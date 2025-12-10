package vdbe

import (
	"testing"

	"tur/pkg/types"
)

// Tests for IF() conditional function
// IF(condition, true_value, false_value) - returns true_value if condition is truthy, else false_value

func TestBuiltinIf_TrueCondition(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF(1, 'yes', 'no') should return 'yes'
	result := fn.Call([]types.Value{
		types.NewInt(1),
		types.NewText("yes"),
		types.NewText("no"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "yes" {
		t.Errorf("Expected 'yes', got %q", result.Text())
	}
}

func TestBuiltinIf_FalseCondition(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF(0, 'yes', 'no') should return 'no'
	result := fn.Call([]types.Value{
		types.NewInt(0),
		types.NewText("yes"),
		types.NewText("no"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "no" {
		t.Errorf("Expected 'no', got %q", result.Text())
	}
}

func TestBuiltinIf_NullCondition(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF(NULL, 'yes', 'no') should return 'no' (NULL is falsy)
	result := fn.Call([]types.Value{
		types.NewNull(),
		types.NewText("yes"),
		types.NewText("no"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "no" {
		t.Errorf("Expected 'no', got %q", result.Text())
	}
}

func TestBuiltinIf_IntegerTrue(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF(1, 100, 200) should return 100 (1 represents TRUE)
	result := fn.Call([]types.Value{
		types.NewInt(1),
		types.NewInt(100),
		types.NewInt(200),
	})

	if result.Type() != types.TypeInt {
		t.Fatalf("Expected TypeInt, got %v", result.Type())
	}
	if result.Int() != 100 {
		t.Errorf("Expected 100, got %d", result.Int())
	}
}

func TestBuiltinIf_IntegerFalse(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF(0, 100, 200) should return 200 (0 represents FALSE)
	result := fn.Call([]types.Value{
		types.NewInt(0),
		types.NewInt(100),
		types.NewInt(200),
	})

	if result.Type() != types.TypeInt {
		t.Fatalf("Expected TypeInt, got %v", result.Type())
	}
	if result.Int() != 200 {
		t.Errorf("Expected 200, got %d", result.Int())
	}
}

func TestBuiltinIf_NonZeroFloat(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF(0.5, 'truthy', 'falsy') should return 'truthy' (non-zero float is truthy)
	result := fn.Call([]types.Value{
		types.NewFloat(0.5),
		types.NewText("truthy"),
		types.NewText("falsy"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "truthy" {
		t.Errorf("Expected 'truthy', got %q", result.Text())
	}
}

func TestBuiltinIf_ZeroFloat(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF(0.0, 'truthy', 'falsy') should return 'falsy' (zero float is falsy)
	result := fn.Call([]types.Value{
		types.NewFloat(0.0),
		types.NewText("truthy"),
		types.NewText("falsy"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "falsy" {
		t.Errorf("Expected 'falsy', got %q", result.Text())
	}
}

func TestBuiltinIf_NonEmptyString(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF('hello', 'truthy', 'falsy') should return 'truthy' (non-empty string is truthy)
	result := fn.Call([]types.Value{
		types.NewText("hello"),
		types.NewText("truthy"),
		types.NewText("falsy"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "truthy" {
		t.Errorf("Expected 'truthy', got %q", result.Text())
	}
}

func TestBuiltinIf_EmptyString(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF('', 'truthy', 'falsy') should return 'falsy' (empty string is falsy)
	result := fn.Call([]types.Value{
		types.NewText(""),
		types.NewText("truthy"),
		types.NewText("falsy"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "falsy" {
		t.Errorf("Expected 'falsy', got %q", result.Text())
	}
}

func TestBuiltinIf_WrongNumberOfArgs(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF with wrong number of args should return NULL
	result := fn.Call([]types.Value{
		types.NewInt(1),
		types.NewText("yes"),
	})

	if !result.IsNull() {
		t.Errorf("Expected NULL for wrong number of args, got %v", result)
	}
}

func TestBuiltinIf_NullTrueValue(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF(1, NULL, 'no') should return NULL
	result := fn.Call([]types.Value{
		types.NewInt(1),
		types.NewNull(),
		types.NewText("no"),
	})

	if !result.IsNull() {
		t.Errorf("Expected NULL, got %v", result)
	}
}

func TestBuiltinIf_NullFalseValue(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF(0, 'yes', NULL) should return NULL
	result := fn.Call([]types.Value{
		types.NewInt(0),
		types.NewText("yes"),
		types.NewNull(),
	})

	if !result.IsNull() {
		t.Errorf("Expected NULL, got %v", result)
	}
}

func TestBuiltinIf_NegativeNumber(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IF")
	if fn == nil {
		t.Fatal("IF function not found in registry")
	}

	// IF(-1, 'truthy', 'falsy') should return 'truthy' (negative numbers are truthy)
	result := fn.Call([]types.Value{
		types.NewInt(-1),
		types.NewText("truthy"),
		types.NewText("falsy"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "truthy" {
		t.Errorf("Expected 'truthy', got %q", result.Text())
	}
}

// Tests for IFNULL() function
// IFNULL(expr, alt_value) - returns expr if not NULL, otherwise alt_value

func TestBuiltinIfNull_NonNullValue(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IFNULL")
	if fn == nil {
		t.Fatal("IFNULL function not found in registry")
	}

	// IFNULL('hello', 'default') should return 'hello'
	result := fn.Call([]types.Value{
		types.NewText("hello"),
		types.NewText("default"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "hello" {
		t.Errorf("Expected 'hello', got %q", result.Text())
	}
}

func TestBuiltinIfNull_NullValue(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IFNULL")
	if fn == nil {
		t.Fatal("IFNULL function not found in registry")
	}

	// IFNULL(NULL, 'default') should return 'default'
	result := fn.Call([]types.Value{
		types.NewNull(),
		types.NewText("default"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "default" {
		t.Errorf("Expected 'default', got %q", result.Text())
	}
}

func TestBuiltinIfNull_ZeroIsNotNull(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IFNULL")
	if fn == nil {
		t.Fatal("IFNULL function not found in registry")
	}

	// IFNULL(0, 100) should return 0 (0 is not NULL)
	result := fn.Call([]types.Value{
		types.NewInt(0),
		types.NewInt(100),
	})

	if result.Type() != types.TypeInt {
		t.Fatalf("Expected TypeInt, got %v", result.Type())
	}
	if result.Int() != 0 {
		t.Errorf("Expected 0, got %d", result.Int())
	}
}

func TestBuiltinIfNull_EmptyStringIsNotNull(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IFNULL")
	if fn == nil {
		t.Fatal("IFNULL function not found in registry")
	}

	// IFNULL('', 'default') should return '' (empty string is not NULL)
	result := fn.Call([]types.Value{
		types.NewText(""),
		types.NewText("default"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "" {
		t.Errorf("Expected '', got %q", result.Text())
	}
}

func TestBuiltinIfNull_WrongNumberOfArgs(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("IFNULL")
	if fn == nil {
		t.Fatal("IFNULL function not found in registry")
	}

	// IFNULL with wrong number of args should return NULL
	result := fn.Call([]types.Value{
		types.NewInt(1),
	})

	if !result.IsNull() {
		t.Errorf("Expected NULL for wrong number of args, got %v", result)
	}
}

// Tests for NULLIF() function
// NULLIF(expr1, expr2) - returns NULL if expr1 = expr2, otherwise returns expr1

func TestBuiltinNullIf_EqualValues(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("NULLIF")
	if fn == nil {
		t.Fatal("NULLIF function not found in registry")
	}

	// NULLIF(5, 5) should return NULL (values are equal)
	result := fn.Call([]types.Value{
		types.NewInt(5),
		types.NewInt(5),
	})

	if !result.IsNull() {
		t.Errorf("Expected NULL for equal values, got %v", result)
	}
}

func TestBuiltinNullIf_DifferentValues(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("NULLIF")
	if fn == nil {
		t.Fatal("NULLIF function not found in registry")
	}

	// NULLIF(5, 10) should return 5 (values are different)
	result := fn.Call([]types.Value{
		types.NewInt(5),
		types.NewInt(10),
	})

	if result.Type() != types.TypeInt {
		t.Fatalf("Expected TypeInt, got %v", result.Type())
	}
	if result.Int() != 5 {
		t.Errorf("Expected 5, got %d", result.Int())
	}
}

func TestBuiltinNullIf_EqualStrings(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("NULLIF")
	if fn == nil {
		t.Fatal("NULLIF function not found in registry")
	}

	// NULLIF('hello', 'hello') should return NULL
	result := fn.Call([]types.Value{
		types.NewText("hello"),
		types.NewText("hello"),
	})

	if !result.IsNull() {
		t.Errorf("Expected NULL for equal strings, got %v", result)
	}
}

func TestBuiltinNullIf_DifferentStrings(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("NULLIF")
	if fn == nil {
		t.Fatal("NULLIF function not found in registry")
	}

	// NULLIF('hello', 'world') should return 'hello'
	result := fn.Call([]types.Value{
		types.NewText("hello"),
		types.NewText("world"),
	})

	if result.Type() != types.TypeText {
		t.Fatalf("Expected TypeText, got %v", result.Type())
	}
	if result.Text() != "hello" {
		t.Errorf("Expected 'hello', got %q", result.Text())
	}
}

func TestBuiltinNullIf_FirstArgNull(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("NULLIF")
	if fn == nil {
		t.Fatal("NULLIF function not found in registry")
	}

	// NULLIF(NULL, 5) should return NULL (first arg is NULL)
	result := fn.Call([]types.Value{
		types.NewNull(),
		types.NewInt(5),
	})

	if !result.IsNull() {
		t.Errorf("Expected NULL when first arg is NULL, got %v", result)
	}
}

func TestBuiltinNullIf_SecondArgNull(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("NULLIF")
	if fn == nil {
		t.Fatal("NULLIF function not found in registry")
	}

	// NULLIF(5, NULL) should return 5 (NULL != 5)
	result := fn.Call([]types.Value{
		types.NewInt(5),
		types.NewNull(),
	})

	if result.Type() != types.TypeInt {
		t.Fatalf("Expected TypeInt, got %v", result.Type())
	}
	if result.Int() != 5 {
		t.Errorf("Expected 5, got %d", result.Int())
	}
}

func TestBuiltinNullIf_WrongNumberOfArgs(t *testing.T) {
	registry := DefaultFunctionRegistry()
	fn := registry.Lookup("NULLIF")
	if fn == nil {
		t.Fatal("NULLIF function not found in registry")
	}

	// NULLIF with wrong number of args should return NULL
	result := fn.Call([]types.Value{
		types.NewInt(1),
	})

	if !result.IsNull() {
		t.Errorf("Expected NULL for wrong number of args, got %v", result)
	}
}
