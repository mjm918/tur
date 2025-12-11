// pkg/types/strict_types_test.go
package types

import (
	"math"
	"testing"
)

// Test new strict integer types
func TestSmallIntType(t *testing.T) {
	// SMALLINT is 2-byte signed integer: -32768 to 32767
	v := NewSmallInt(100)
	if v.Type() != TypeSmallInt {
		t.Errorf("expected TypeSmallInt, got %v", v.Type())
	}
	if v.SmallInt() != 100 {
		t.Errorf("expected 100, got %d", v.SmallInt())
	}

	// Test boundary values
	vMin := NewSmallInt(-32768)
	if vMin.SmallInt() != -32768 {
		t.Errorf("expected -32768, got %d", vMin.SmallInt())
	}

	vMax := NewSmallInt(32767)
	if vMax.SmallInt() != 32767 {
		t.Errorf("expected 32767, got %d", vMax.SmallInt())
	}
}

func TestIntType(t *testing.T) {
	// INT is 4-byte signed integer: -2147483648 to 2147483647
	v := NewInt32(12345)
	if v.Type() != TypeInt32 {
		t.Errorf("expected TypeInt32, got %v", v.Type())
	}
	if v.Int32() != 12345 {
		t.Errorf("expected 12345, got %d", v.Int32())
	}

	// Test boundary values
	vMin := NewInt32(-2147483648)
	if vMin.Int32() != -2147483648 {
		t.Errorf("expected -2147483648, got %d", vMin.Int32())
	}

	vMax := NewInt32(2147483647)
	if vMax.Int32() != 2147483647 {
		t.Errorf("expected 2147483647, got %d", vMax.Int32())
	}
}

func TestBigIntType(t *testing.T) {
	// BIGINT is 8-byte signed integer
	v := NewBigInt(9223372036854775807)
	if v.Type() != TypeBigInt {
		t.Errorf("expected TypeBigInt, got %v", v.Type())
	}
	if v.BigInt() != 9223372036854775807 {
		t.Errorf("expected 9223372036854775807, got %d", v.BigInt())
	}

	// Test negative
	vNeg := NewBigInt(-9223372036854775808)
	if vNeg.BigInt() != -9223372036854775808 {
		t.Errorf("expected -9223372036854775808, got %d", vNeg.BigInt())
	}
}

func TestSerialType(t *testing.T) {
	// SERIAL is auto-incrementing 4-byte integer
	v := NewSerial(1)
	if v.Type() != TypeSerial {
		t.Errorf("expected TypeSerial, got %v", v.Type())
	}
	if v.Serial() != 1 {
		t.Errorf("expected 1, got %d", v.Serial())
	}
}

func TestBigSerialType(t *testing.T) {
	// BIGSERIAL is auto-incrementing 8-byte integer
	v := NewBigSerial(9223372036854775807)
	if v.Type() != TypeBigSerial {
		t.Errorf("expected TypeBigSerial, got %v", v.Type())
	}
	if v.BigSerial() != 9223372036854775807 {
		t.Errorf("expected 9223372036854775807, got %d", v.BigSerial())
	}
}

func TestGUIDType(t *testing.T) {
	// GUID/UUID is a 128-bit identifier stored as 16 bytes
	uuid := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
	v := NewGUID(uuid)
	if v.Type() != TypeGUID {
		t.Errorf("expected TypeGUID, got %v", v.Type())
	}
	if v.GUID() != uuid {
		t.Errorf("GUID mismatch")
	}

	// Test GUID from string
	v2, err := NewGUIDFromString("550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v2.Type() != TypeGUID {
		t.Errorf("expected TypeGUID, got %v", v2.Type())
	}

	// Test invalid GUID string
	_, err = NewGUIDFromString("invalid-guid")
	if err == nil {
		t.Error("expected error for invalid GUID string")
	}
}

func TestDecimalType(t *testing.T) {
	// DECIMAL(precision, scale) for exact numeric values
	// DECIMAL(10, 2) means 10 total digits with 2 after decimal point
	v, err := NewDecimal("123.45", 10, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Type() != TypeDecimal {
		t.Errorf("expected TypeDecimal, got %v", v.Type())
	}

	str := v.DecimalString()
	if str != "123.45" {
		t.Errorf("expected '123.45', got '%s'", str)
	}

	prec, scale := v.DecimalPrecisionScale()
	if prec != 10 || scale != 2 {
		t.Errorf("expected precision=10, scale=2, got precision=%d, scale=%d", prec, scale)
	}

	// Test negative decimal
	vNeg, err := NewDecimal("-999.99", 5, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if vNeg.DecimalString() != "-999.99" {
		t.Errorf("expected '-999.99', got '%s'", vNeg.DecimalString())
	}

	// Test zero
	vZero, err := NewDecimal("0.00", 5, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if vZero.DecimalString() != "0.00" {
		t.Errorf("expected '0.00', got '%s'", vZero.DecimalString())
	}

	// Test precision overflow - should error
	_, err = NewDecimal("12345678901", 10, 0) // 11 digits, only 10 allowed
	if err == nil {
		t.Error("expected error for precision overflow")
	}
}

func TestVarcharType(t *testing.T) {
	// VARCHAR(n) is variable-length string with max length n
	v := NewVarchar("hello", 10)
	if v.Type() != TypeVarchar {
		t.Errorf("expected TypeVarchar, got %v", v.Type())
	}
	if v.Varchar() != "hello" {
		t.Errorf("expected 'hello', got '%s'", v.Varchar())
	}
	if v.VarcharMaxLen() != 10 {
		t.Errorf("expected max len 10, got %d", v.VarcharMaxLen())
	}

	// Test empty string
	vEmpty := NewVarchar("", 10)
	if vEmpty.Varchar() != "" {
		t.Errorf("expected empty string, got '%s'", vEmpty.Varchar())
	}

	// Test max length string
	vMax := NewVarchar("1234567890", 10)
	if vMax.Varchar() != "1234567890" {
		t.Errorf("expected '1234567890', got '%s'", vMax.Varchar())
	}
}

func TestCharType(t *testing.T) {
	// CHAR(n) is fixed-length string, padded with spaces
	v := NewChar("hello", 10)
	if v.Type() != TypeChar {
		t.Errorf("expected TypeChar, got %v", v.Type())
	}
	// CHAR should be space-padded to the specified length
	if v.Char() != "hello     " {
		t.Errorf("expected 'hello     ' (space-padded), got '%s'", v.Char())
	}
	if v.CharLen() != 10 {
		t.Errorf("expected char len 10, got %d", v.CharLen())
	}
}

func TestTypeStringRepresentations(t *testing.T) {
	tests := []struct {
		typ      ValueType
		expected string
	}{
		{TypeSmallInt, "SMALLINT"},
		{TypeInt32, "INT"},
		{TypeBigInt, "BIGINT"},
		{TypeSerial, "SERIAL"},
		{TypeBigSerial, "BIGSERIAL"},
		{TypeGUID, "GUID"},
		{TypeDecimal, "DECIMAL"},
		{TypeVarchar, "VARCHAR"},
		{TypeChar, "CHAR"},
	}

	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.expected {
			t.Errorf("Type %d: expected %q, got %q", tt.typ, tt.expected, got)
		}
	}
}

func TestIntegerRangeValidation(t *testing.T) {
	// Test that ValidateSmallInt correctly validates range
	if err := ValidateSmallInt(0); err != nil {
		t.Errorf("0 should be valid SmallInt: %v", err)
	}
	if err := ValidateSmallInt(32767); err != nil {
		t.Errorf("32767 should be valid SmallInt: %v", err)
	}
	if err := ValidateSmallInt(-32768); err != nil {
		t.Errorf("-32768 should be valid SmallInt: %v", err)
	}
	if err := ValidateSmallInt(32768); err == nil {
		t.Error("32768 should be invalid SmallInt")
	}
	if err := ValidateSmallInt(-32769); err == nil {
		t.Error("-32769 should be invalid SmallInt")
	}

	// Test that ValidateInt32 correctly validates range
	if err := ValidateInt32(0); err != nil {
		t.Errorf("0 should be valid Int32: %v", err)
	}
	if err := ValidateInt32(2147483647); err != nil {
		t.Errorf("2147483647 should be valid Int32: %v", err)
	}
	if err := ValidateInt32(-2147483648); err != nil {
		t.Errorf("-2147483648 should be valid Int32: %v", err)
	}
	if err := ValidateInt32(2147483648); err == nil {
		t.Error("2147483648 should be invalid Int32")
	}
	if err := ValidateInt32(-2147483649); err == nil {
		t.Error("-2147483649 should be invalid Int32")
	}
}

func TestVarcharLengthValidation(t *testing.T) {
	// Test ValidateVarchar
	if err := ValidateVarchar("hello", 10); err != nil {
		t.Errorf("'hello' should be valid for VARCHAR(10): %v", err)
	}
	if err := ValidateVarchar("1234567890", 10); err != nil {
		t.Errorf("'1234567890' should be valid for VARCHAR(10): %v", err)
	}
	if err := ValidateVarchar("12345678901", 10); err == nil {
		t.Error("'12345678901' should be invalid for VARCHAR(10)")
	}
}

func TestDecimalComparison(t *testing.T) {
	v1, _ := NewDecimal("100.50", 10, 2)
	v2, _ := NewDecimal("100.50", 10, 2)
	v3, _ := NewDecimal("200.00", 10, 2)

	if Compare(v1, v2) != 0 {
		t.Error("100.50 should equal 100.50")
	}
	if Compare(v1, v3) >= 0 {
		t.Error("100.50 should be less than 200.00")
	}
	if Compare(v3, v1) <= 0 {
		t.Error("200.00 should be greater than 100.50")
	}
}

func TestGUIDComparison(t *testing.T) {
	uuid1 := [16]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}
	uuid2 := [16]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}

	v1 := NewGUID(uuid1)
	v2 := NewGUID(uuid1)
	v3 := NewGUID(uuid2)

	if Compare(v1, v2) != 0 {
		t.Error("identical GUIDs should be equal")
	}
	if Compare(v1, v3) >= 0 {
		t.Error("uuid1 should be less than uuid2")
	}
}

func TestTypeConversions(t *testing.T) {
	// Test that we can convert between compatible integer types
	smallVal := NewSmallInt(100)
	int32Val := NewInt32(100)
	bigIntVal := NewBigInt(100)

	// These should all have the same underlying value
	if smallVal.SmallInt() != 100 || int32Val.Int32() != 100 || bigIntVal.BigInt() != 100 {
		t.Error("values should all be 100")
	}
}

// Test that existing TypeInt still works (backwards compatibility)
func TestLegacyIntTypeStillWorks(t *testing.T) {
	v := NewInt(12345)
	if v.Type() != TypeInt {
		t.Errorf("expected TypeInt, got %v", v.Type())
	}
	if v.Int() != 12345 {
		t.Errorf("expected 12345, got %d", v.Int())
	}
}

func TestDecimalArithmetic(t *testing.T) {
	// Test that decimal values maintain precision
	v1, _ := NewDecimal("10.25", 10, 2)
	v2, _ := NewDecimal("5.75", 10, 2)

	// Get the internal representation for comparison
	// The exact arithmetic is implementation-dependent but values should be stored precisely
	if v1.DecimalString() != "10.25" {
		t.Errorf("expected '10.25', got '%s'", v1.DecimalString())
	}
	if v2.DecimalString() != "5.75" {
		t.Errorf("expected '5.75', got '%s'", v2.DecimalString())
	}
}

func TestNullValuesForNewTypes(t *testing.T) {
	// Null should work with all types
	null := NewNull()
	if !null.IsNull() {
		t.Error("NewNull() should be null")
	}

	// Comparison with null
	v := NewInt32(100)
	if Compare(null, v) >= 0 {
		t.Error("null should be less than any non-null value")
	}
}

// Ensure math package is used (for IDE/compiler)
var _ = math.MaxInt32
