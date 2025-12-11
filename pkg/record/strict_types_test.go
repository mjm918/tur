// pkg/record/strict_types_test.go
package record

import (
	"bytes"
	"testing"

	"tur/pkg/types"
)

// Test serialization of strict integer types

func TestEncodeDecode_SmallInt(t *testing.T) {
	testValues := []int16{0, 1, -1, 127, -128, 32767, -32768}

	for _, i := range testValues {
		values := []types.Value{types.NewSmallInt(i)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode SmallInt(%d): got %d values, want 1", i, len(decoded))
		}
		// After decode, the type should be preserved as SmallInt
		if decoded[0].Type() != types.TypeSmallInt {
			t.Errorf("Decode SmallInt(%d): got type %v, want TypeSmallInt", i, decoded[0].Type())
		}
		if decoded[0].SmallInt() != i {
			t.Errorf("Decode SmallInt(%d): got %d", i, decoded[0].SmallInt())
		}
	}
}

func TestEncodeDecode_Int32(t *testing.T) {
	testValues := []int32{0, 1, -1, 32767, -32768, 2147483647, -2147483648}

	for _, i := range testValues {
		values := []types.Value{types.NewInt32(i)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode Int32(%d): got %d values, want 1", i, len(decoded))
		}
		if decoded[0].Type() != types.TypeInt32 {
			t.Errorf("Decode Int32(%d): got type %v, want TypeInt32", i, decoded[0].Type())
		}
		if decoded[0].Int32() != i {
			t.Errorf("Decode Int32(%d): got %d", i, decoded[0].Int32())
		}
	}
}

func TestEncodeDecode_BigInt(t *testing.T) {
	testValues := []int64{0, 1, -1, 2147483647, -2147483648, 9223372036854775807, -9223372036854775808}

	for _, i := range testValues {
		values := []types.Value{types.NewBigInt(i)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode BigInt(%d): got %d values, want 1", i, len(decoded))
		}
		if decoded[0].Type() != types.TypeBigInt {
			t.Errorf("Decode BigInt(%d): got type %v, want TypeBigInt", i, decoded[0].Type())
		}
		if decoded[0].BigInt() != i {
			t.Errorf("Decode BigInt(%d): got %d", i, decoded[0].BigInt())
		}
	}
}

func TestEncodeDecode_Serial(t *testing.T) {
	testValues := []int32{1, 100, 1000, 2147483647}

	for _, i := range testValues {
		values := []types.Value{types.NewSerial(i)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode Serial(%d): got %d values, want 1", i, len(decoded))
		}
		if decoded[0].Type() != types.TypeSerial {
			t.Errorf("Decode Serial(%d): got type %v, want TypeSerial", i, decoded[0].Type())
		}
		if decoded[0].Serial() != i {
			t.Errorf("Decode Serial(%d): got %d", i, decoded[0].Serial())
		}
	}
}

func TestEncodeDecode_BigSerial(t *testing.T) {
	testValues := []int64{1, 1000, 1000000, 9223372036854775807}

	for _, i := range testValues {
		values := []types.Value{types.NewBigSerial(i)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode BigSerial(%d): got %d values, want 1", i, len(decoded))
		}
		if decoded[0].Type() != types.TypeBigSerial {
			t.Errorf("Decode BigSerial(%d): got type %v, want TypeBigSerial", i, decoded[0].Type())
		}
		if decoded[0].BigSerial() != i {
			t.Errorf("Decode BigSerial(%d): got %d", i, decoded[0].BigSerial())
		}
	}
}

func TestEncodeDecode_GUID(t *testing.T) {
	testGUIDs := [][16]byte{
		{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0},
		{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
	}

	for _, guid := range testGUIDs {
		values := []types.Value{types.NewGUID(guid)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode GUID: got %d values, want 1", len(decoded))
		}
		if decoded[0].Type() != types.TypeGUID {
			t.Errorf("Decode GUID: got type %v, want TypeGUID", decoded[0].Type())
		}
		decodedGUID := decoded[0].GUID()
		if decodedGUID != guid {
			t.Errorf("Decode GUID: got %v, want %v", decodedGUID, guid)
		}
	}
}

func TestEncodeDecode_GUIDFromString(t *testing.T) {
	testStrings := []string{
		"12345678-9abc-def0-1234-56789abcdef0",
		"00000000-0000-0000-0000-000000000000",
		"ffffffff-ffff-ffff-ffff-ffffffffffff",
	}

	for _, s := range testStrings {
		v, err := types.NewGUIDFromString(s)
		if err != nil {
			t.Fatalf("NewGUIDFromString(%s) failed: %v", s, err)
		}

		values := []types.Value{v}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode GUID: got %d values, want 1", len(decoded))
		}
		if decoded[0].Type() != types.TypeGUID {
			t.Errorf("Decode GUID: got type %v, want TypeGUID", decoded[0].Type())
		}
		if decoded[0].GUIDString() != s {
			t.Errorf("Decode GUID: got %s, want %s", decoded[0].GUIDString(), s)
		}
	}
}

func TestEncodeDecode_Decimal(t *testing.T) {
	testCases := []struct {
		value     string
		precision int
		scale     int
	}{
		{"123.45", 10, 2},
		{"0.00", 10, 2},
		{"-999.99", 10, 2},
		{"12345678.1234", 15, 4},
		{"0", 5, 0},
	}

	for _, tc := range testCases {
		v, err := types.NewDecimal(tc.value, tc.precision, tc.scale)
		if err != nil {
			t.Fatalf("NewDecimal(%s, %d, %d) failed: %v", tc.value, tc.precision, tc.scale, err)
		}

		values := []types.Value{v}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode Decimal: got %d values, want 1", len(decoded))
		}
		if decoded[0].Type() != types.TypeDecimal {
			t.Errorf("Decode Decimal(%s): got type %v, want TypeDecimal", tc.value, decoded[0].Type())
		}

		// Compare the string representation
		originalStr := v.DecimalString()
		decodedStr := decoded[0].DecimalString()
		if decodedStr != originalStr {
			t.Errorf("Decode Decimal(%s): got %s, want %s", tc.value, decodedStr, originalStr)
		}

		// Compare precision and scale
		origP, origS := v.DecimalPrecisionScale()
		decP, decS := decoded[0].DecimalPrecisionScale()
		if decP != origP || decS != origS {
			t.Errorf("Decode Decimal precision/scale: got (%d, %d), want (%d, %d)", decP, decS, origP, origS)
		}
	}
}

func TestEncodeDecode_Varchar(t *testing.T) {
	testCases := []struct {
		value  string
		maxLen int
	}{
		{"hello", 100},
		{"", 10},
		{"test string", 50},
		{"日本語テスト", 100},
	}

	for _, tc := range testCases {
		values := []types.Value{types.NewVarchar(tc.value, tc.maxLen)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode Varchar: got %d values, want 1", len(decoded))
		}
		if decoded[0].Type() != types.TypeVarchar {
			t.Errorf("Decode Varchar(%q): got type %v, want TypeVarchar", tc.value, decoded[0].Type())
		}
		if decoded[0].Varchar() != tc.value {
			t.Errorf("Decode Varchar: got %q, want %q", decoded[0].Varchar(), tc.value)
		}
		if decoded[0].VarcharMaxLen() != tc.maxLen {
			t.Errorf("Decode Varchar maxLen: got %d, want %d", decoded[0].VarcharMaxLen(), tc.maxLen)
		}
	}
}

func TestEncodeDecode_Char(t *testing.T) {
	testCases := []struct {
		value  string
		length int
		expect string // expected value after padding
	}{
		{"abc", 5, "abc  "},      // padded with spaces
		{"hello", 5, "hello"},    // exact length
		{"toolong", 5, "toolo"},  // truncated
		{"", 3, "   "},           // all spaces
	}

	for _, tc := range testCases {
		values := []types.Value{types.NewChar(tc.value, tc.length)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode Char: got %d values, want 1", len(decoded))
		}
		if decoded[0].Type() != types.TypeChar {
			t.Errorf("Decode Char(%q): got type %v, want TypeChar", tc.value, decoded[0].Type())
		}
		if decoded[0].Char() != tc.expect {
			t.Errorf("Decode Char: got %q, want %q", decoded[0].Char(), tc.expect)
		}
		if decoded[0].CharLen() != tc.length {
			t.Errorf("Decode Char len: got %d, want %d", decoded[0].CharLen(), tc.length)
		}
	}
}

func TestEncodeDecode_MixedStrictTypes(t *testing.T) {
	guid := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0}
	decimal, _ := types.NewDecimal("123.45", 10, 2)

	values := []types.Value{
		types.NewSmallInt(100),
		types.NewInt32(1000000),
		types.NewBigInt(9223372036854775807),
		types.NewSerial(42),
		types.NewBigSerial(123456789012345),
		types.NewGUID(guid),
		decimal,
		types.NewVarchar("hello world", 100),
		types.NewChar("ABC", 5),
	}

	encoded := Encode(values)
	decoded := Decode(encoded)

	if len(decoded) != len(values) {
		t.Fatalf("Decode: got %d values, want %d", len(decoded), len(values))
	}

	// Check SmallInt
	if decoded[0].Type() != types.TypeSmallInt || decoded[0].SmallInt() != 100 {
		t.Errorf("decoded[0] SmallInt: got type %v, value %d", decoded[0].Type(), decoded[0].SmallInt())
	}

	// Check Int32
	if decoded[1].Type() != types.TypeInt32 || decoded[1].Int32() != 1000000 {
		t.Errorf("decoded[1] Int32: got type %v, value %d", decoded[1].Type(), decoded[1].Int32())
	}

	// Check BigInt
	if decoded[2].Type() != types.TypeBigInt || decoded[2].BigInt() != 9223372036854775807 {
		t.Errorf("decoded[2] BigInt: got type %v, value %d", decoded[2].Type(), decoded[2].BigInt())
	}

	// Check Serial
	if decoded[3].Type() != types.TypeSerial || decoded[3].Serial() != 42 {
		t.Errorf("decoded[3] Serial: got type %v, value %d", decoded[3].Type(), decoded[3].Serial())
	}

	// Check BigSerial
	if decoded[4].Type() != types.TypeBigSerial || decoded[4].BigSerial() != 123456789012345 {
		t.Errorf("decoded[4] BigSerial: got type %v, value %d", decoded[4].Type(), decoded[4].BigSerial())
	}

	// Check GUID
	if decoded[5].Type() != types.TypeGUID || decoded[5].GUID() != guid {
		t.Errorf("decoded[5] GUID: got type %v, value %v", decoded[5].Type(), decoded[5].GUID())
	}

	// Check Decimal
	if decoded[6].Type() != types.TypeDecimal || decoded[6].DecimalString() != "123.45" {
		t.Errorf("decoded[6] Decimal: got type %v, value %s", decoded[6].Type(), decoded[6].DecimalString())
	}

	// Check Varchar
	if decoded[7].Type() != types.TypeVarchar || decoded[7].Varchar() != "hello world" {
		t.Errorf("decoded[7] Varchar: got type %v, value %q", decoded[7].Type(), decoded[7].Varchar())
	}

	// Check Char
	if decoded[8].Type() != types.TypeChar || decoded[8].Char() != "ABC  " {
		t.Errorf("decoded[8] Char: got type %v, value %q", decoded[8].Type(), decoded[8].Char())
	}
}

func TestSerialTypeFor_StrictTypes(t *testing.T) {
	// Verify that strict integer types get appropriate serial types
	// SmallInt should fit in 2 bytes
	smallIntST := SerialTypeFor(types.NewSmallInt(32767))
	if smallIntST == SerialTypeNull {
		t.Error("SerialTypeFor(SmallInt) should not return NULL serial type")
	}

	// Int32 should fit in 4 bytes
	int32ST := SerialTypeFor(types.NewInt32(2147483647))
	if int32ST == SerialTypeNull {
		t.Error("SerialTypeFor(Int32) should not return NULL serial type")
	}

	// BigInt should fit in 8 bytes
	bigIntST := SerialTypeFor(types.NewBigInt(9223372036854775807))
	if bigIntST == SerialTypeNull {
		t.Error("SerialTypeFor(BigInt) should not return NULL serial type")
	}

	// GUID should be 16 bytes (blob)
	guidST := SerialTypeFor(types.NewGUID([16]byte{}))
	if guidST == SerialTypeNull {
		t.Error("SerialTypeFor(GUID) should not return NULL serial type")
	}

	// Decimal is stored as blob
	decimal, _ := types.NewDecimal("123.45", 10, 2)
	decimalST := SerialTypeFor(decimal)
	if decimalST == SerialTypeNull {
		t.Error("SerialTypeFor(Decimal) should not return NULL serial type")
	}

	// Varchar is stored as text
	varcharST := SerialTypeFor(types.NewVarchar("test", 100))
	if varcharST == SerialTypeNull {
		t.Error("SerialTypeFor(Varchar) should not return NULL serial type")
	}

	// Char is stored as text
	charST := SerialTypeFor(types.NewChar("ABC", 5))
	if charST == SerialTypeNull {
		t.Error("SerialTypeFor(Char) should not return NULL serial type")
	}
}

// Test that GUID bytes are preserved correctly
func TestEncodeDecode_GUIDBytes(t *testing.T) {
	guid := [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	values := []types.Value{types.NewGUID(guid)}

	encoded := Encode(values)
	decoded := Decode(encoded)

	if len(decoded) != 1 {
		t.Fatalf("Decode: got %d values, want 1", len(decoded))
	}

	decodedGUID := decoded[0].GUID()
	if !bytes.Equal(decodedGUID[:], guid[:]) {
		t.Errorf("GUID bytes mismatch: got %v, want %v", decodedGUID, guid)
	}
}
