package record

import (
	"bytes"
	"testing"

	"tur/pkg/types"
)

func TestSerialType_Null(t *testing.T) {
	st := SerialTypeFor(types.NewNull())
	if st != 0 {
		t.Errorf("SerialTypeFor(NULL): got %d, want 0", st)
	}
	if SerialTypeSize(0) != 0 {
		t.Errorf("SerialTypeSize(0): got %d, want 0", SerialTypeSize(0))
	}
}

func TestSerialType_Int(t *testing.T) {
	tests := []struct {
		value      int64
		serialType uint64
		size       int
	}{
		{0, 8, 0},   // constant 0
		{1, 9, 0},   // constant 1
		{127, 1, 1}, // fits in 1 byte
		{-128, 1, 1},
		{128, 2, 2}, // fits in 2 bytes
		{32767, 2, 2},
		{-32768, 2, 2},
		{32768, 3, 3}, // fits in 3 bytes
		{8388607, 3, 3},
		{-8388608, 3, 3},
		{8388608, 4, 4}, // fits in 4 bytes
		{2147483647, 4, 4},
		{-2147483648, 4, 4},
		{2147483648, 5, 6}, // fits in 6 bytes (skip 5-byte)
		{140737488355327, 5, 6},
		{-140737488355328, 5, 6},
		{140737488355328, 6, 8}, // fits in 8 bytes
	}

	for _, tt := range tests {
		v := types.NewInt(tt.value)
		st := SerialTypeFor(v)
		if st != tt.serialType {
			t.Errorf("SerialTypeFor(%d): got %d, want %d", tt.value, st, tt.serialType)
		}
		if SerialTypeSize(st) != tt.size {
			t.Errorf("SerialTypeSize(%d) for value %d: got %d, want %d", st, tt.value, SerialTypeSize(st), tt.size)
		}
	}
}

func TestSerialType_Float(t *testing.T) {
	v := types.NewFloat(3.14)
	st := SerialTypeFor(v)
	if st != 7 {
		t.Errorf("SerialTypeFor(float): got %d, want 7", st)
	}
	if SerialTypeSize(7) != 8 {
		t.Errorf("SerialTypeSize(7): got %d, want 8", SerialTypeSize(7))
	}
}

func TestSerialType_Text(t *testing.T) {
	tests := []struct {
		text       string
		serialType uint64
		size       int
	}{
		{"", 13, 0},
		{"a", 15, 1},
		{"hello", 23, 5},
	}

	for _, tt := range tests {
		v := types.NewText(tt.text)
		st := SerialTypeFor(v)
		if st != tt.serialType {
			t.Errorf("SerialTypeFor(%q): got %d, want %d", tt.text, st, tt.serialType)
		}
		if SerialTypeSize(st) != tt.size {
			t.Errorf("SerialTypeSize(%d): got %d, want %d", st, SerialTypeSize(st), tt.size)
		}
	}
}

func TestSerialType_Blob(t *testing.T) {
	tests := []struct {
		blob       []byte
		serialType uint64
		size       int
	}{
		{[]byte{}, 12, 0},
		{[]byte{0x01}, 14, 1},
		{[]byte{0x01, 0x02, 0x03, 0x04, 0x05}, 22, 5},
	}

	for _, tt := range tests {
		v := types.NewBlob(tt.blob)
		st := SerialTypeFor(v)
		if st != tt.serialType {
			t.Errorf("SerialTypeFor(blob len %d): got %d, want %d", len(tt.blob), st, tt.serialType)
		}
		if SerialTypeSize(st) != tt.size {
			t.Errorf("SerialTypeSize(%d): got %d, want %d", st, SerialTypeSize(st), tt.size)
		}
	}
}

func TestEncodeDecode_Null(t *testing.T) {
	values := []types.Value{types.NewNull()}
	encoded := Encode(values)
	decoded := Decode(encoded)

	if len(decoded) != 1 {
		t.Fatalf("Decode: got %d values, want 1", len(decoded))
	}
	if !decoded[0].IsNull() {
		t.Errorf("Decode: expected NULL, got type %v", decoded[0].Type())
	}
}

func TestEncodeDecode_Int(t *testing.T) {
	testInts := []int64{0, 1, -1, 127, -128, 128, 32767, -32768, 2147483647, -2147483648}

	for _, i := range testInts {
		values := []types.Value{types.NewInt(i)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode(%d): got %d values, want 1", i, len(decoded))
		}
		if decoded[0].Type() != types.TypeInt32 {
			t.Errorf("Decode(%d): got type %v, want TypeInt32", i, decoded[0].Type())
		}
		if decoded[0].Int() != i {
			t.Errorf("Decode(%d): got %d", i, decoded[0].Int())
		}
	}
}

func TestEncodeDecode_Float(t *testing.T) {
	testFloats := []float64{0.0, 1.0, -1.0, 3.14159, 1e100, -1e-100}

	for _, f := range testFloats {
		values := []types.Value{types.NewFloat(f)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode(%f): got %d values, want 1", f, len(decoded))
		}
		if decoded[0].Type() != types.TypeFloat {
			t.Errorf("Decode(%f): got type %v, want TypeFloat", f, decoded[0].Type())
		}
		if decoded[0].Float() != f {
			t.Errorf("Decode(%f): got %f", f, decoded[0].Float())
		}
	}
}

func TestEncodeDecode_Text(t *testing.T) {
	testStrings := []string{"", "hello", "world", "hello, world!", "日本語"}

	for _, s := range testStrings {
		values := []types.Value{types.NewText(s)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode(%q): got %d values, want 1", s, len(decoded))
		}
		if decoded[0].Type() != types.TypeText {
			t.Errorf("Decode(%q): got type %v, want TypeText", s, decoded[0].Type())
		}
		if decoded[0].Text() != s {
			t.Errorf("Decode(%q): got %q", s, decoded[0].Text())
		}
	}
}

func TestEncodeDecode_Blob(t *testing.T) {
	testBlobs := [][]byte{{}, {0x00}, {0x01, 0x02, 0x03}, {0xFF, 0xFE, 0xFD, 0xFC}}

	for _, b := range testBlobs {
		values := []types.Value{types.NewBlob(b)}
		encoded := Encode(values)
		decoded := Decode(encoded)

		if len(decoded) != 1 {
			t.Fatalf("Decode(blob len %d): got %d values, want 1", len(b), len(decoded))
		}
		if decoded[0].Type() != types.TypeBlob {
			t.Errorf("Decode(blob len %d): got type %v, want TypeBlob", len(b), decoded[0].Type())
		}
		if !bytes.Equal(decoded[0].Blob(), b) {
			t.Errorf("Decode(blob): got %v, want %v", decoded[0].Blob(), b)
		}
	}
}

func TestEncodeDecode_MultipleValues(t *testing.T) {
	values := []types.Value{
		types.NewInt(42),
		types.NewText("hello"),
		types.NewFloat(3.14),
		types.NewNull(),
		types.NewBlob([]byte{0xDE, 0xAD, 0xBE, 0xEF}),
	}

	encoded := Encode(values)
	decoded := Decode(encoded)

	if len(decoded) != len(values) {
		t.Fatalf("Decode: got %d values, want %d", len(decoded), len(values))
	}

	if decoded[0].Int() != 42 {
		t.Errorf("decoded[0]: got %d, want 42", decoded[0].Int())
	}
	if decoded[1].Text() != "hello" {
		t.Errorf("decoded[1]: got %q, want 'hello'", decoded[1].Text())
	}
	if decoded[2].Float() != 3.14 {
		t.Errorf("decoded[2]: got %f, want 3.14", decoded[2].Float())
	}
	if !decoded[3].IsNull() {
		t.Errorf("decoded[3]: expected NULL")
	}
	if !bytes.Equal(decoded[4].Blob(), []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Errorf("decoded[4]: got %v", decoded[4].Blob())
	}
}

func TestEncodeDecode_EmptyRecord(t *testing.T) {
	values := []types.Value{}
	encoded := Encode(values)
	decoded := Decode(encoded)

	if len(decoded) != 0 {
		t.Fatalf("Decode: got %d values, want 0", len(decoded))
	}
}
