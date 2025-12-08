package record

import "testing"

func TestPutVarint_SingleByte(t *testing.T) {
	tests := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7F}},
	}

	for _, tt := range tests {
		buf := make([]byte, 9)
		n := PutVarint(buf, tt.value)
		if n != len(tt.expected) {
			t.Errorf("PutVarint(%d): got length %d, want %d", tt.value, n, len(tt.expected))
		}
		for i := 0; i < n; i++ {
			if buf[i] != tt.expected[i] {
				t.Errorf("PutVarint(%d): got byte[%d]=%02x, want %02x", tt.value, i, buf[i], tt.expected[i])
			}
		}
	}
}

func TestPutVarint_MultiByte(t *testing.T) {
	tests := []struct {
		value    uint64
		expected []byte
	}{
		{128, []byte{0x81, 0x00}},
		{255, []byte{0x81, 0x7F}},
		{16383, []byte{0xFF, 0x7F}},
		{16384, []byte{0x81, 0x80, 0x00}},
	}

	for _, tt := range tests {
		buf := make([]byte, 9)
		n := PutVarint(buf, tt.value)
		if n != len(tt.expected) {
			t.Errorf("PutVarint(%d): got length %d, want %d", tt.value, n, len(tt.expected))
		}
		for i := 0; i < n; i++ {
			if buf[i] != tt.expected[i] {
				t.Errorf("PutVarint(%d): got byte[%d]=%02x, want %02x", tt.value, i, buf[i], tt.expected[i])
			}
		}
	}
}

func TestGetVarint_SingleByte(t *testing.T) {
	tests := []struct {
		input    []byte
		expected uint64
		length   int
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0x01}, 1, 1},
		{[]byte{0x7F}, 127, 1},
	}

	for _, tt := range tests {
		v, n := GetVarint(tt.input)
		if v != tt.expected || n != tt.length {
			t.Errorf("GetVarint(%v): got (%d, %d), want (%d, %d)", tt.input, v, n, tt.expected, tt.length)
		}
	}
}

func TestGetVarint_MultiByte(t *testing.T) {
	tests := []struct {
		input    []byte
		expected uint64
		length   int
	}{
		{[]byte{0x81, 0x00}, 128, 2},
		{[]byte{0x81, 0x7F}, 255, 2},
		{[]byte{0xFF, 0x7F}, 16383, 2},
		{[]byte{0x81, 0x80, 0x00}, 16384, 3},
	}

	for _, tt := range tests {
		v, n := GetVarint(tt.input)
		if v != tt.expected || n != tt.length {
			t.Errorf("GetVarint(%v): got (%d, %d), want (%d, %d)", tt.input, v, n, tt.expected, tt.length)
		}
	}
}

func TestVarintRoundTrip(t *testing.T) {
	values := []uint64{
		0, 1, 127, 128, 255, 256, 16383, 16384,
		0x7FFFFFFF, 0xFFFFFFFF, 0x7FFFFFFFFFFFFFFF,
	}

	for _, v := range values {
		buf := make([]byte, 9)
		n := PutVarint(buf, v)
		got, m := GetVarint(buf[:n])
		if got != v || m != n {
			t.Errorf("Roundtrip(%d): put %d bytes, got (%d, %d)", v, n, got, m)
		}
	}
}

func TestVarintLength(t *testing.T) {
	tests := []struct {
		value  uint64
		length int
	}{
		{0, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
		{0x1FFFFF, 3},
		{0x200000, 4},
	}

	for _, tt := range tests {
		n := VarintLength(tt.value)
		if n != tt.length {
			t.Errorf("VarintLength(%d): got %d, want %d", tt.value, n, tt.length)
		}
	}
}
