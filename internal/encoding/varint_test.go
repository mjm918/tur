// internal/encoding/varint_test.go
package encoding

import "testing"

func TestPutVarint(t *testing.T) {
	tests := []struct {
		value    uint64
		expected []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7f}},
		{128, []byte{0x81, 0x00}},
		{255, []byte{0x81, 0x7f}},
		{16383, []byte{0xff, 0x7f}},
		{16384, []byte{0x81, 0x80, 0x00}},
	}
	for _, tt := range tests {
		buf := make([]byte, 10)
		n := PutVarint(buf, tt.value)
		if n != len(tt.expected) {
			t.Errorf("PutVarint(%d): expected %d bytes, got %d", tt.value, len(tt.expected), n)
		}
		for i := 0; i < n; i++ {
			if buf[i] != tt.expected[i] {
				t.Errorf("PutVarint(%d): byte %d expected %02x, got %02x", tt.value, i, tt.expected[i], buf[i])
			}
		}
	}
}

func TestGetVarint(t *testing.T) {
	tests := []struct {
		input    []byte
		expected uint64
		size     int
	}{
		{[]byte{0x00}, 0, 1},
		{[]byte{0x01}, 1, 1},
		{[]byte{0x7f}, 127, 1},
		{[]byte{0x81, 0x00}, 128, 2},
		{[]byte{0x81, 0x7f}, 255, 2},
		{[]byte{0xff, 0x7f}, 16383, 2},
		{[]byte{0x81, 0x80, 0x00}, 16384, 3},
	}
	for _, tt := range tests {
		val, n := GetVarint(tt.input)
		if val != tt.expected {
			t.Errorf("GetVarint(%v): expected %d, got %d", tt.input, tt.expected, val)
		}
		if n != tt.size {
			t.Errorf("GetVarint(%v): expected size %d, got %d", tt.input, tt.size, n)
		}
	}
}

func TestVarintRoundTrip(t *testing.T) {
	values := []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, 1 << 20, 1 << 30, 1 << 40}
	for _, v := range values {
		buf := make([]byte, 10)
		n := PutVarint(buf, v)
		got, m := GetVarint(buf[:n])
		if got != v || m != n {
			t.Errorf("roundtrip failed for %d: got %d, sizes %d vs %d", v, got, n, m)
		}
	}
}
