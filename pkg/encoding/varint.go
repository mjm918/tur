// pkg/encoding/varint.go
package encoding

// PutVarint encodes v as a varint into buf and returns the number of bytes written.
// The buffer must have at least 9 bytes available.
// Uses SQLite-style varint encoding: lower 7 bits of each byte contain data,
// high bit indicates continuation. The 9th byte (if needed) uses all 8 bits.
func PutVarint(buf []byte, v uint64) int {
	if v <= 127 {
		buf[0] = byte(v)
		return 1
	}

	// Count how many bytes we need
	n := VarintLen(v)

	// Special case: 9-byte encoding
	if n == 9 {
		buf[8] = byte(v & 0xFF) // Last byte uses all 8 bits
		v >>= 8
		for i := 7; i >= 0; i-- {
			buf[i] = byte((v & 0x7F) | 0x80)
			v >>= 7
		}
		return 9
	}

	// Encode from right to left
	for i := n - 1; i >= 0; i-- {
		if i == n-1 {
			buf[i] = byte(v & 0x7F)
		} else {
			buf[i] = byte((v & 0x7F) | 0x80)
		}
		v >>= 7
	}

	return n
}

// GetVarint decodes a varint from buf and returns the value and number of bytes read.
func GetVarint(buf []byte) (uint64, int) {
	var v uint64
	var n int

	for i := 0; i < len(buf) && i < 9; i++ {
		b := buf[i]
		n++

		if i == 8 {
			// 9th byte uses all 8 bits
			v = (v << 8) | uint64(b)
			break
		}

		v = (v << 7) | uint64(b&0x7F)

		if b&0x80 == 0 {
			break
		}
	}

	return v, n
}

// VarintLen returns the number of bytes needed to encode v as a varint.
func VarintLen(v uint64) int {
	if v <= 0x7F {
		return 1
	}
	if v <= 0x3FFF {
		return 2
	}
	if v <= 0x1FFFFF {
		return 3
	}
	if v <= 0xFFFFFFF {
		return 4
	}
	if v <= 0x7FFFFFFFF {
		return 5
	}
	if v <= 0x3FFFFFFFFFF {
		return 6
	}
	if v <= 0x1FFFFFFFFFFFF {
		return 7
	}
	if v <= 0xFFFFFFFFFFFFFF {
		return 8
	}
	return 9
}
