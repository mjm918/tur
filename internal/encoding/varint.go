// internal/encoding/varint.go
package encoding

// PutVarint encodes a uint64 as a variable-length integer (SQLite format)
// Returns the number of bytes written
// Format: each byte has 7 bits of data, high bit set if more bytes follow
func PutVarint(buf []byte, v uint64) int {
	if v <= 127 {
		buf[0] = byte(v)
		return 1
	}

	// Calculate how many bytes we need
	n := 0
	temp := v
	for temp > 0 {
		n++
		temp >>= 7
	}

	// Write bytes in big-endian order with continuation bits
	for i := n - 1; i >= 0; i-- {
		b := byte(v >> (uint(i) * 7) & 0x7f)
		if i > 0 {
			b |= 0x80 // set continuation bit
		}
		buf[n-1-i] = b
	}
	return n
}

// GetVarint decodes a variable-length integer from buf
// Returns the value and the number of bytes read
func GetVarint(buf []byte) (uint64, int) {
	var v uint64
	var n int
	for n = 0; n < len(buf) && n < 9; n++ {
		b := buf[n]
		v = (v << 7) | uint64(b&0x7f)
		if b&0x80 == 0 {
			return v, n + 1
		}
	}
	return v, n
}

// VarintLen returns the number of bytes needed to encode v
func VarintLen(v uint64) int {
	if v <= 127 {
		return 1
	}
	n := 0
	for v > 0 {
		n++
		v >>= 7
	}
	return n
}
