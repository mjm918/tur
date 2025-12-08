// pkg/record/record.go
package record

import (
	"encoding/binary"
	"math"

	"tur/pkg/encoding"
	"tur/pkg/types"
)

// Serial type constants following SQLite conventions
const (
	SerialTypeNull  = 0
	SerialTypeInt8  = 1
	SerialTypeInt16 = 2
	SerialTypeInt24 = 3
	SerialTypeInt32 = 4
	SerialTypeInt48 = 5
	SerialTypeInt64 = 6
	SerialTypeFloat = 7
	SerialTypeZero  = 8
	SerialTypeOne   = 9
	SerialTypeBlob0 = 12 // even >= 12 for BLOB
	SerialTypeText0 = 13 // odd >= 13 for TEXT
)

// SerialTypeFor returns the serial type for a value
func SerialTypeFor(v types.Value) uint64 {
	switch v.Type() {
	case types.TypeNull:
		return SerialTypeNull
	case types.TypeInt:
		return serialTypeForInt(v.Int())
	case types.TypeFloat:
		return SerialTypeFloat
	case types.TypeText:
		return SerialTypeText0 + uint64(len(v.Text()))*2
	case types.TypeBlob:
		return SerialTypeBlob0 + uint64(len(v.Blob()))*2
	default:
		return SerialTypeNull
	}
}

// serialTypeForInt returns the smallest serial type that can hold the integer
func serialTypeForInt(i int64) uint64 {
	if i == 0 {
		return SerialTypeZero
	}
	if i == 1 {
		return SerialTypeOne
	}
	if i >= -128 && i <= 127 {
		return SerialTypeInt8
	}
	if i >= -32768 && i <= 32767 {
		return SerialTypeInt16
	}
	if i >= -8388608 && i <= 8388607 {
		return SerialTypeInt24
	}
	if i >= -2147483648 && i <= 2147483647 {
		return SerialTypeInt32
	}
	if i >= -140737488355328 && i <= 140737488355327 {
		return SerialTypeInt48
	}
	return SerialTypeInt64
}

// SerialTypeSize returns the number of bytes needed to store a value with this serial type
func SerialTypeSize(st uint64) int {
	switch st {
	case SerialTypeNull:
		return 0
	case SerialTypeInt8:
		return 1
	case SerialTypeInt16:
		return 2
	case SerialTypeInt24:
		return 3
	case SerialTypeInt32:
		return 4
	case SerialTypeInt48:
		return 6
	case SerialTypeInt64:
		return 8
	case SerialTypeFloat:
		return 8
	case SerialTypeZero, SerialTypeOne:
		return 0
	default:
		if st >= SerialTypeBlob0 {
			if st&1 == 0 { // even = blob
				return int((st - 12) / 2)
			}
			// odd = text
			return int((st - 13) / 2)
		}
		return 0
	}
}

// Encode encodes values into a record
// Record format: [hdr-size][type0][type1]...[typeN][data0][data1]...[dataN]
func Encode(values []types.Value) []byte {
	if len(values) == 0 {
		// Empty record: just header size (1)
		return []byte{1}
	}

	// Calculate serial types and sizes
	serialTypes := make([]uint64, len(values))
	dataSize := 0
	headerSize := 0

	for i, v := range values {
		st := SerialTypeFor(v)
		serialTypes[i] = st
		dataSize += SerialTypeSize(st)
		headerSize += encoding.VarintLen(st)
	}

	// Header size includes the header-size varint itself
	hdrSizeLen := encoding.VarintLen(uint64(headerSize + 1))
	for hdrSizeLen != encoding.VarintLen(uint64(headerSize+hdrSizeLen)) {
		hdrSizeLen = encoding.VarintLen(uint64(headerSize + hdrSizeLen))
	}
	headerSize += hdrSizeLen

	// Allocate buffer
	buf := make([]byte, headerSize+dataSize)

	// Write header
	pos := encoding.PutVarint(buf, uint64(headerSize))
	for _, st := range serialTypes {
		pos += encoding.PutVarint(buf[pos:], st)
	}

	// Write data
	for i, v := range values {
		pos += encodeValue(buf[pos:], v, serialTypes[i])
	}

	return buf
}

// encodeValue encodes a single value into buf, returns bytes written
func encodeValue(buf []byte, v types.Value, st uint64) int {
	switch st {
	case SerialTypeNull, SerialTypeZero, SerialTypeOne:
		return 0
	case SerialTypeInt8:
		buf[0] = byte(v.Int())
		return 1
	case SerialTypeInt16:
		binary.BigEndian.PutUint16(buf, uint16(v.Int()))
		return 2
	case SerialTypeInt24:
		i := v.Int()
		buf[0] = byte(i >> 16)
		buf[1] = byte(i >> 8)
		buf[2] = byte(i)
		return 3
	case SerialTypeInt32:
		binary.BigEndian.PutUint32(buf, uint32(v.Int()))
		return 4
	case SerialTypeInt48:
		i := v.Int()
		buf[0] = byte(i >> 40)
		buf[1] = byte(i >> 32)
		buf[2] = byte(i >> 24)
		buf[3] = byte(i >> 16)
		buf[4] = byte(i >> 8)
		buf[5] = byte(i)
		return 6
	case SerialTypeInt64:
		binary.BigEndian.PutUint64(buf, uint64(v.Int()))
		return 8
	case SerialTypeFloat:
		binary.BigEndian.PutUint64(buf, math.Float64bits(v.Float()))
		return 8
	default:
		// Text or Blob
		size := SerialTypeSize(st)
		if st&1 == 0 { // even = blob
			copy(buf, v.Blob())
		} else { // odd = text
			copy(buf, v.Text())
		}
		return size
	}
}

// Decode decodes a record into values
func Decode(data []byte) []types.Value {
	if len(data) == 0 {
		return nil
	}

	// Read header size
	headerSize, n := encoding.GetVarint(data)
	if headerSize == 0 || int(headerSize) > len(data) {
		return nil
	}

	// Read serial types from header
	var serialTypes []uint64
	pos := n
	for pos < int(headerSize) {
		st, m := encoding.GetVarint(data[pos:])
		serialTypes = append(serialTypes, st)
		pos += m
	}

	// Decode values
	values := make([]types.Value, len(serialTypes))
	dataPos := int(headerSize)

	for i, st := range serialTypes {
		values[i], dataPos = decodeValue(data, dataPos, st)
	}

	return values
}

// decodeValue decodes a single value from data starting at pos
func decodeValue(data []byte, pos int, st uint64) (types.Value, int) {
	switch st {
	case SerialTypeNull:
		return types.NewNull(), pos
	case SerialTypeZero:
		return types.NewInt(0), pos
	case SerialTypeOne:
		return types.NewInt(1), pos
	case SerialTypeInt8:
		return types.NewInt(int64(int8(data[pos]))), pos + 1
	case SerialTypeInt16:
		v := int16(binary.BigEndian.Uint16(data[pos:]))
		return types.NewInt(int64(v)), pos + 2
	case SerialTypeInt24:
		v := int32(data[pos])<<16 | int32(data[pos+1])<<8 | int32(data[pos+2])
		if v >= 0x800000 {
			v -= 0x1000000 // sign extend
		}
		return types.NewInt(int64(v)), pos + 3
	case SerialTypeInt32:
		v := int32(binary.BigEndian.Uint32(data[pos:]))
		return types.NewInt(int64(v)), pos + 4
	case SerialTypeInt48:
		v := int64(data[pos])<<40 | int64(data[pos+1])<<32 | int64(data[pos+2])<<24 |
			int64(data[pos+3])<<16 | int64(data[pos+4])<<8 | int64(data[pos+5])
		if v >= 0x800000000000 {
			v -= 0x1000000000000 // sign extend
		}
		return types.NewInt(v), pos + 6
	case SerialTypeInt64:
		v := int64(binary.BigEndian.Uint64(data[pos:]))
		return types.NewInt(v), pos + 8
	case SerialTypeFloat:
		bits := binary.BigEndian.Uint64(data[pos:])
		return types.NewFloat(math.Float64frombits(bits)), pos + 8
	default:
		size := SerialTypeSize(st)
		if st&1 == 0 { // even = blob
			blob := make([]byte, size)
			copy(blob, data[pos:pos+size])
			return types.NewBlob(blob), pos + size
		}
		// odd = text
		return types.NewText(string(data[pos : pos+size])), pos + size
	}
}
