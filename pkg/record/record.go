// pkg/record/record.go
package record

import (
	"encoding/binary"
	"math"
	"math/big"
	"strings"

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

	// Extended serial types for strict types (using very high values to avoid conflicts)
	// These are TurDB extensions beyond SQLite's serial types.
	// We use 0x40000000+ to ensure no collision with TEXT/BLOB serial types
	// (which would require strings of 500+ million chars to reach this range)
	SerialTypeSmallInt  = 0x40000000 // 2-byte signed integer (strict SMALLINT)
	SerialTypeInt32Ext  = 0x40000001 // 4-byte signed integer (strict INT)
	SerialTypeBigInt    = 0x40000002 // 8-byte signed integer (strict BIGINT)
	SerialTypeSerial    = 0x40000003 // Auto-incrementing 4-byte integer
	SerialTypeBigSerial = 0x40000004 // Auto-incrementing 8-byte integer
	SerialTypeGUID      = 0x40000005 // 16-byte UUID/GUID
	SerialTypeDecimal   = 0x40000006 // Variable-length decimal (header byte indicates size)
	SerialTypeVarchar   = 0x40000007 // Variable-length string with max length metadata
	SerialTypeChar      = 0x40000008 // Fixed-length string
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
	case types.TypeJSON:
		// JSON is stored as TEXT in the record format
		return SerialTypeText0 + uint64(len(v.JSON()))*2
	case types.TypeBlob:
		return SerialTypeBlob0 + uint64(len(v.Blob()))*2

	// Strict integer types
	case types.TypeSmallInt:
		return SerialTypeSmallInt
	case types.TypeInt32:
		return SerialTypeInt32Ext
	case types.TypeBigInt:
		return SerialTypeBigInt
	case types.TypeSerial:
		return SerialTypeSerial
	case types.TypeBigSerial:
		return SerialTypeBigSerial

	// Other strict types
	case types.TypeGUID:
		return SerialTypeGUID
	case types.TypeDecimal:
		return SerialTypeDecimal
	case types.TypeVarchar:
		return SerialTypeVarchar
	case types.TypeChar:
		return SerialTypeChar

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
// For variable-length strict types, this returns -1 (size encoded in data)
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

	// Strict integer types (fixed sizes)
	case SerialTypeSmallInt:
		return 2
	case SerialTypeInt32Ext, SerialTypeSerial:
		return 4
	case SerialTypeBigInt, SerialTypeBigSerial:
		return 8
	case SerialTypeGUID:
		return 16

	// Variable-length strict types (size encoded in data)
	case SerialTypeDecimal, SerialTypeVarchar, SerialTypeChar:
		return -1 // Size is determined during encode/decode

	default:
		// TEXT/BLOB serial types: >= 12 and < strict type range
		// Even values >= 12 are BLOB, odd values >= 13 are TEXT
		// Strict types are now at 0x40000000+ so no overlap with reasonable TEXT/BLOB
		if st >= SerialTypeBlob0 && st < SerialTypeSmallInt {
			if st&1 == 0 { // even = blob
				return int((st - 12) / 2)
			}
			// odd = text
			return int((st - 13) / 2)
		}
		return 0
	}
}

// encodedValueSize returns the size needed to encode a value
func encodedValueSize(v types.Value, st uint64) int {
	size := SerialTypeSize(st)
	if size >= 0 {
		return size
	}

	// Variable-length strict types
	switch st {
	case SerialTypeDecimal:
		// Format: [len:2][blob data...]
		return 2 + len(v.Blob())
	case SerialTypeVarchar:
		// Format: [maxLen:2][strLen:2][string data...]
		return 4 + len(v.Varchar())
	case SerialTypeChar:
		// Format: [charLen:2][string data...]
		return 2 + len(v.Char())
	default:
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
		dataSize += encodedValueSize(v, st)
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

	// Strict integer types
	case SerialTypeSmallInt:
		binary.BigEndian.PutUint16(buf, uint16(v.SmallInt()))
		return 2
	case SerialTypeInt32Ext:
		binary.BigEndian.PutUint32(buf, uint32(v.Int32()))
		return 4
	case SerialTypeBigInt:
		binary.BigEndian.PutUint64(buf, uint64(v.BigInt()))
		return 8
	case SerialTypeSerial:
		binary.BigEndian.PutUint32(buf, uint32(v.Serial()))
		return 4
	case SerialTypeBigSerial:
		binary.BigEndian.PutUint64(buf, uint64(v.BigSerial()))
		return 8

	// GUID (16 bytes)
	case SerialTypeGUID:
		guid := v.GUID()
		copy(buf, guid[:])
		return 16

	// Decimal: [len:2][blob data...]
	case SerialTypeDecimal:
		blob := v.Blob()
		binary.BigEndian.PutUint16(buf, uint16(len(blob)))
		copy(buf[2:], blob)
		return 2 + len(blob)

	// Varchar: [maxLen:2][strLen:2][string data...]
	case SerialTypeVarchar:
		str := v.Varchar()
		maxLen := v.VarcharMaxLen()
		binary.BigEndian.PutUint16(buf, uint16(maxLen))
		binary.BigEndian.PutUint16(buf[2:], uint16(len(str)))
		copy(buf[4:], str)
		return 4 + len(str)

	// Char: [charLen:2][string data...]
	case SerialTypeChar:
		str := v.Char()
		charLen := v.CharLen()
		binary.BigEndian.PutUint16(buf, uint16(charLen))
		copy(buf[2:], str)
		return 2 + len(str)

	default:
		// Text or Blob (standard SQLite types)
		size := SerialTypeSize(st)
		if size < 0 {
			return 0
		}
		if st&1 == 0 { // even = blob
			copy(buf, v.Blob())
		} else { // odd = text or JSON (both stored as text)
			if v.Type() == types.TypeJSON {
				copy(buf, v.JSON())
			} else {
				copy(buf, v.Text())
			}
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

	// Strict integer types
	case SerialTypeSmallInt:
		v := int16(binary.BigEndian.Uint16(data[pos:]))
		return types.NewSmallInt(v), pos + 2
	case SerialTypeInt32Ext:
		v := int32(binary.BigEndian.Uint32(data[pos:]))
		return types.NewInt32(v), pos + 4
	case SerialTypeBigInt:
		v := int64(binary.BigEndian.Uint64(data[pos:]))
		return types.NewBigInt(v), pos + 8
	case SerialTypeSerial:
		v := int32(binary.BigEndian.Uint32(data[pos:]))
		return types.NewSerial(v), pos + 4
	case SerialTypeBigSerial:
		v := int64(binary.BigEndian.Uint64(data[pos:]))
		return types.NewBigSerial(v), pos + 8

	// GUID (16 bytes)
	case SerialTypeGUID:
		var guid [16]byte
		copy(guid[:], data[pos:pos+16])
		return types.NewGUID(guid), pos + 16

	// Decimal: [len:2][blob data...]
	case SerialTypeDecimal:
		blobLen := int(binary.BigEndian.Uint16(data[pos:]))
		blob := make([]byte, blobLen)
		copy(blob, data[pos+2:pos+2+blobLen])
		// Reconstruct Decimal value from blob
		return decodeDecimalFromBlob(blob), pos + 2 + blobLen

	// Varchar: [maxLen:2][strLen:2][string data...]
	case SerialTypeVarchar:
		maxLen := int(binary.BigEndian.Uint16(data[pos:]))
		strLen := int(binary.BigEndian.Uint16(data[pos+2:]))
		str := string(data[pos+4 : pos+4+strLen])
		return types.NewVarchar(str, maxLen), pos + 4 + strLen

	// Char: [charLen:2][string data...]
	case SerialTypeChar:
		charLen := int(binary.BigEndian.Uint16(data[pos:]))
		str := string(data[pos+2 : pos+2+charLen])
		return types.NewChar(str, charLen), pos + 2 + charLen

	default:
		size := SerialTypeSize(st)
		if size < 0 {
			return types.NewNull(), pos
		}
		if st&1 == 0 { // even = blob
			blob := make([]byte, size)
			copy(blob, data[pos:pos+size])
			return types.NewBlob(blob), pos + size
		}
		// odd = text
		return types.NewText(string(data[pos : pos+size])), pos + size
	}
}

// decodeDecimalFromBlob reconstructs a Decimal value from the encoded blob
func decodeDecimalFromBlob(blob []byte) types.Value {
	if len(blob) < 5 {
		return types.NewNull()
	}

	negative := blob[0] == 1
	precision := int(blob[1])
	scale := int(blob[2])
	coeffLen := int(blob[3])<<8 | int(blob[4])

	if len(blob) < 5+coeffLen {
		return types.NewNull()
	}

	// Reconstruct coefficient
	coeff := new(big.Int).SetBytes(blob[5 : 5+coeffLen])
	if negative {
		coeff.Neg(coeff)
	}

	// Format as string
	str := formatDecimalCoeff(coeff, scale)

	// Create the decimal using NewDecimal
	v, err := types.NewDecimal(str, precision, scale)
	if err != nil {
		return types.NewNull()
	}
	return v
}

// formatDecimalCoeff formats a big.Int coefficient with the given scale
func formatDecimalCoeff(coeff *big.Int, scale int) string {
	negative := coeff.Sign() < 0
	absCoeff := new(big.Int).Abs(coeff)
	s := absCoeff.String()

	// Pad with leading zeros if necessary
	if len(s) <= scale {
		s = strings.Repeat("0", scale-len(s)+1) + s
	}

	// Insert decimal point
	var result string
	if scale > 0 {
		intPart := s[:len(s)-scale]
		fracPart := s[len(s)-scale:]
		result = intPart + "." + fracPart
	} else {
		result = s
	}

	if negative {
		result = "-" + result
	}

	return result
}
