// pkg/record/record_view.go
package record

import (
	"encoding/binary"
	"math"
	"math/big"
	"strings"
	"unsafe"

	"tur/pkg/encoding"
	"tur/pkg/types"
)

// RecordView provides zero-copy access to record data stored in mmap'd memory.
// It lazily decodes values on demand, avoiding allocations for fields that
// are never accessed.
type RecordView struct {
	data        []byte   // Points directly into mmap (no copy)
	headerSize  int      // Size of the header in bytes
	serialTypes []uint64 // Cached serial types from header
	offsets     []int    // Pre-computed byte offset of each column's data
	decoded     bool     // Whether we've parsed the header
}

// NewRecordView creates a RecordView from raw record data.
// The data slice should point directly into mmap'd memory for zero-copy access.
func NewRecordView(data []byte) *RecordView {
	return &RecordView{
		data: data,
	}
}

// ensureDecoded parses the header and computes offsets if not already done.
func (r *RecordView) ensureDecoded() {
	if r.decoded || len(r.data) == 0 {
		return
	}
	r.decoded = true

	// Read header size
	headerSize, n := encoding.GetVarint(r.data)
	if headerSize == 0 || int(headerSize) > len(r.data) {
		return
	}
	r.headerSize = int(headerSize)

	// Read serial types from header
	pos := n
	for pos < r.headerSize {
		st, m := encoding.GetVarint(r.data[pos:])
		r.serialTypes = append(r.serialTypes, st)
		pos += m
	}

	// Pre-compute data offsets for each column
	r.offsets = make([]int, len(r.serialTypes))
	dataPos := r.headerSize
	for i, st := range r.serialTypes {
		r.offsets[i] = dataPos
		dataPos += r.columnSize(st, dataPos)
	}
}

// columnSize returns the size of a column's data given its serial type and position.
func (r *RecordView) columnSize(st uint64, pos int) int {
	size := SerialTypeSize(st)
	if size >= 0 {
		return size
	}

	// Variable-length strict types need to read size from data
	switch st {
	case SerialTypeDecimal:
		if pos+2 <= len(r.data) {
			blobLen := int(binary.BigEndian.Uint16(r.data[pos:]))
			return 2 + blobLen
		}
		return 0
	case SerialTypeVarchar:
		if pos+4 <= len(r.data) {
			strLen := int(binary.BigEndian.Uint16(r.data[pos+2:]))
			return 4 + strLen
		}
		return 0
	case SerialTypeChar:
		if pos+2 <= len(r.data) {
			charLen := int(binary.BigEndian.Uint16(r.data[pos:]))
			return 2 + charLen
		}
		return 0
	default:
		return 0
	}
}

// ColumnCount returns the number of columns in the record.
func (r *RecordView) ColumnCount() int {
	r.ensureDecoded()
	return len(r.serialTypes)
}

// IsNull returns true if the column at the given index is NULL.
func (r *RecordView) IsNull(col int) bool {
	r.ensureDecoded()
	if col < 0 || col >= len(r.serialTypes) {
		return true
	}
	return r.serialTypes[col] == SerialTypeNull
}

// GetInt returns the integer value at the given column index.
// Returns 0 if the column is not an integer type.
func (r *RecordView) GetInt(col int) int64 {
	r.ensureDecoded()
	if col < 0 || col >= len(r.serialTypes) {
		return 0
	}

	st := r.serialTypes[col]
	pos := r.offsets[col]

	switch st {
	case SerialTypeNull:
		return 0
	case SerialTypeZero:
		return 0
	case SerialTypeOne:
		return 1
	case SerialTypeInt8:
		return int64(int8(r.data[pos]))
	case SerialTypeInt16:
		return int64(int16(binary.BigEndian.Uint16(r.data[pos:])))
	case SerialTypeInt24:
		v := int32(r.data[pos])<<16 | int32(r.data[pos+1])<<8 | int32(r.data[pos+2])
		if v >= 0x800000 {
			v -= 0x1000000
		}
		return int64(v)
	case SerialTypeInt32:
		return int64(int32(binary.BigEndian.Uint32(r.data[pos:])))
	case SerialTypeInt48:
		v := int64(r.data[pos])<<40 | int64(r.data[pos+1])<<32 | int64(r.data[pos+2])<<24 |
			int64(r.data[pos+3])<<16 | int64(r.data[pos+4])<<8 | int64(r.data[pos+5])
		if v >= 0x800000000000 {
			v -= 0x1000000000000
		}
		return v
	case SerialTypeInt64:
		return int64(binary.BigEndian.Uint64(r.data[pos:]))
	case SerialTypeSmallInt:
		return int64(int16(binary.BigEndian.Uint16(r.data[pos:])))
	case SerialTypeInt32Ext:
		return int64(int32(binary.BigEndian.Uint32(r.data[pos:])))
	case SerialTypeBigInt:
		return int64(binary.BigEndian.Uint64(r.data[pos:]))
	case SerialTypeSerial:
		return int64(int32(binary.BigEndian.Uint32(r.data[pos:])))
	case SerialTypeBigSerial:
		return int64(binary.BigEndian.Uint64(r.data[pos:]))
	default:
		return 0
	}
}

// GetFloat returns the float value at the given column index.
func (r *RecordView) GetFloat(col int) float64 {
	r.ensureDecoded()
	if col < 0 || col >= len(r.serialTypes) {
		return 0
	}

	st := r.serialTypes[col]
	if st != SerialTypeFloat {
		return 0
	}

	pos := r.offsets[col]
	bits := binary.BigEndian.Uint64(r.data[pos:])
	return math.Float64frombits(bits)
}

// GetTextUnsafe returns the text value at the given column index as a zero-copy string.
// WARNING: The returned string is only valid as long as the underlying mmap is valid.
// Do not store the string beyond the lifetime of the database operation.
func (r *RecordView) GetTextUnsafe(col int) string {
	r.ensureDecoded()
	if col < 0 || col >= len(r.serialTypes) {
		return ""
	}

	st := r.serialTypes[col]
	pos := r.offsets[col]

	// Standard TEXT type (odd serial type >= 13)
	if st >= SerialTypeText0 && st&1 == 1 {
		size := int((st - 13) / 2)
		if pos+size > len(r.data) {
			return ""
		}
		return unsafeString(r.data[pos : pos+size])
	}

	return ""
}

// GetText returns a safe copy of the text value at the given column index.
func (r *RecordView) GetText(col int) string {
	r.ensureDecoded()
	if col < 0 || col >= len(r.serialTypes) {
		return ""
	}

	st := r.serialTypes[col]
	pos := r.offsets[col]

	// Standard TEXT type (odd serial type >= 13)
	if st >= SerialTypeText0 && st&1 == 1 {
		size := int((st - 13) / 2)
		if pos+size > len(r.data) {
			return ""
		}
		return string(r.data[pos : pos+size])
	}

	return ""
}

// GetBlob returns the blob value at the given column index.
// Returns a slice into the original data (zero-copy).
func (r *RecordView) GetBlobUnsafe(col int) []byte {
	r.ensureDecoded()
	if col < 0 || col >= len(r.serialTypes) {
		return nil
	}

	st := r.serialTypes[col]
	pos := r.offsets[col]

	// Standard BLOB type (even serial type >= 12)
	if st >= SerialTypeBlob0 && st&1 == 0 {
		size := int((st - 12) / 2)
		if pos+size > len(r.data) {
			return nil
		}
		return r.data[pos : pos+size]
	}

	return nil
}

// GetBlob returns a safe copy of the blob value.
func (r *RecordView) GetBlob(col int) []byte {
	unsafe := r.GetBlobUnsafe(col)
	if unsafe == nil {
		return nil
	}
	result := make([]byte, len(unsafe))
	copy(result, unsafe)
	return result
}

// GetValue returns the value at the given column index as a types.Value.
// This allocates memory for the value, use Get* methods for zero-copy access.
func (r *RecordView) GetValue(col int) types.Value {
	r.ensureDecoded()
	if col < 0 || col >= len(r.serialTypes) {
		return types.NewNull()
	}

	st := r.serialTypes[col]
	pos := r.offsets[col]

	return r.decodeValueAt(st, pos)
}

// decodeValueAt decodes a value at the given position with the given serial type.
func (r *RecordView) decodeValueAt(st uint64, pos int) types.Value {
	switch st {
	case SerialTypeNull:
		return types.NewNull()
	case SerialTypeZero:
		return types.NewInt(0)
	case SerialTypeOne:
		return types.NewInt(1)
	case SerialTypeInt8:
		return types.NewInt(int64(int8(r.data[pos])))
	case SerialTypeInt16:
		v := int16(binary.BigEndian.Uint16(r.data[pos:]))
		return types.NewInt(int64(v))
	case SerialTypeInt24:
		v := int32(r.data[pos])<<16 | int32(r.data[pos+1])<<8 | int32(r.data[pos+2])
		if v >= 0x800000 {
			v -= 0x1000000
		}
		return types.NewInt(int64(v))
	case SerialTypeInt32:
		v := int32(binary.BigEndian.Uint32(r.data[pos:]))
		return types.NewInt(int64(v))
	case SerialTypeInt48:
		v := int64(r.data[pos])<<40 | int64(r.data[pos+1])<<32 | int64(r.data[pos+2])<<24 |
			int64(r.data[pos+3])<<16 | int64(r.data[pos+4])<<8 | int64(r.data[pos+5])
		if v >= 0x800000000000 {
			v -= 0x1000000000000
		}
		return types.NewInt(v)
	case SerialTypeInt64:
		v := int64(binary.BigEndian.Uint64(r.data[pos:]))
		return types.NewInt(v)
	case SerialTypeFloat:
		bits := binary.BigEndian.Uint64(r.data[pos:])
		return types.NewFloat(math.Float64frombits(bits))

	// Strict integer types
	case SerialTypeSmallInt:
		v := int16(binary.BigEndian.Uint16(r.data[pos:]))
		return types.NewSmallInt(v)
	case SerialTypeInt32Ext:
		v := int32(binary.BigEndian.Uint32(r.data[pos:]))
		return types.NewInt32(v)
	case SerialTypeBigInt:
		v := int64(binary.BigEndian.Uint64(r.data[pos:]))
		return types.NewBigInt(v)
	case SerialTypeSerial:
		v := int32(binary.BigEndian.Uint32(r.data[pos:]))
		return types.NewSerial(v)
	case SerialTypeBigSerial:
		v := int64(binary.BigEndian.Uint64(r.data[pos:]))
		return types.NewBigSerial(v)

	// GUID (16 bytes)
	case SerialTypeGUID:
		var guid [16]byte
		copy(guid[:], r.data[pos:pos+16])
		return types.NewGUID(guid)

	// Decimal: [len:2][blob data...]
	case SerialTypeDecimal:
		blobLen := int(binary.BigEndian.Uint16(r.data[pos:]))
		blob := r.data[pos+2 : pos+2+blobLen]
		return decodeDecimalFromBlobView(blob)

	// Varchar: [maxLen:2][strLen:2][string data...]
	case SerialTypeVarchar:
		maxLen := int(binary.BigEndian.Uint16(r.data[pos:]))
		strLen := int(binary.BigEndian.Uint16(r.data[pos+2:]))
		str := string(r.data[pos+4 : pos+4+strLen])
		return types.NewVarchar(str, maxLen)

	// Char: [charLen:2][string data...]
	case SerialTypeChar:
		charLen := int(binary.BigEndian.Uint16(r.data[pos:]))
		str := string(r.data[pos+2 : pos+2+charLen])
		return types.NewChar(str, charLen)

	default:
		size := SerialTypeSize(st)
		if size < 0 {
			return types.NewNull()
		}
		if st&1 == 0 { // even = blob
			blob := make([]byte, size)
			copy(blob, r.data[pos:pos+size])
			return types.NewBlob(blob)
		}
		// odd = text
		return types.NewText(string(r.data[pos : pos+size]))
	}
}

// ToValues converts the entire record to a slice of types.Value.
// This is equivalent to Decode() but uses the RecordView internally.
func (r *RecordView) ToValues() []types.Value {
	r.ensureDecoded()
	if len(r.serialTypes) == 0 {
		return nil
	}

	values := make([]types.Value, len(r.serialTypes))
	for i, st := range r.serialTypes {
		values[i] = r.decodeValueAt(st, r.offsets[i])
	}
	return values
}

// decodeDecimalFromBlobView reconstructs a Decimal value from the encoded blob (zero-copy)
func decodeDecimalFromBlobView(blob []byte) types.Value {
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
	str := formatDecimalCoeffView(coeff, scale)

	// Create the decimal using NewDecimal
	v, err := types.NewDecimal(str, precision, scale)
	if err != nil {
		return types.NewNull()
	}
	return v
}

// formatDecimalCoeffView formats a big.Int coefficient with the given scale
func formatDecimalCoeffView(coeff *big.Int, scale int) string {
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

// unsafeString converts a byte slice to a string without copying.
// WARNING: The returned string is only valid as long as the byte slice is valid.
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return *(*string)(unsafe.Pointer(&b))
}

// DecodeWithView decodes a record using RecordView, returning []types.Value.
// This is a drop-in replacement for Decode() that may be faster for
// records where not all columns are accessed.
func DecodeWithView(data []byte) []types.Value {
	view := NewRecordView(data)
	return view.ToValues()
}
