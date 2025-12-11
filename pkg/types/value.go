// pkg/types/value.go
package types

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// ValueType represents the type of a database value
type ValueType int

const (
	TypeNull ValueType = iota
	TypeInt
	TypeFloat
	TypeText
	TypeBlob
	TypeVector
	TypeDate        // Calendar date without time
	TypeTime        // Time of day without timezone
	TypeTimeTZ      // Time with timezone offset
	TypeTimestamp   // Date and time without timezone
	TypeTimestampTZ // Date and time in UTC
	TypeInterval    // Duration for date arithmetic
	TypeJSON        // JSON data type

	// Strict data types with fixed sizes
	TypeSmallInt  // 2-byte signed integer (-32768 to 32767)
	TypeInt32     // 4-byte signed integer (-2147483648 to 2147483647)
	TypeBigInt    // 8-byte signed integer
	TypeSerial    // Auto-incrementing 4-byte integer
	TypeBigSerial // Auto-incrementing 8-byte integer
	TypeGUID      // 128-bit UUID/GUID
	TypeDecimal   // Exact numeric with precision and scale
	TypeVarchar   // Variable-length string with max length
	TypeChar      // Fixed-length string
)

// IsIntegerType returns true if the type is any integer type
func IsIntegerType(t ValueType) bool {
	switch t {
	case TypeInt, TypeInt32, TypeSmallInt, TypeBigInt, TypeSerial, TypeBigSerial:
		return true
	default:
		return false
	}
}

// IntervalValue represents a duration for date arithmetic
type IntervalValue struct {
	Months       int64 // Months component
	Microseconds int64 // Microseconds component (days, hours, minutes, seconds, microseconds)
}

// Value represents a database value (like SQLite's Mem structure)
type Value struct {
	typ          ValueType
	intVal       int64
	floatVal     float64
	textVal      string
	blobVal      []byte
	vectorVal    *Vector
	dateVal      int32         // Days since 2000-01-01 for DATE
	timeVal      int64         // Microseconds since midnight for TIME
	tzOffsetVal  int32         // Timezone offset in seconds for TIMETZ
	timestampVal time.Time     // For TIMESTAMP and TIMESTAMPTZ
	intervalVal  IntervalValue // For INTERVAL
	jsonVal      string        // For JSON
}

func NewNull() Value {
	return Value{typ: TypeNull}
}

func NewInt(i int64) Value {
	return Value{typ: TypeInt, intVal: i}
}

func NewFloat(f float64) Value {
	return Value{typ: TypeFloat, floatVal: f}
}

func NewText(s string) Value {
	return Value{typ: TypeText, textVal: s}
}

func NewBlob(b []byte) Value {
	if b == nil {
		return Value{typ: TypeBlob, blobVal: nil}
	}
	copied := make([]byte, len(b))
	copy(copied, b)
	return Value{typ: TypeBlob, blobVal: copied}
}

func NewVectorValue(v *Vector) Value {
	return Value{typ: TypeVector, vectorVal: v}
}

func (v Value) Type() ValueType { return v.typ }
func (v Value) IsNull() bool    { return v.typ == TypeNull }
func (v Value) Int() int64      { return v.intVal }
func (v Value) Float() float64  { return v.floatVal }
func (v Value) Text() string    { return v.textVal }
func (v Value) Blob() []byte {
	if v.blobVal == nil {
		return nil
	}
	copied := make([]byte, len(v.blobVal))
	copy(copied, v.blobVal)
	return copied
}

func (v Value) Vector() *Vector {
	return v.vectorVal
}

// NewDate creates a new DATE value (days since PostgreSQL epoch 2000-01-01)
func NewDate(year, month, day int) Value {
	// PostgreSQL epoch: 2000-01-01
	epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	days := int32(date.Sub(epoch) / (24 * time.Hour))
	return Value{typ: TypeDate, dateVal: days}
}

// DateValue returns the date components (year, month, day)
func (v Value) DateValue() (year, month, day int) {
	epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	date := epoch.AddDate(0, 0, int(v.dateVal))
	return date.Year(), int(date.Month()), date.Day()
}

// NewTime creates a new TIME value (microseconds since midnight)
func NewTime(hour, minute, second, microsecond int) Value {
	usec := int64(hour)*3600*1000000 + int64(minute)*60*1000000 + int64(second)*1000000 + int64(microsecond)
	return Value{typ: TypeTime, timeVal: usec}
}

// TimeValue returns the time components (hour, minute, second, microsecond)
func (v Value) TimeValue() (hour, minute, second, microsecond int) {
	usec := v.timeVal
	hour = int(usec / (3600 * 1000000))
	usec -= int64(hour) * 3600 * 1000000
	minute = int(usec / (60 * 1000000))
	usec -= int64(minute) * 60 * 1000000
	second = int(usec / 1000000)
	usec -= int64(second) * 1000000
	microsecond = int(usec)
	return
}

// NewTimeTZ creates a new TIMETZ value (time with timezone offset in seconds)
func NewTimeTZ(hour, minute, second, microsecond, offsetSeconds int) Value {
	usec := int64(hour)*3600*1000000 + int64(minute)*60*1000000 + int64(second)*1000000 + int64(microsecond)
	return Value{typ: TypeTimeTZ, timeVal: usec, tzOffsetVal: int32(offsetSeconds)}
}

// TimeTZValue returns the time components with timezone offset (hour, minute, second, microsecond, offsetSeconds)
func (v Value) TimeTZValue() (hour, minute, second, microsecond, offsetSeconds int) {
	usec := v.timeVal
	hour = int(usec / (3600 * 1000000))
	usec -= int64(hour) * 3600 * 1000000
	minute = int(usec / (60 * 1000000))
	usec -= int64(minute) * 60 * 1000000
	second = int(usec / 1000000)
	usec -= int64(second) * 1000000
	microsecond = int(usec)
	offsetSeconds = int(v.tzOffsetVal)
	return
}

// NewTimestamp creates a new TIMESTAMP value (datetime without timezone)
func NewTimestamp(year, month, day, hour, minute, second, microsecond int) Value {
	t := time.Date(year, time.Month(month), day, hour, minute, second, microsecond*1000, time.UTC)
	return Value{typ: TypeTimestamp, timestampVal: t}
}

// TimestampValue returns the timestamp as a Go time.Time
func (v Value) TimestampValue() time.Time {
	return v.timestampVal
}

// NewTimestampTZ creates a new TIMESTAMPTZ value (datetime in UTC)
func NewTimestampTZ(t time.Time) Value {
	// Convert to UTC for storage
	utc := t.UTC()
	return Value{typ: TypeTimestampTZ, timestampVal: utc}
}

// TimestampTZValue returns the timestamptz as a Go time.Time in UTC
func (v Value) TimestampTZValue() time.Time {
	return v.timestampVal
}

// NewInterval creates a new INTERVAL value (months + microseconds)
func NewInterval(months, microseconds int64) Value {
	return Value{
		typ:         TypeInterval,
		intervalVal: IntervalValue{Months: months, Microseconds: microseconds},
	}
}

// IntervalValue returns the interval components (months, microseconds)
func (v Value) IntervalValue() (months, microseconds int64) {
	return v.intervalVal.Months, v.intervalVal.Microseconds
}

// NewJSON creates a new JSON value from a JSON string
func NewJSON(s string) Value {
	return Value{typ: TypeJSON, jsonVal: s}
}

// JSON returns the JSON string value
func (v Value) JSON() string {
	return v.jsonVal
}

// String returns the string representation of ValueType
func (t ValueType) String() string {
	switch t {
	case TypeNull:
		return "NULL"
	case TypeInt:
		return "INTEGER"
	case TypeFloat:
		return "REAL"
	case TypeText:
		return "TEXT"
	case TypeBlob:
		return "BLOB"
	case TypeVector:
		return "VECTOR"
	case TypeDate:
		return "DATE"
	case TypeTime:
		return "TIME"
	case TypeTimeTZ:
		return "TIMETZ"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeTimestampTZ:
		return "TIMESTAMPTZ"
	case TypeInterval:
		return "INTERVAL"
	case TypeJSON:
		return "JSON"
	// Strict types
	case TypeSmallInt:
		return "SMALLINT"
	case TypeInt32:
		return "INT"
	case TypeBigInt:
		return "BIGINT"
	case TypeSerial:
		return "SERIAL"
	case TypeBigSerial:
		return "BIGSERIAL"
	case TypeGUID:
		return "GUID"
	case TypeDecimal:
		return "DECIMAL"
	case TypeVarchar:
		return "VARCHAR"
	case TypeChar:
		return "CHAR"
	default:
		return "UNKNOWN"
	}
}

// isIntegerType returns true if the type is any integer type (legacy or strict)
func isIntegerType(t ValueType) bool {
	switch t {
	case TypeInt, TypeSmallInt, TypeInt32, TypeBigInt, TypeSerial, TypeBigSerial:
		return true
	}
	return false
}

// isStringType returns true if the type is any string type
func isStringType(t ValueType) bool {
	switch t {
	case TypeText, TypeVarchar, TypeChar:
		return true
	}
	return false
}

// Compare compares two Values and returns:
// -1 if a < b
//
//	0 if a == b
//	1 if a > b
//
// NULL values are considered less than non-NULL values.
// Different types are compared by type order, except for compatible types
// (e.g., all integer types can be compared with each other).
func Compare(a, b Value) int {
	// Handle NULL cases
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1
	}
	if b.IsNull() {
		return 1
	}

	// Handle cross-type comparisons for compatible types
	if a.typ != b.typ {
		// All integer types can be compared with each other
		if isIntegerType(a.typ) && isIntegerType(b.typ) {
			if a.intVal < b.intVal {
				return -1
			} else if a.intVal > b.intVal {
				return 1
			}
			return 0
		}

		// All string types can be compared with each other
		if isStringType(a.typ) && isStringType(b.typ) {
			if a.textVal < b.textVal {
				return -1
			} else if a.textVal > b.textVal {
				return 1
			}
			return 0
		}

		// Integer and float can be compared
		if isIntegerType(a.typ) && b.typ == TypeFloat {
			aFloat := float64(a.intVal)
			if aFloat < b.floatVal {
				return -1
			} else if aFloat > b.floatVal {
				return 1
			}
			return 0
		}
		if a.typ == TypeFloat && isIntegerType(b.typ) {
			bFloat := float64(b.intVal)
			if a.floatVal < bFloat {
				return -1
			} else if a.floatVal > bFloat {
				return 1
			}
			return 0
		}

		// Different incompatible types - compare by type order
		if a.typ < b.typ {
			return -1
		}
		return 1
	}

	// Same type - compare values
	switch a.typ {
	case TypeInt:
		if a.intVal < b.intVal {
			return -1
		} else if a.intVal > b.intVal {
			return 1
		}
		return 0

	case TypeFloat:
		if a.floatVal < b.floatVal {
			return -1
		} else if a.floatVal > b.floatVal {
			return 1
		}
		return 0

	case TypeText:
		if a.textVal < b.textVal {
			return -1
		} else if a.textVal > b.textVal {
			return 1
		}
		return 0

	case TypeBlob:
		// Compare blobs byte by byte
		minLen := len(a.blobVal)
		if len(b.blobVal) < minLen {
			minLen = len(b.blobVal)
		}
		for i := 0; i < minLen; i++ {
			if a.blobVal[i] < b.blobVal[i] {
				return -1
			} else if a.blobVal[i] > b.blobVal[i] {
				return 1
			}
		}
		if len(a.blobVal) < len(b.blobVal) {
			return -1
		} else if len(a.blobVal) > len(b.blobVal) {
			return 1
		}
		return 0

	case TypeDate:
		if a.dateVal < b.dateVal {
			return -1
		} else if a.dateVal > b.dateVal {
			return 1
		}
		return 0

	case TypeTime, TypeTimeTZ:
		if a.timeVal < b.timeVal {
			return -1
		} else if a.timeVal > b.timeVal {
			return 1
		}
		return 0

	case TypeTimestamp, TypeTimestampTZ:
		if a.timestampVal.Before(b.timestampVal) {
			return -1
		} else if a.timestampVal.After(b.timestampVal) {
			return 1
		}
		return 0

	case TypeInterval:
		// Compare months first, then microseconds
		if a.intervalVal.Months < b.intervalVal.Months {
			return -1
		} else if a.intervalVal.Months > b.intervalVal.Months {
			return 1
		}
		if a.intervalVal.Microseconds < b.intervalVal.Microseconds {
			return -1
		} else if a.intervalVal.Microseconds > b.intervalVal.Microseconds {
			return 1
		}
		return 0

	case TypeJSON:
		// Compare JSON values lexicographically by their string representation
		if a.jsonVal < b.jsonVal {
			return -1
		} else if a.jsonVal > b.jsonVal {
			return 1
		}
		return 0

	// Strict integer types - all use intVal
	case TypeSmallInt, TypeInt32, TypeBigInt, TypeSerial, TypeBigSerial:
		if a.intVal < b.intVal {
			return -1
		} else if a.intVal > b.intVal {
			return 1
		}
		return 0

	case TypeGUID:
		// Compare GUIDs byte by byte
		for i := 0; i < 16 && i < len(a.blobVal) && i < len(b.blobVal); i++ {
			if a.blobVal[i] < b.blobVal[i] {
				return -1
			} else if a.blobVal[i] > b.blobVal[i] {
				return 1
			}
		}
		return 0

	case TypeDecimal:
		// Compare decimals using their coefficient and scale
		aCoeff := a.DecimalCoefficient()
		bCoeff := b.DecimalCoefficient()
		_, aScale := a.DecimalPrecisionScale()
		_, bScale := b.DecimalPrecisionScale()

		// Normalize to same scale for comparison
		if aScale < bScale {
			// Scale up a
			factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(bScale-aScale)), nil)
			aCoeff = new(big.Int).Mul(aCoeff, factor)
		} else if bScale < aScale {
			// Scale up b
			factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(aScale-bScale)), nil)
			bCoeff = new(big.Int).Mul(bCoeff, factor)
		}

		return aCoeff.Cmp(bCoeff)

	case TypeVarchar, TypeChar:
		// Compare as strings
		if a.textVal < b.textVal {
			return -1
		} else if a.textVal > b.textVal {
			return 1
		}
		return 0

	default:
		return 0
	}
}

// DecimalValue stores the decimal as a scaled integer for precision
type DecimalValue struct {
	// Coefficient stores the unscaled value (e.g., for 123.45, this is 12345)
	Coefficient *big.Int
	// Precision is the total number of digits
	Precision int
	// Scale is the number of digits after the decimal point
	Scale int
}

// NewSmallInt creates a new SMALLINT value (2-byte signed integer)
func NewSmallInt(i int16) Value {
	return Value{typ: TypeSmallInt, intVal: int64(i)}
}

// SmallInt returns the value as a 16-bit integer
func (v Value) SmallInt() int16 {
	return int16(v.intVal)
}

// NewInt32 creates a new INT value (4-byte signed integer)
func NewInt32(i int32) Value {
	return Value{typ: TypeInt32, intVal: int64(i)}
}

// Int32 returns the value as a 32-bit integer
func (v Value) Int32() int32 {
	return int32(v.intVal)
}

// NewBigInt creates a new BIGINT value (8-byte signed integer)
func NewBigInt(i int64) Value {
	return Value{typ: TypeBigInt, intVal: i}
}

// BigInt returns the value as a 64-bit integer
func (v Value) BigInt() int64 {
	return v.intVal
}

// NewSerial creates a new SERIAL value (auto-incrementing 4-byte integer)
func NewSerial(i int32) Value {
	return Value{typ: TypeSerial, intVal: int64(i)}
}

// Serial returns the value as a 32-bit integer
func (v Value) Serial() int32 {
	return int32(v.intVal)
}

// NewBigSerial creates a new BIGSERIAL value (auto-incrementing 8-byte integer)
func NewBigSerial(i int64) Value {
	return Value{typ: TypeBigSerial, intVal: i}
}

// BigSerial returns the value as a 64-bit integer
func (v Value) BigSerial() int64 {
	return v.intVal
}

// NewGUID creates a new GUID value from a 16-byte array
func NewGUID(uuid [16]byte) Value {
	// Store UUID in blobVal
	b := make([]byte, 16)
	copy(b, uuid[:])
	return Value{typ: TypeGUID, blobVal: b}
}

// GUID returns the value as a 16-byte UUID array
func (v Value) GUID() [16]byte {
	var uuid [16]byte
	if len(v.blobVal) >= 16 {
		copy(uuid[:], v.blobVal[:16])
	}
	return uuid
}

// GUIDString returns the UUID as a formatted string (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
func (v Value) GUIDString() string {
	uuid := v.GUID()
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}

// NewGUIDFromString creates a new GUID value from a string representation
func NewGUIDFromString(s string) (Value, error) {
	// Remove hyphens
	s = strings.ReplaceAll(s, "-", "")

	if len(s) != 32 {
		return Value{}, errors.New("invalid GUID string: must be 32 hex characters")
	}

	bytes, err := hex.DecodeString(s)
	if err != nil {
		return Value{}, fmt.Errorf("invalid GUID string: %w", err)
	}

	var uuid [16]byte
	copy(uuid[:], bytes)
	return NewGUID(uuid), nil
}

// NewDecimal creates a new DECIMAL value from a string representation
func NewDecimal(s string, precision, scale int) (Value, error) {
	if precision < 1 {
		return Value{}, errors.New("precision must be at least 1")
	}
	if scale < 0 {
		return Value{}, errors.New("scale cannot be negative")
	}
	if scale > precision {
		return Value{}, errors.New("scale cannot exceed precision")
	}

	// Parse the decimal string
	s = strings.TrimSpace(s)
	negative := false
	if strings.HasPrefix(s, "-") {
		negative = true
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}

	// Split into integer and fractional parts
	parts := strings.Split(s, ".")
	intPart := parts[0]
	fracPart := ""
	if len(parts) > 1 {
		fracPart = parts[1]
	}

	// Validate and adjust fractional part to match scale
	if len(fracPart) > scale {
		// Truncate or round (we'll truncate for simplicity)
		fracPart = fracPart[:scale]
	} else {
		// Pad with zeros
		fracPart = fracPart + strings.Repeat("0", scale-len(fracPart))
	}

	// Combine into coefficient
	coeffStr := intPart + fracPart
	// Remove leading zeros but keep at least one digit
	coeffStr = strings.TrimLeft(coeffStr, "0")
	if coeffStr == "" {
		coeffStr = "0"
	}

	// Remove leading zeros from integer part for counting
	intPartTrimmed := strings.TrimLeft(intPart, "0")
	if intPartTrimmed == "" {
		intPartTrimmed = "0"
	}

	// Check if we exceed precision (integer digits must fit in precision - scale)
	if len(intPartTrimmed) > precision-scale {
		return Value{}, fmt.Errorf("value exceeds precision: %d integer digits, max allowed %d", len(intPartTrimmed), precision-scale)
	}

	coeff := new(big.Int)
	_, ok := coeff.SetString(intPart+fracPart, 10)
	if !ok {
		return Value{}, errors.New("invalid decimal string")
	}

	if negative {
		coeff.Neg(coeff)
	}

	// Store decimal info in the Value
	// We'll encode: precision (1 byte) + scale (1 byte) + coefficient bytes
	coeffBytes := coeff.Bytes()
	if coeff.Sign() < 0 {
		// Use big.Int's signed representation
		coeffBytes = coeff.Bytes()
	}

	// Encode: [negative:1][precision:1][scale:1][coeff_len:2][coeff:...]
	encoded := make([]byte, 0, 5+len(coeffBytes))
	if negative {
		encoded = append(encoded, 1)
	} else {
		encoded = append(encoded, 0)
	}
	encoded = append(encoded, byte(precision), byte(scale))
	encoded = append(encoded, byte(len(coeffBytes)>>8), byte(len(coeffBytes)))
	encoded = append(encoded, coeffBytes...)

	// Reconstruct with proper formatting
	formattedStr := formatDecimal(coeff, scale)

	return Value{typ: TypeDecimal, blobVal: encoded, textVal: formattedStr}, nil
}

// formatDecimal formats a big.Int coefficient with the given scale
func formatDecimal(coeff *big.Int, scale int) string {
	negative := coeff.Sign() < 0
	absCoeff := new(big.Int).Abs(coeff)
	s := absCoeff.String()

	// Pad with leading zeros if necessary
	if len(s) <= scale {
		s = strings.Repeat("0", scale-len(s)+1) + s
	}

	// Insert decimal point
	intPart := s[:len(s)-scale]
	fracPart := s[len(s)-scale:]

	result := intPart
	if scale > 0 {
		result = intPart + "." + fracPart
	}

	if negative {
		result = "-" + result
	}

	return result
}

// DecimalString returns the decimal as a string
func (v Value) DecimalString() string {
	return v.textVal
}

// DecimalPrecisionScale returns the precision and scale of the decimal
func (v Value) DecimalPrecisionScale() (precision, scale int) {
	if len(v.blobVal) < 3 {
		return 0, 0
	}
	return int(v.blobVal[1]), int(v.blobVal[2])
}

// DecimalCoefficient returns the coefficient as a big.Int
func (v Value) DecimalCoefficient() *big.Int {
	if len(v.blobVal) < 5 {
		return big.NewInt(0)
	}

	negative := v.blobVal[0] == 1
	coeffLen := int(v.blobVal[3])<<8 | int(v.blobVal[4])

	if len(v.blobVal) < 5+coeffLen {
		return big.NewInt(0)
	}

	coeff := new(big.Int).SetBytes(v.blobVal[5 : 5+coeffLen])
	if negative {
		coeff.Neg(coeff)
	}

	return coeff
}

// NewVarchar creates a new VARCHAR value with a maximum length
func NewVarchar(s string, maxLen int) Value {
	// Store maxLen in intVal for retrieval
	return Value{typ: TypeVarchar, textVal: s, intVal: int64(maxLen)}
}

// Varchar returns the string value
func (v Value) Varchar() string {
	return v.textVal
}

// VarcharMaxLen returns the maximum length of the VARCHAR
func (v Value) VarcharMaxLen() int {
	return int(v.intVal)
}

// NewChar creates a new CHAR value with fixed length (space-padded)
func NewChar(s string, length int) Value {
	// Pad or truncate to exact length
	if len(s) < length {
		s = s + strings.Repeat(" ", length-len(s))
	} else if len(s) > length {
		s = s[:length]
	}
	return Value{typ: TypeChar, textVal: s, intVal: int64(length)}
}

// Char returns the fixed-length string (with padding)
func (v Value) Char() string {
	return v.textVal
}

// CharLen returns the fixed length of the CHAR
func (v Value) CharLen() int {
	return int(v.intVal)
}

// Validation functions

// ValidateSmallInt checks if a value fits in a SMALLINT
func ValidateSmallInt(val int64) error {
	if val < -32768 || val > 32767 {
		return fmt.Errorf("value %d out of range for SMALLINT (-32768 to 32767)", val)
	}
	return nil
}

// ValidateInt32 checks if a value fits in an INT (4-byte)
func ValidateInt32(val int64) error {
	if val < -2147483648 || val > 2147483647 {
		return fmt.Errorf("value %d out of range for INT (-2147483648 to 2147483647)", val)
	}
	return nil
}

// ValidateVarchar checks if a string fits in a VARCHAR(n)
func ValidateVarchar(s string, maxLen int) error {
	if len(s) > maxLen {
		return fmt.Errorf("string length %d exceeds VARCHAR(%d) limit", len(s), maxLen)
	}
	return nil
}
