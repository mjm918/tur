// pkg/types/value.go
package types

import "time"

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
)

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
	default:
		return "UNKNOWN"
	}
}

// Compare compares two Values and returns:
// -1 if a < b
//
//	0 if a == b
//	1 if a > b
//
// NULL values are considered less than non-NULL values.
// Different types are compared by their type order.
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

	// Different types - compare by type order
	if a.typ != b.typ {
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

	default:
		return 0
	}
}
