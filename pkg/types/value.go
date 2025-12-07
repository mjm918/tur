// pkg/types/value.go
package types

// ValueType represents the type of a database value
type ValueType int

const (
	TypeNull ValueType = iota
	TypeInt
	TypeFloat
	TypeText
	TypeBlob
	TypeVector
)

// Value represents a database value (like SQLite's Mem structure)
type Value struct {
	typ      ValueType
	intVal   int64
	floatVal float64
	textVal  string
	blobVal  []byte
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
	return Value{typ: TypeBlob, blobVal: b}
}

func (v Value) Type() ValueType { return v.typ }
func (v Value) IsNull() bool    { return v.typ == TypeNull }
func (v Value) Int() int64      { return v.intVal }
func (v Value) Float() float64  { return v.floatVal }
func (v Value) Text() string    { return v.textVal }
func (v Value) Blob() []byte    { return v.blobVal }
