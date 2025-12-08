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
	typ       ValueType
	intVal    int64
	floatVal  float64
	textVal   string
	blobVal   []byte
	vectorVal *Vector
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
