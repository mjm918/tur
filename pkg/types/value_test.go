// pkg/types/value_test.go
package types

import "testing"

func TestValueNull(t *testing.T) {
	v := NewNull()
	if v.Type() != TypeNull {
		t.Errorf("expected TypeNull, got %v", v.Type())
	}
	if !v.IsNull() {
		t.Error("expected IsNull to return true")
	}
}

func TestValueInt(t *testing.T) {
	v := NewInt(42)
	if v.Type() != TypeInt {
		t.Errorf("expected TypeInt, got %v", v.Type())
	}
	if v.Int() != 42 {
		t.Errorf("expected 42, got %d", v.Int())
	}
}

func TestValueFloat(t *testing.T) {
	v := NewFloat(3.14)
	if v.Type() != TypeFloat {
		t.Errorf("expected TypeFloat, got %v", v.Type())
	}
	if v.Float() != 3.14 {
		t.Errorf("expected 3.14, got %f", v.Float())
	}
}

func TestValueText(t *testing.T) {
	v := NewText("hello")
	if v.Type() != TypeText {
		t.Errorf("expected TypeText, got %v", v.Type())
	}
	if v.Text() != "hello" {
		t.Errorf("expected 'hello', got %s", v.Text())
	}
}

func TestValueBlob(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03}
	v := NewBlob(data)
	if v.Type() != TypeBlob {
		t.Errorf("expected TypeBlob, got %v", v.Type())
	}
	if string(v.Blob()) != string(data) {
		t.Errorf("expected %v, got %v", data, v.Blob())
	}
}
