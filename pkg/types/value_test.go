// pkg/types/value_test.go
package types

import (
	"testing"
	"time"
)

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

func TestDateType(t *testing.T) {
	d := NewDate(2025, 12, 10)
	if d.Type() != TypeDate {
		t.Errorf("expected TypeDate, got %v", d.Type())
	}
	if d.IsNull() {
		t.Error("date should not be null")
	}
	year, month, day := d.DateValue()
	if year != 2025 || month != 12 || day != 10 {
		t.Errorf("expected 2025-12-10, got %d-%d-%d", year, month, day)
	}
}

func TestTimeType(t *testing.T) {
	tm := NewTime(14, 30, 45, 123456)
	if tm.Type() != TypeTime {
		t.Errorf("expected TypeTime, got %v", tm.Type())
	}
	hour, min, sec, usec := tm.TimeValue()
	if hour != 14 || min != 30 || sec != 45 || usec != 123456 {
		t.Errorf("expected 14:30:45.123456, got %d:%d:%d.%d", hour, min, sec, usec)
	}
}

func TestTimeTZType(t *testing.T) {
	tm := NewTimeTZ(14, 30, 45, 0, 5*3600+30*60) // +05:30 offset
	if tm.Type() != TypeTimeTZ {
		t.Errorf("expected TypeTimeTZ, got %v", tm.Type())
	}
	hour, min, sec, _, offset := tm.TimeTZValue()
	if hour != 14 || min != 30 || sec != 45 || offset != 19800 {
		t.Errorf("unexpected timetz value")
	}
}

func TestTimestampType(t *testing.T) {
	ts := NewTimestamp(2025, 12, 10, 14, 30, 45, 0)
	if ts.Type() != TypeTimestamp {
		t.Errorf("expected TypeTimestamp, got %v", ts.Type())
	}
	goTime := ts.TimestampValue()
	if goTime.Year() != 2025 || goTime.Month() != 12 || goTime.Day() != 10 {
		t.Errorf("unexpected timestamp date part")
	}
	if goTime.Hour() != 14 || goTime.Minute() != 30 || goTime.Second() != 45 {
		t.Errorf("unexpected timestamp time part")
	}
}

func TestTimestampTZType(t *testing.T) {
	ts := NewTimestampTZ(time.Date(2025, 12, 10, 14, 30, 45, 0, time.UTC))
	if ts.Type() != TypeTimestampTZ {
		t.Errorf("expected TypeTimestampTZ, got %v", ts.Type())
	}
	goTime := ts.TimestampTZValue()
	if goTime.Location() != time.UTC {
		t.Error("timestamptz should be stored in UTC")
	}
}

func TestIntervalType(t *testing.T) {
	iv := NewInterval(14, 3*24*3600*1000000+4*3600*1000000) // 14 months + days/hours in microseconds
	if iv.Type() != TypeInterval {
		t.Errorf("expected TypeInterval, got %v", iv.Type())
	}
	months, _ := iv.IntervalValue()
	if months != 14 {
		t.Errorf("expected 14 months, got %d", months)
	}
}
