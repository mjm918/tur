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

func TestDateBeforeEpoch(t *testing.T) {
	// Date before PostgreSQL epoch (2000-01-01)
	d := NewDate(1999, 12, 31)
	year, month, day := d.DateValue()
	if year != 1999 || month != 12 || day != 31 {
		t.Errorf("expected 1999-12-31, got %d-%d-%d", year, month, day)
	}
}

func TestDateBoundary(t *testing.T) {
	// Test leap year
	d := NewDate(2000, 2, 29)
	year, month, day := d.DateValue()
	if year != 2000 || month != 2 || day != 29 {
		t.Errorf("expected 2000-02-29, got %d-%d-%d", year, month, day)
	}

	// Far future date
	d = NewDate(2100, 6, 15)
	year, month, day = d.DateValue()
	if year != 2100 || month != 6 || day != 15 {
		t.Errorf("expected 2100-06-15, got %d-%d-%d", year, month, day)
	}
}

func TestTimestampTZFromNonUTC(t *testing.T) {
	// Create timestamp in non-UTC timezone
	loc, _ := time.LoadLocation("America/New_York")
	localTime := time.Date(2025, 12, 10, 14, 30, 0, 0, loc)

	ts := NewTimestampTZ(localTime)
	stored := ts.TimestampTZValue()

	// Should be stored as UTC
	if stored.Location() != time.UTC {
		t.Error("timestamptz should be stored in UTC")
	}

	// UTC time should be different from local time (EST is UTC-5)
	if stored.Hour() == 14 {
		t.Error("expected hour to be converted to UTC")
	}
}

func TestIntervalNegative(t *testing.T) {
	// Negative interval
	iv := NewInterval(-2, -3600*1000000) // -2 months, -1 hour
	months, usec := iv.IntervalValue()
	if months != -2 {
		t.Errorf("expected -2 months, got %d", months)
	}
	if usec != -3600*1000000 {
		t.Errorf("expected -3600000000 usec, got %d", usec)
	}
}

func TestDateTypeString(t *testing.T) {
	tests := []struct {
		typ    ValueType
		expect string
	}{
		{TypeDate, "DATE"},
		{TypeTime, "TIME"},
		{TypeTimeTZ, "TIMETZ"},
		{TypeTimestamp, "TIMESTAMP"},
		{TypeTimestampTZ, "TIMESTAMPTZ"},
		{TypeInterval, "INTERVAL"},
	}

	for _, tc := range tests {
		if tc.typ.String() != tc.expect {
			t.Errorf("expected %q, got %q", tc.expect, tc.typ.String())
		}
	}
}

func TestDateComparison(t *testing.T) {
	d1 := NewDate(2025, 12, 10)
	d2 := NewDate(2025, 12, 11)
	d3 := NewDate(2025, 12, 10)

	if Compare(d1, d2) >= 0 {
		t.Error("d1 should be less than d2")
	}
	if Compare(d2, d1) <= 0 {
		t.Error("d2 should be greater than d1")
	}
	if Compare(d1, d3) != 0 {
		t.Error("d1 should equal d3")
	}
}

func TestTimestampComparison(t *testing.T) {
	ts1 := NewTimestamp(2025, 12, 10, 14, 30, 0, 0)
	ts2 := NewTimestamp(2025, 12, 10, 14, 31, 0, 0)
	ts3 := NewTimestamp(2025, 12, 10, 14, 30, 0, 0)

	if Compare(ts1, ts2) >= 0 {
		t.Error("ts1 should be less than ts2")
	}
	if Compare(ts2, ts1) <= 0 {
		t.Error("ts2 should be greater than ts1")
	}
	if Compare(ts1, ts3) != 0 {
		t.Error("ts1 should equal ts3")
	}
}

func TestTimeComparison(t *testing.T) {
	t1 := NewTime(14, 30, 0, 0)
	t2 := NewTime(14, 31, 0, 0)
	t3 := NewTime(14, 30, 0, 0)

	if Compare(t1, t2) >= 0 {
		t.Error("t1 should be less than t2")
	}
	if Compare(t1, t3) != 0 {
		t.Error("t1 should equal t3")
	}
}

func TestIntervalComparison(t *testing.T) {
	// Intervals compare months first, then microseconds
	i1 := NewInterval(1, 0)      // 1 month
	i2 := NewInterval(2, 0)      // 2 months
	i3 := NewInterval(1, 1000000) // 1 month + 1 second

	if Compare(i1, i2) >= 0 {
		t.Error("1 month should be less than 2 months")
	}
	if Compare(i1, i3) >= 0 {
		t.Error("1 month should be less than 1 month + 1 second")
	}
}
