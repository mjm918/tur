// pkg/sql/executor/strict_type_validation_test.go
package executor

import (
	"os"
	"strings"
	"testing"

	"tur/pkg/pager"
)

// Tests for strict type validation during INSERT operations

func TestExecutor_InsertSmallIntValidation(t *testing.T) {
	tmpFile := "test_smallint_validation.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	mustExec(t, exec, `CREATE TABLE test_smallint (id INTEGER PRIMARY KEY, value SMALLINT);`)

	// Valid SMALLINT values should succeed
	tests := []struct {
		name  string
		value int64
		valid bool
	}{
		{"minimum value", -32768, true},
		{"maximum value", 32767, true},
		{"zero", 0, true},
		{"positive", 100, true},
		{"negative", -100, true},
		{"overflow positive", 32768, false},
		{"overflow negative", -32769, false},
		{"large overflow", 100000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := `INSERT INTO test_smallint (id, value) VALUES (NULL, ` + int64toa(tt.value) + `);`
			_, err := exec.Execute(sql)
			if tt.valid && err != nil {
				t.Errorf("expected insert to succeed for %d, got error: %v", tt.value, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("expected insert to fail for %d (out of SMALLINT range)", tt.value)
			}
		})
	}
}

func TestExecutor_InsertInt32Validation(t *testing.T) {
	tmpFile := "test_int32_validation.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	mustExec(t, exec, `CREATE TABLE test_int32 (id INTEGER PRIMARY KEY, value INT);`)

	tests := []struct {
		name  string
		value int64
		valid bool
	}{
		{"minimum value", -2147483648, true},
		{"maximum value", 2147483647, true},
		{"zero", 0, true},
		{"positive", 1000000, true},
		{"negative", -1000000, true},
		{"overflow positive", 2147483648, false},
		{"overflow negative", -2147483649, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := `INSERT INTO test_int32 (id, value) VALUES (NULL, ` + int64toa(tt.value) + `);`
			_, err := exec.Execute(sql)
			if tt.valid && err != nil {
				t.Errorf("expected insert to succeed for %d, got error: %v", tt.value, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("expected insert to fail for %d (out of INT range)", tt.value)
			}
		})
	}
}

func TestExecutor_InsertVarcharValidation(t *testing.T) {
	tmpFile := "test_varchar_validation.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	mustExec(t, exec, `CREATE TABLE test_varchar (id INTEGER PRIMARY KEY, name VARCHAR(10));`)

	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"empty string", "", true},
		{"short string", "hello", true},
		{"exact length", "1234567890", true},
		{"too long", "12345678901", false},
		{"way too long", "this string is way too long for varchar 10", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := `INSERT INTO test_varchar (id, name) VALUES (NULL, '` + tt.value + `');`
			_, err := exec.Execute(sql)
			if tt.valid && err != nil {
				t.Errorf("expected insert to succeed for %q, got error: %v", tt.value, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("expected insert to fail for %q (exceeds VARCHAR(10) length)", tt.value)
			}
		})
	}
}

func TestExecutor_InsertCharValidation(t *testing.T) {
	tmpFile := "test_char_validation.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	mustExec(t, exec, `CREATE TABLE test_char (id INTEGER PRIMARY KEY, code CHAR(5));`)

	// CHAR should pad shorter strings and truncate longer ones (or error based on mode)
	// For now, we'll test that it accepts any string and pads/truncates as needed
	tests := []struct {
		name     string
		value    string
		expected string // expected value after padding/truncation
	}{
		{"empty string", "", "     "},     // padded to 5 spaces
		{"short string", "AB", "AB   "},   // padded
		{"exact length", "ABCDE", "ABCDE"}, // no change
		{"too long", "ABCDEFG", "ABCDE"},  // truncated
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear table
			exec.Execute(`DELETE FROM test_char;`)

			sql := `INSERT INTO test_char (id, code) VALUES (1, '` + tt.value + `');`
			_, err := exec.Execute(sql)
			if err != nil {
				t.Fatalf("insert failed: %v", err)
			}

			res, err := exec.Execute(`SELECT code FROM test_char WHERE id = 1;`)
			if err != nil {
				t.Fatalf("select failed: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(res.Rows))
			}

			got := res.Rows[0][0].Text()
			if got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestExecutor_InsertDecimalValidation(t *testing.T) {
	tmpFile := "test_decimal_validation.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// DECIMAL(5,2) = 5 total digits, 2 after decimal point = max 999.99
	mustExec(t, exec, `CREATE TABLE test_decimal (id INTEGER PRIMARY KEY, price DECIMAL(5, 2));`)

	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"zero", "0", true},
		{"simple decimal", "123.45", true},
		{"max value", "999.99", true},
		{"negative", "-123.45", true},
		{"exceeds precision", "1000.00", false},  // 4 integer digits exceeds (5-2=3)
		{"exceeds precision large", "99999.99", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := `INSERT INTO test_decimal (id, price) VALUES (NULL, ` + tt.value + `);`
			_, err := exec.Execute(sql)
			if tt.valid && err != nil {
				t.Errorf("expected insert to succeed for %s, got error: %v", tt.value, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("expected insert to fail for %s (exceeds DECIMAL precision)", tt.value)
			}
		})
	}
}

func TestExecutor_InsertGUIDValidation(t *testing.T) {
	tmpFile := "test_guid_validation.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	mustExec(t, exec, `CREATE TABLE test_guid (id GUID PRIMARY KEY);`)

	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"valid guid with dashes", "'12345678-1234-1234-1234-123456789abc'", true},
		{"valid guid lowercase", "'abcdef01-2345-6789-abcd-ef0123456789'", true},
		{"invalid format", "'not-a-guid'", false},
		{"too short", "'12345678'", false},
		{"too long", "'12345678-1234-1234-1234-123456789abc-extra'", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := `INSERT INTO test_guid (id) VALUES (` + tt.value + `);`
			_, err := exec.Execute(sql)
			if tt.valid && err != nil {
				t.Errorf("expected insert to succeed for %s, got error: %v", tt.value, err)
			}
			if !tt.valid && err == nil {
				t.Errorf("expected insert to fail for %s (invalid GUID format)", tt.value)
			}
		})
	}
}

func TestExecutor_InsertSerialAutoIncrement(t *testing.T) {
	tmpFile := "test_serial_autoincrement.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	mustExec(t, exec, `CREATE TABLE test_serial (id SERIAL PRIMARY KEY, name TEXT);`)

	// Insert without specifying id - should auto-increment
	mustExec(t, exec, `INSERT INTO test_serial (name) VALUES ('first');`)
	mustExec(t, exec, `INSERT INTO test_serial (name) VALUES ('second');`)
	mustExec(t, exec, `INSERT INTO test_serial (name) VALUES ('third');`)

	res, err := exec.Execute(`SELECT id, name FROM test_serial ORDER BY id;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}

	if len(res.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Rows))
	}

	// Check that IDs are 1, 2, 3
	for i, row := range res.Rows {
		expected := int64(i + 1)
		if row[0].Int() != expected {
			t.Errorf("row %d: expected id %d, got %d", i, expected, row[0].Int())
		}
	}
}

func TestExecutor_InsertBigSerialAutoIncrement(t *testing.T) {
	tmpFile := "test_bigserial_autoincrement.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	mustExec(t, exec, `CREATE TABLE test_bigserial (id BIGSERIAL PRIMARY KEY, name TEXT);`)

	// Insert without specifying id - should auto-increment
	mustExec(t, exec, `INSERT INTO test_bigserial (name) VALUES ('first');`)
	mustExec(t, exec, `INSERT INTO test_bigserial (name) VALUES ('second');`)

	res, err := exec.Execute(`SELECT id, name FROM test_bigserial ORDER BY id;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}

	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}

	// Check that IDs are 1, 2
	if res.Rows[0][0].Int() != 1 || res.Rows[1][0].Int() != 2 {
		t.Errorf("expected ids 1, 2; got %d, %d", res.Rows[0][0].Int(), res.Rows[1][0].Int())
	}
}

func TestExecutor_StrictTypeRoundTrip(t *testing.T) {
	tmpFile := "test_strict_roundtrip.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	// Create a table with all strict types
	mustExec(t, exec, `CREATE TABLE all_strict (
		id INTEGER PRIMARY KEY,
		small_val SMALLINT,
		int_val INT,
		big_val BIGINT,
		name VARCHAR(50),
		code CHAR(5),
		price DECIMAL(10, 2)
	);`)

	// Insert values
	mustExec(t, exec, `INSERT INTO all_strict VALUES (1, 100, 1000000, 9223372036854775807, 'hello world', 'ABC', 123.45);`)

	// Select and verify
	res, err := exec.Execute(`SELECT * FROM all_strict WHERE id = 1;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}

	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}

	row := res.Rows[0]

	// Verify each value
	if row[1].Int() != 100 {
		t.Errorf("small_val: expected 100, got %d", row[1].Int())
	}
	if row[2].Int() != 1000000 {
		t.Errorf("int_val: expected 1000000, got %d", row[2].Int())
	}
	if row[3].Int() != 9223372036854775807 {
		t.Errorf("big_val: expected 9223372036854775807, got %d", row[3].Int())
	}
	if row[4].Text() != "hello world" {
		t.Errorf("name: expected 'hello world', got %q", row[4].Text())
	}
	// CHAR(5) should pad to 5 characters
	if row[5].Text() != "ABC  " {
		t.Errorf("code: expected 'ABC  ', got %q", row[5].Text())
	}
}

// Helper function to convert int64 to string
func int64toa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var s string
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}

func TestExecutor_InsertMultipleStrictTypes(t *testing.T) {
	tmpFile := "test_multiple_strict.db"
	_ = os.Remove(tmpFile)
	defer os.Remove(tmpFile)

	p, err := pager.Open(tmpFile, pager.Options{})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}

	exec := New(p)
	defer exec.Close()

	mustExec(t, exec, `CREATE TABLE products (
		id SERIAL PRIMARY KEY,
		sku CHAR(10),
		name VARCHAR(100),
		price DECIMAL(10, 2),
		quantity INT
	);`)

	mustExec(t, exec, `INSERT INTO products (sku, name, price, quantity) VALUES ('WIDGET001', 'Super Widget', 19.99, 100);`)
	mustExec(t, exec, `INSERT INTO products (sku, name, price, quantity) VALUES ('GADGET002', 'Mega Gadget', 49.99, 50);`)

	res, err := exec.Execute(`SELECT id, sku, name, price, quantity FROM products ORDER BY id;`)
	if err != nil {
		t.Fatalf("select failed: %v", err)
	}

	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}

	// Verify first row
	row := res.Rows[0]
	if row[0].Int() != 1 {
		t.Errorf("id: expected 1, got %d", row[0].Int())
	}
	// CHAR(10) should pad SKU
	sku := strings.TrimRight(row[1].Text(), " ")
	if sku != "WIDGET001" {
		t.Errorf("sku: expected 'WIDGET001', got %q", sku)
	}
	if row[2].Text() != "Super Widget" {
		t.Errorf("name: expected 'Super Widget', got %q", row[2].Text())
	}
}
