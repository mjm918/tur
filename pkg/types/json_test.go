// pkg/types/json_test.go
package types

import (
	"testing"
)

func TestJSONType(t *testing.T) {
	// Test creating a JSON value from string
	t.Run("create JSON value from valid JSON string", func(t *testing.T) {
		jsonStr := `{"name": "John", "age": 30}`
		v := NewJSON(jsonStr)

		if v.Type() != TypeJSON {
			t.Errorf("expected type TypeJSON, got %v", v.Type())
		}

		if v.IsNull() {
			t.Error("JSON value should not be null")
		}

		if v.JSON() != jsonStr {
			t.Errorf("expected JSON %q, got %q", jsonStr, v.JSON())
		}
	})

	t.Run("create JSON value from JSON array", func(t *testing.T) {
		jsonStr := `[1, 2, 3, "hello"]`
		v := NewJSON(jsonStr)

		if v.Type() != TypeJSON {
			t.Errorf("expected type TypeJSON, got %v", v.Type())
		}

		if v.JSON() != jsonStr {
			t.Errorf("expected JSON %q, got %q", jsonStr, v.JSON())
		}
	})

	t.Run("JSON type string representation", func(t *testing.T) {
		if TypeJSON.String() != "JSON" {
			t.Errorf("expected TypeJSON string to be 'JSON', got %q", TypeJSON.String())
		}
	})
}

func TestJSONCompare(t *testing.T) {
	t.Run("compare equal JSON values", func(t *testing.T) {
		v1 := NewJSON(`{"a": 1}`)
		v2 := NewJSON(`{"a": 1}`)

		if Compare(v1, v2) != 0 {
			t.Error("equal JSON values should compare as equal")
		}
	})

	t.Run("compare different JSON values", func(t *testing.T) {
		v1 := NewJSON(`{"a": 1}`)
		v2 := NewJSON(`{"a": 2}`)

		// JSON comparison is lexicographic on the raw string
		result := Compare(v1, v2)
		if result == 0 {
			t.Error("different JSON values should not compare as equal")
		}
	})

	t.Run("compare JSON with null", func(t *testing.T) {
		v1 := NewJSON(`{"a": 1}`)
		v2 := NewNull()

		if Compare(v1, v2) <= 0 {
			t.Error("JSON value should be greater than NULL")
		}
	})
}
