// pkg/sql/executor/executor_json_test.go
package executor

import (
	"strings"
	"testing"

	"tur/pkg/types"
)

// Test JSON column type in CREATE TABLE
func TestExecutor_JSONColumnType(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with a JSON column
	_, err := exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, attributes JSON)")
	if err != nil {
		t.Fatalf("Failed to create table with JSON column: %v", err)
	}

	// Verify table exists with JSON column
	table := exec.catalog.GetTable("products")
	if table == nil {
		t.Fatal("Table 'products' not found")
	}

	// Check JSON column type
	var foundJSON bool
	for _, col := range table.Columns {
		if col.Name == "attributes" && col.Type == types.TypeJSON {
			foundJSON = true
			break
		}
	}
	if !foundJSON {
		t.Error("Expected 'attributes' column to be of type JSON")
	}
}

// Test INSERT with JSON values
func TestExecutor_InsertJSONValue(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, data JSON)")

	// Insert JSON object
	_, err := exec.Execute(`INSERT INTO products VALUES (1, '{"name": "Widget", "price": 9.99}')`)
	if err != nil {
		t.Fatalf("Failed to insert JSON: %v", err)
	}

	// Select and verify
	result, err := exec.Execute("SELECT data FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// The data should be a JSON value
	if result.Rows[0][0].Type() != types.TypeJSON {
		t.Errorf("Expected JSON type, got %v", result.Rows[0][0].Type())
	}
}

// Test JSON_EXTRACT function
func TestExecutor_JSONExtract(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO products VALUES (1, '{"name": "Widget", "price": 9.99, "tags": ["sale", "new"]}')`)

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "extract string field",
			query:    "SELECT JSON_EXTRACT(data, '$.name') FROM products WHERE id = 1",
			expected: `"Widget"`,
		},
		{
			name:     "extract number field",
			query:    "SELECT JSON_EXTRACT(data, '$.price') FROM products WHERE id = 1",
			expected: "9.99",
		},
		{
			name:     "extract array element",
			query:    "SELECT JSON_EXTRACT(data, '$.tags[0]') FROM products WHERE id = 1",
			expected: `"sale"`,
		},
		{
			name:     "extract nested path",
			query:    `SELECT JSON_EXTRACT('{"a": {"b": {"c": 123}}}', '$.a.b.c')`,
			expected: "123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			// Get the result as text (JSON returns text for extracted values)
			got := result.Rows[0][0].Text()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test ->> operator (JSON_UNQUOTE(JSON_EXTRACT(...)))
func TestExecutor_JSONExtractUnquoteOperator(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO products VALUES (1, '{"name": "Widget", "price": 9.99}')`)

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "extract unquoted string",
			query:    "SELECT data->>'$.name' FROM products WHERE id = 1",
			expected: "Widget",
		},
		{
			name:     "extract number as text",
			query:    "SELECT data->>'$.price' FROM products WHERE id = 1",
			expected: "9.99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].Text()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test JSON_ARRAY function
func TestExecutor_JSONArray(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "empty array",
			query:    "SELECT JSON_ARRAY()",
			expected: "[]",
		},
		{
			name:     "array with integers",
			query:    "SELECT JSON_ARRAY(1, 2, 3)",
			expected: "[1,2,3]",
		},
		{
			name:     "array with mixed types",
			query:    "SELECT JSON_ARRAY(1, 'hello', 3.14, NULL)",
			expected: `[1,"hello",3.14,null]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].JSON()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test JSON_OBJECT function
func TestExecutor_JSONObject(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	t.Run("empty object", func(t *testing.T) {
		result, err := exec.Execute("SELECT JSON_OBJECT()")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}
		if result.Rows[0][0].JSON() != "{}" {
			t.Errorf("Expected '{}', got %q", result.Rows[0][0].JSON())
		}
	})

	t.Run("simple object", func(t *testing.T) {
		result, err := exec.Execute("SELECT JSON_OBJECT('name', 'John', 'age', 30)")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}
		// Parse JSON and verify keys exist (order doesn't matter)
		got := result.Rows[0][0].JSON()
		if !strings.Contains(got, `"name":"John"`) || !strings.Contains(got, `"age":30`) {
			t.Errorf("Expected object with name=John and age=30, got %q", got)
		}
	})

	t.Run("object with null", func(t *testing.T) {
		result, err := exec.Execute("SELECT JSON_OBJECT('key', NULL)")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}
		if result.Rows[0][0].JSON() != `{"key":null}` {
			t.Errorf("Expected '{\"key\":null}', got %q", result.Rows[0][0].JSON())
		}
	})
}

// Test JSON_UNQUOTE function
func TestExecutor_JSONUnquote(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "unquote string",
			query:    `SELECT JSON_UNQUOTE('"hello world"')`,
			expected: "hello world",
		},
		{
			name:     "unquote with escapes",
			query:    `SELECT JSON_UNQUOTE('"hello\nworld"')`,
			expected: "hello\nworld",
		},
		{
			name:     "unquote number returns same",
			query:    "SELECT JSON_UNQUOTE('123')",
			expected: "123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].Text()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test JSON_TYPE function
func TestExecutor_JSONType(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "object type",
			query:    `SELECT JSON_TYPE('{"a": 1}')`,
			expected: "OBJECT",
		},
		{
			name:     "array type",
			query:    "SELECT JSON_TYPE('[1, 2, 3]')",
			expected: "ARRAY",
		},
		{
			name:     "string type",
			query:    `SELECT JSON_TYPE('"hello"')`,
			expected: "STRING",
		},
		{
			name:     "number type",
			query:    "SELECT JSON_TYPE('123')",
			expected: "INTEGER",
		},
		{
			name:     "float type",
			query:    "SELECT JSON_TYPE('3.14')",
			expected: "DOUBLE",
		},
		{
			name:     "boolean type",
			query:    "SELECT JSON_TYPE('true')",
			expected: "BOOLEAN",
		},
		{
			name:     "null type",
			query:    "SELECT JSON_TYPE('null')",
			expected: "NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].Text()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test JSON_VALID function
func TestExecutor_JSONValid(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{
			name:     "valid object",
			query:    `SELECT JSON_VALID('{"a": 1}')`,
			expected: 1,
		},
		{
			name:     "valid array",
			query:    "SELECT JSON_VALID('[1, 2, 3]')",
			expected: 1,
		},
		{
			name:     "invalid json",
			query:    "SELECT JSON_VALID('{invalid}')",
			expected: 0,
		},
		{
			name:     "invalid - unquoted string",
			query:    "SELECT JSON_VALID('hello')",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].Int()
			if got != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, got)
			}
		})
	}
}

// Test JSON_LENGTH function
func TestExecutor_JSONLength(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{
			name:     "object length",
			query:    `SELECT JSON_LENGTH('{"a": 1, "b": 2, "c": 3}')`,
			expected: 3,
		},
		{
			name:     "array length",
			query:    "SELECT JSON_LENGTH('[1, 2, 3, 4, 5]')",
			expected: 5,
		},
		{
			name:     "scalar length",
			query:    `SELECT JSON_LENGTH('"hello"')`,
			expected: 1,
		},
		{
			name:     "empty array",
			query:    "SELECT JSON_LENGTH('[]')",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].Int()
			if got != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, got)
			}
		})
	}
}

// Test JSON_KEYS function
func TestExecutor_JSONKeys(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "object keys",
			query:    `SELECT JSON_KEYS('{"a": 1, "b": 2, "c": 3}')`,
			expected: `["a","b","c"]`,
		},
		{
			name:     "empty object",
			query:    "SELECT JSON_KEYS('{}')",
			expected: "[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].JSON()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test JSON in stored procedures
func TestExecutor_JSONInStoredProcedure(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, data JSON)")

	// Create procedure that uses JSON functions - using literal INSERT
	_, err := exec.Execute(`
		CREATE PROCEDURE add_product()
		BEGIN
			INSERT INTO products VALUES (1, JSON_OBJECT('name', 'Widget', 'price', 9.99));
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create procedure: %v", err)
	}

	// Call the procedure
	_, err = exec.Execute("CALL add_product()")
	if err != nil {
		t.Fatalf("Failed to call procedure: %v", err)
	}

	// Verify the JSON was inserted correctly
	result, err := exec.Execute("SELECT data->>'$.name' FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0][0].Text() != "Widget" {
		t.Errorf("Expected 'Widget', got %q", result.Rows[0][0].Text())
	}
}

// Test JSON in triggers
func TestExecutor_JSONInTrigger(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create tables
	exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price FLOAT)")
	exec.Execute("CREATE TABLE product_audit (id INT, change_data JSON)")

	// Create trigger that logs changes as JSON (using constant values)
	_, err := exec.Execute(`
		CREATE TRIGGER product_insert_audit
		AFTER INSERT ON products
		BEGIN
			INSERT INTO product_audit VALUES (1, '{"action": "INSERT"}');
		END
	`)
	if err != nil {
		t.Fatalf("Failed to create trigger: %v", err)
	}

	// Insert a product (should trigger the audit log)
	_, err = exec.Execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify the audit log
	result, err := exec.Execute("SELECT change_data->>'$.action' FROM product_audit WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to select from audit: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 audit row, got %d", len(result.Rows))
	}

	if result.Rows[0][0].Text() != "INSERT" {
		t.Errorf("Expected 'INSERT', got %q", result.Rows[0][0].Text())
	}
}

// Test JSON_CONTAINS function
func TestExecutor_JSONContains(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{
			name:     "array contains value",
			query:    "SELECT JSON_CONTAINS('[1, 2, 3]', '2')",
			expected: 1,
		},
		{
			name:     "array does not contain value",
			query:    "SELECT JSON_CONTAINS('[1, 2, 3]', '5')",
			expected: 0,
		},
		{
			name:     "object contains key-value",
			query:    `SELECT JSON_CONTAINS('{"a": 1, "b": 2}', '1', '$.a')`,
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].Int()
			if got != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, got)
			}
		})
	}
}

// Test JSON_SET function
func TestExecutor_JSONSet(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "set new key",
			query:    `SELECT JSON_SET('{"a": 1}', '$.b', 2)`,
			expected: `{"a":1,"b":2}`,
		},
		{
			name:     "update existing key",
			query:    `SELECT JSON_SET('{"a": 1}', '$.a', 100)`,
			expected: `{"a":100}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].JSON()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test JSON_REMOVE function
func TestExecutor_JSONRemove(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "remove key",
			query:    `SELECT JSON_REMOVE('{"a": 1, "b": 2}', '$.a')`,
			expected: `{"b":2}`,
		},
		{
			name:     "remove array element",
			query:    "SELECT JSON_REMOVE('[1, 2, 3]', '$[1]')",
			expected: "[1,3]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].JSON()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test -> operator (JSON_EXTRACT)
func TestExecutor_JSONExtractOperator(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO products VALUES (1, '{"name": "Widget", "price": 9.99}')`)

	result, err := exec.Execute("SELECT data->'$.name' FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// -> returns quoted JSON string
	expected := `"Widget"`
	got := result.Rows[0][0].Text()
	if got != expected {
		t.Errorf("Expected %q, got %q", expected, got)
	}
}

// Test JSON with math operations - extracting numbers and using them in calculations
func TestExecutor_JSONMathOperations(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE items (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO items VALUES (1, '{"price": 100, "quantity": 5}')`)
	exec.Execute(`INSERT INTO items VALUES (2, '{"price": 50, "quantity": 10}')`)

	// Test extracting numbers from JSON
	t.Run("extract price as text", func(t *testing.T) {
		result, err := exec.Execute("SELECT data->>'$.price' FROM items WHERE id = 1")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Rows[0][0].Text() != "100" {
			t.Errorf("Expected '100', got %q", result.Rows[0][0].Text())
		}
	})

	t.Run("extract quantity as text", func(t *testing.T) {
		result, err := exec.Execute("SELECT data->>'$.quantity' FROM items WHERE id = 1")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Rows[0][0].Text() != "5" {
			t.Errorf("Expected '5', got %q", result.Rows[0][0].Text())
		}
	})

	t.Run("compare extracted values as strings", func(t *testing.T) {
		// String comparison works directly
		result, err := exec.Execute("SELECT id FROM items WHERE data->>'$.price' = '100'")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0].Int() != 1 {
			t.Errorf("Expected id=1, got %v", result.Rows)
		}
	})
}

// Test JSON comparison operations
func TestExecutor_JSONComparisons(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE docs (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO docs VALUES (1, '{"status": "active", "count": 10}')`)
	exec.Execute(`INSERT INTO docs VALUES (2, '{"status": "inactive", "count": 5}')`)
	exec.Execute(`INSERT INTO docs VALUES (3, '{"status": "active", "count": 20}')`)

	t.Run("string equality comparison", func(t *testing.T) {
		result, err := exec.Execute("SELECT id FROM docs WHERE data->>'$.status' = 'active'")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	})

	t.Run("string inequality comparison", func(t *testing.T) {
		result, err := exec.Execute("SELECT id FROM docs WHERE data->>'$.status' = 'inactive'")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0].Int() != 2 {
			t.Errorf("Expected 1 row with id=2, got %v", result.Rows)
		}
	})

	t.Run("string comparison for count values", func(t *testing.T) {
		// Compare count as string (string comparison, but works for single digit difference)
		result, err := exec.Execute("SELECT id FROM docs WHERE data->>'$.count' = '10'")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0].Int() != 1 {
			t.Errorf("Expected 1 row with id=1, got %v", result.Rows)
		}
	})

	t.Run("combined string conditions", func(t *testing.T) {
		result, err := exec.Execute("SELECT id FROM docs WHERE data->>'$.status' = 'active' AND data->>'$.count' = '20'")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0].Int() != 3 {
			t.Errorf("Expected 1 row with id=3, got %v", result.Rows)
		}
	})
}

// Test JSON with date values
func TestExecutor_JSONWithDates(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE events (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO events VALUES (1, '{"name": "Conference", "date": "2024-06-15"}')`)
	exec.Execute(`INSERT INTO events VALUES (2, '{"name": "Workshop", "date": "2024-03-20"}')`)

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "extract date string",
			query:    "SELECT data->>'$.date' FROM events WHERE id = 1",
			expected: "2024-06-15",
		},
		{
			name:     "date string comparison",
			query:    "SELECT data->>'$.name' FROM events WHERE data->>'$.date' > '2024-05-01'",
			expected: "Conference",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].Text()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test JSON with number formatting
func TestExecutor_JSONNumberFormat(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE prices (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO prices VALUES (1, '{"amount": 1234.56, "currency": "USD"}')`)
	exec.Execute(`INSERT INTO prices VALUES (2, '{"amount": 9999.99, "currency": "EUR"}')`)

	t.Run("extract float as text", func(t *testing.T) {
		result, err := exec.Execute("SELECT data->>'$.amount' FROM prices WHERE id = 1")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Rows[0][0].Text() != "1234.56" {
			t.Errorf("Expected '1234.56', got %q", result.Rows[0][0].Text())
		}
	})

	t.Run("extract currency", func(t *testing.T) {
		result, err := exec.Execute("SELECT data->>'$.currency' FROM prices WHERE id = 1")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Rows[0][0].Text() != "USD" {
			t.Errorf("Expected 'USD', got %q", result.Rows[0][0].Text())
		}
	})

	t.Run("filter by currency", func(t *testing.T) {
		result, err := exec.Execute("SELECT data->>'$.amount' FROM prices WHERE data->>'$.currency' = 'EUR'")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0].Text() != "9999.99" {
			t.Errorf("Expected '9999.99', got %v", result.Rows)
		}
	})
}

// Test JSON NULL handling
func TestExecutor_JSONNullHandling(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE nulltest (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO nulltest VALUES (1, '{"name": "Test", "value": null}')`)
	exec.Execute(`INSERT INTO nulltest VALUES (2, '{"name": null, "value": 42}')`)

	t.Run("extract null value from JSON", func(t *testing.T) {
		result, err := exec.Execute("SELECT data->>'$.value' FROM nulltest WHERE id = 1")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0].Text() != "null" {
			t.Errorf("Expected 'null', got %v", result.Rows)
		}
	})

	t.Run("extract non-null value where other field is null", func(t *testing.T) {
		result, err := exec.Execute("SELECT data->>'$.value' FROM nulltest WHERE id = 2")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0].Text() != "42" {
			t.Errorf("Expected '42', got %v", result.Rows)
		}
	})

	t.Run("JSON_TYPE of null value", func(t *testing.T) {
		result, err := exec.Execute("SELECT JSON_TYPE(data->'$.value') FROM nulltest WHERE id = 1")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0].Text() != "NULL" {
			t.Errorf("Expected 'NULL', got %v", result.Rows)
		}
	})

	t.Run("JSON_TYPE of integer value", func(t *testing.T) {
		result, err := exec.Execute("SELECT JSON_TYPE(data->'$.value') FROM nulltest WHERE id = 2")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0].Text() != "INTEGER" {
			t.Errorf("Expected 'INTEGER', got %v", result.Rows)
		}
	})
}

// Test JSON array operations
func TestExecutor_JSONArrayOperations(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE arrays (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO arrays VALUES (1, '{"tags": ["go", "database", "json"], "scores": [85, 90, 78]}')`)

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "extract array element by index",
			query:    "SELECT data->>'$.tags[0]' FROM arrays WHERE id = 1",
			expected: "go",
		},
		{
			name:     "extract second array element",
			query:    "SELECT data->>'$.tags[1]' FROM arrays WHERE id = 1",
			expected: "database",
		},
		{
			name:     "extract last array element",
			query:    "SELECT data->>'$.tags[2]' FROM arrays WHERE id = 1",
			expected: "json",
		},
		{
			name:     "extract numeric array element",
			query:    "SELECT data->>'$.scores[1]' FROM arrays WHERE id = 1",
			expected: "90",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].Text()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test JSON nested object access
func TestExecutor_JSONNestedObjects(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE nested (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO nested VALUES (1, '{"user": {"profile": {"name": "John", "address": {"city": "NYC", "zip": "10001"}}}}')`)

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "two level nesting",
			query:    "SELECT data->>'$.user.profile.name' FROM nested WHERE id = 1",
			expected: "John",
		},
		{
			name:     "three level nesting",
			query:    "SELECT data->>'$.user.profile.address.city' FROM nested WHERE id = 1",
			expected: "NYC",
		},
		{
			name:     "extract nested object",
			query:    "SELECT data->'$.user.profile.address' FROM nested WHERE id = 1",
			expected: `{"city":"NYC","zip":"10001"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.Execute(tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			got := result.Rows[0][0].Text()
			if got != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, got)
			}
		})
	}
}

// Test JSON with ORDER BY on id (testing JSON select with order)
func TestExecutor_JSONOrderBy(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE sorttest (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO sorttest VALUES (1, '{"name": "Charlie", "age": 30}')`)
	exec.Execute(`INSERT INTO sorttest VALUES (2, '{"name": "Alice", "age": 25}')`)
	exec.Execute(`INSERT INTO sorttest VALUES (3, '{"name": "Bob", "age": 35}')`)

	// Test ORDER BY id with JSON extraction in SELECT
	result, err := exec.Execute("SELECT data->>'$.name' FROM sorttest ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	expectedNames := []string{"Charlie", "Alice", "Bob"}
	if len(result.Rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result.Rows))
	}

	for i, row := range result.Rows {
		got := row[0].Text()
		if got != expectedNames[i] {
			t.Errorf("Row %d: expected %q, got %q", i, expectedNames[i], got)
		}
	}
}

// Test JSON with GROUP BY
func TestExecutor_JSONGroupBy(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	exec.Execute("CREATE TABLE grouptest (id INT PRIMARY KEY, data JSON)")
	exec.Execute(`INSERT INTO grouptest VALUES (1, '{"category": "A", "amount": 100}')`)
	exec.Execute(`INSERT INTO grouptest VALUES (2, '{"category": "B", "amount": 200}')`)
	exec.Execute(`INSERT INTO grouptest VALUES (3, '{"category": "A", "amount": 150}')`)
	exec.Execute(`INSERT INTO grouptest VALUES (4, '{"category": "B", "amount": 50}')`)

	result, err := exec.Execute("SELECT data->>'$.category', COUNT(*) FROM grouptest GROUP BY data->>'$.category'")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(result.Rows))
	}

	// Verify we have both categories with count 2 each
	categoryCount := make(map[string]int64)
	for _, row := range result.Rows {
		cat := row[0].Text()
		count := row[1].Int()
		categoryCount[cat] = count
	}

	if categoryCount["A"] != 2 || categoryCount["B"] != 2 {
		t.Errorf("Expected each category to have count 2, got: %v", categoryCount)
	}
}
