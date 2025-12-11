// pkg/sql/parser/strict_types_test.go
package parser

import (
	"testing"

	"tur/pkg/types"
)

func TestParseCreateTableWithStrictTypes(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		columnName  string
		expectedType types.ValueType
		maxLen      int
		precision   int
		scale       int
	}{
		{
			name:         "SMALLINT type",
			sql:          "CREATE TABLE t (id SMALLINT)",
			columnName:   "id",
			expectedType: types.TypeSmallInt,
		},
		{
			name:         "INT type (strict)",
			sql:          "CREATE TABLE t (id INT)",
			columnName:   "id",
			expectedType: types.TypeInt32,
		},
		{
			name:         "BIGINT type",
			sql:          "CREATE TABLE t (id BIGINT)",
			columnName:   "id",
			expectedType: types.TypeBigInt,
		},
		{
			name:         "SERIAL type",
			sql:          "CREATE TABLE t (id SERIAL)",
			columnName:   "id",
			expectedType: types.TypeSerial,
		},
		{
			name:         "BIGSERIAL type",
			sql:          "CREATE TABLE t (id BIGSERIAL)",
			columnName:   "id",
			expectedType: types.TypeBigSerial,
		},
		{
			name:         "GUID type",
			sql:          "CREATE TABLE t (id GUID)",
			columnName:   "id",
			expectedType: types.TypeGUID,
		},
		{
			name:         "UUID type (alias for GUID)",
			sql:          "CREATE TABLE t (id UUID)",
			columnName:   "id",
			expectedType: types.TypeGUID,
		},
		{
			name:         "VARCHAR with length",
			sql:          "CREATE TABLE t (name VARCHAR(100))",
			columnName:   "name",
			expectedType: types.TypeVarchar,
			maxLen:       100,
		},
		{
			name:         "CHAR with length",
			sql:          "CREATE TABLE t (code CHAR(3))",
			columnName:   "code",
			expectedType: types.TypeChar,
			maxLen:       3,
		},
		{
			name:         "DECIMAL with precision and scale",
			sql:          "CREATE TABLE t (price DECIMAL(10, 2))",
			columnName:   "price",
			expectedType: types.TypeDecimal,
			precision:    10,
			scale:        2,
		},
		{
			name:         "NUMERIC (alias for DECIMAL)",
			sql:          "CREATE TABLE t (amount NUMERIC(15, 4))",
			columnName:   "amount",
			expectedType: types.TypeDecimal,
			precision:    15,
			scale:        4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(tt.sql)
			stmt, err := p.Parse()
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			createStmt, ok := stmt.(*CreateTableStmt)
			if !ok {
				t.Fatalf("expected CreateTableStmt, got %T", stmt)
			}

			if len(createStmt.Columns) == 0 {
				t.Fatal("expected at least one column")
			}

			col := createStmt.Columns[0]
			if col.Name != tt.columnName {
				t.Errorf("expected column name %s, got %s", tt.columnName, col.Name)
			}

			if col.Type != tt.expectedType {
				t.Errorf("expected type %v, got %v", tt.expectedType, col.Type)
			}

			if tt.maxLen > 0 && col.MaxLength != tt.maxLen {
				t.Errorf("expected MaxLength %d, got %d", tt.maxLen, col.MaxLength)
			}

			if tt.precision > 0 && col.Precision != tt.precision {
				t.Errorf("expected Precision %d, got %d", tt.precision, col.Precision)
			}

			if tt.scale > 0 && col.Scale != tt.scale {
				t.Errorf("expected Scale %d, got %d", tt.scale, col.Scale)
			}
		})
	}
}

func TestParseCreateTableWithMultipleStrictTypes(t *testing.T) {
	sql := `CREATE TABLE products (
		id SERIAL PRIMARY KEY,
		sku CHAR(10) NOT NULL,
		name VARCHAR(255),
		price DECIMAL(10, 2),
		quantity INT,
		product_guid UUID
	)`

	p := New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}

	if len(createStmt.Columns) != 6 {
		t.Fatalf("expected 6 columns, got %d", len(createStmt.Columns))
	}

	// Check each column
	expected := []struct {
		name      string
		typ       types.ValueType
		maxLen    int
		precision int
		scale     int
	}{
		{"id", types.TypeSerial, 0, 0, 0},
		{"sku", types.TypeChar, 10, 0, 0},
		{"name", types.TypeVarchar, 255, 0, 0},
		{"price", types.TypeDecimal, 0, 10, 2},
		{"quantity", types.TypeInt32, 0, 0, 0},
		{"product_guid", types.TypeGUID, 0, 0, 0},
	}

	for i, exp := range expected {
		col := createStmt.Columns[i]
		if col.Name != exp.name {
			t.Errorf("column %d: expected name %s, got %s", i, exp.name, col.Name)
		}
		if col.Type != exp.typ {
			t.Errorf("column %s: expected type %v, got %v", exp.name, exp.typ, col.Type)
		}
		if exp.maxLen > 0 && col.MaxLength != exp.maxLen {
			t.Errorf("column %s: expected maxLen %d, got %d", exp.name, exp.maxLen, col.MaxLength)
		}
		if exp.precision > 0 && col.Precision != exp.precision {
			t.Errorf("column %s: expected precision %d, got %d", exp.name, exp.precision, col.Precision)
		}
		if exp.scale > 0 && col.Scale != exp.scale {
			t.Errorf("column %s: expected scale %d, got %d", exp.name, exp.scale, col.Scale)
		}
	}
}

func TestParseVarcharWithoutLength(t *testing.T) {
	// VARCHAR without length should fail
	sql := "CREATE TABLE t (name VARCHAR)"
	p := New(sql)
	_, err := p.Parse()
	if err == nil {
		t.Error("expected error for VARCHAR without length parameter")
	}
}

func TestParseCharWithoutLength(t *testing.T) {
	// CHAR without length should default to CHAR(1)
	sql := "CREATE TABLE t (flag CHAR)"
	p := New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}

	col := createStmt.Columns[0]
	if col.Type != types.TypeChar {
		t.Errorf("expected TypeChar, got %v", col.Type)
	}
	if col.MaxLength != 1 {
		t.Errorf("expected MaxLength 1 for CHAR without length, got %d", col.MaxLength)
	}
}

func TestParseDecimalWithOnlyPrecision(t *testing.T) {
	// DECIMAL(precision) without scale should default scale to 0
	sql := "CREATE TABLE t (num DECIMAL(5))"
	p := New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}

	col := createStmt.Columns[0]
	if col.Type != types.TypeDecimal {
		t.Errorf("expected TypeDecimal, got %v", col.Type)
	}
	if col.Precision != 5 {
		t.Errorf("expected Precision 5, got %d", col.Precision)
	}
	if col.Scale != 0 {
		t.Errorf("expected Scale 0 for DECIMAL(5), got %d", col.Scale)
	}
}

func TestParseSerialWithPrimaryKey(t *testing.T) {
	sql := "CREATE TABLE t (id SERIAL PRIMARY KEY)"
	p := New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}

	col := createStmt.Columns[0]
	if col.Type != types.TypeSerial {
		t.Errorf("expected TypeSerial, got %v", col.Type)
	}
	if !col.PrimaryKey {
		t.Error("expected PrimaryKey to be true")
	}
}

func TestINT_MapsTo_TypeInt32(t *testing.T) {
	// INT keyword maps to TypeInt32
	sql := "CREATE TABLE t (id INT)"
	p := New(sql)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected CreateTableStmt, got %T", stmt)
	}

	col := createStmt.Columns[0]
	// INT maps to TypeInt32
	if col.Type != types.TypeInt32 {
		t.Errorf("expected TypeInt32 for INT keyword, got %v", col.Type)
	}
}
