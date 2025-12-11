// pkg/schema/strict_types_test.go
package schema

import (
	"testing"

	"tur/pkg/types"
)

func TestColumnDefWithTypeParameters(t *testing.T) {
	// Test VARCHAR with max length
	varcharCol := ColumnDef{
		Name:      "name",
		Type:      types.TypeVarchar,
		MaxLength: 100,
	}
	if varcharCol.MaxLength != 100 {
		t.Errorf("expected MaxLength 100, got %d", varcharCol.MaxLength)
	}

	// Test CHAR with fixed length
	charCol := ColumnDef{
		Name:      "code",
		Type:      types.TypeChar,
		MaxLength: 3,
	}
	if charCol.MaxLength != 3 {
		t.Errorf("expected MaxLength 3, got %d", charCol.MaxLength)
	}

	// Test DECIMAL with precision and scale
	decimalCol := ColumnDef{
		Name:      "price",
		Type:      types.TypeDecimal,
		Precision: 10,
		Scale:     2,
	}
	if decimalCol.Precision != 10 {
		t.Errorf("expected Precision 10, got %d", decimalCol.Precision)
	}
	if decimalCol.Scale != 2 {
		t.Errorf("expected Scale 2, got %d", decimalCol.Scale)
	}
}

func TestColumnDefStrictIntegerTypes(t *testing.T) {
	tests := []struct {
		name     string
		colType  types.ValueType
		typeName string
	}{
		{"smallint_col", types.TypeSmallInt, "SMALLINT"},
		{"int_col", types.TypeInt32, "INT"},
		{"bigint_col", types.TypeBigInt, "BIGINT"},
		{"serial_col", types.TypeSerial, "SERIAL"},
		{"bigserial_col", types.TypeBigSerial, "BIGSERIAL"},
	}

	for _, tt := range tests {
		col := ColumnDef{
			Name: tt.name,
			Type: tt.colType,
		}
		if col.Type != tt.colType {
			t.Errorf("%s: expected type %v, got %v", tt.name, tt.colType, col.Type)
		}
		if col.Type.String() != tt.typeName {
			t.Errorf("%s: expected type name %s, got %s", tt.name, tt.typeName, col.Type.String())
		}
	}
}

func TestColumnDefGUIDType(t *testing.T) {
	col := ColumnDef{
		Name: "id",
		Type: types.TypeGUID,
	}
	if col.Type != types.TypeGUID {
		t.Errorf("expected TypeGUID, got %v", col.Type)
	}
	if col.Type.String() != "GUID" {
		t.Errorf("expected 'GUID', got %s", col.Type.String())
	}
}

func TestTableDefWithStrictTypes(t *testing.T) {
	table := TableDef{
		Name: "products",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeSerial, PrimaryKey: true},
			{Name: "sku", Type: types.TypeChar, MaxLength: 10},
			{Name: "name", Type: types.TypeVarchar, MaxLength: 255},
			{Name: "price", Type: types.TypeDecimal, Precision: 10, Scale: 2},
			{Name: "quantity", Type: types.TypeInt32},
			{Name: "guid", Type: types.TypeGUID},
		},
	}

	if len(table.Columns) != 6 {
		t.Fatalf("expected 6 columns, got %d", len(table.Columns))
	}

	// Check ID column
	idCol, idx := table.GetColumn("id")
	if idx == -1 {
		t.Fatal("id column not found")
	}
	if idCol.Type != types.TypeSerial {
		t.Errorf("id: expected TypeSerial, got %v", idCol.Type)
	}

	// Check SKU column
	skuCol, idx := table.GetColumn("sku")
	if idx == -1 {
		t.Fatal("sku column not found")
	}
	if skuCol.Type != types.TypeChar {
		t.Errorf("sku: expected TypeChar, got %v", skuCol.Type)
	}
	if skuCol.MaxLength != 10 {
		t.Errorf("sku: expected MaxLength 10, got %d", skuCol.MaxLength)
	}

	// Check price column
	priceCol, idx := table.GetColumn("price")
	if idx == -1 {
		t.Fatal("price column not found")
	}
	if priceCol.Type != types.TypeDecimal {
		t.Errorf("price: expected TypeDecimal, got %v", priceCol.Type)
	}
	if priceCol.Precision != 10 || priceCol.Scale != 2 {
		t.Errorf("price: expected Precision=10, Scale=2, got Precision=%d, Scale=%d",
			priceCol.Precision, priceCol.Scale)
	}
}

func TestIsStrictIntegerType(t *testing.T) {
	tests := []struct {
		typ      types.ValueType
		isStrict bool
	}{
		{types.TypeSmallInt, true},
		{types.TypeInt32, true},
		{types.TypeBigInt, true},
		{types.TypeSerial, true},
		{types.TypeBigSerial, true},
		{types.TypeText, false},
		{types.TypeFloat, false},
	}

	for _, tt := range tests {
		result := IsStrictIntegerType(tt.typ)
		if result != tt.isStrict {
			t.Errorf("IsStrictIntegerType(%v): expected %v, got %v", tt.typ, tt.isStrict, result)
		}
	}
}

func TestIsAutoIncrementType(t *testing.T) {
	tests := []struct {
		typ          types.ValueType
		isAutoIncr bool
	}{
		{types.TypeSerial, true},
		{types.TypeBigSerial, true},
		{types.TypeSmallInt, false},
		{types.TypeInt32, false},
		{types.TypeBigInt, false},
	}

	for _, tt := range tests {
		result := IsAutoIncrementType(tt.typ)
		if result != tt.isAutoIncr {
			t.Errorf("IsAutoIncrementType(%v): expected %v, got %v", tt.typ, tt.isAutoIncr, result)
		}
	}
}

func TestRequiresLengthParameter(t *testing.T) {
	tests := []struct {
		typ         types.ValueType
		reqLength   bool
	}{
		{types.TypeVarchar, true},
		{types.TypeChar, true},
		{types.TypeText, false},
		{types.TypeInt32, false},
	}

	for _, tt := range tests {
		result := RequiresLengthParameter(tt.typ)
		if result != tt.reqLength {
			t.Errorf("RequiresLengthParameter(%v): expected %v, got %v", tt.typ, tt.reqLength, result)
		}
	}
}

func TestRequiresPrecisionScale(t *testing.T) {
	tests := []struct {
		typ           types.ValueType
		reqPrecScale  bool
	}{
		{types.TypeDecimal, true},
		{types.TypeFloat, false},
		{types.TypeInt32, false},
	}

	for _, tt := range tests {
		result := RequiresPrecisionScale(tt.typ)
		if result != tt.reqPrecScale {
			t.Errorf("RequiresPrecisionScale(%v): expected %v, got %v", tt.typ, tt.reqPrecScale, result)
		}
	}
}
