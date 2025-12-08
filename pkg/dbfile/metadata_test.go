// pkg/dbfile/metadata_test.go
package dbfile

import (
	"testing"
)

func TestMetadataPageHeader_Size(t *testing.T) {
	// Metadata page header should be well-defined
	if MetadataPageHeaderSize < 8 {
		t.Errorf("MetadataPageHeaderSize = %d, want at least 8 bytes", MetadataPageHeaderSize)
	}
}

func TestSchemaEntry_Types(t *testing.T) {
	// Test all valid schema entry types
	tests := []struct {
		entryType SchemaEntryType
		name      string
	}{
		{SchemaEntryTable, "table"},
		{SchemaEntryIndex, "index"},
		{SchemaEntryView, "view"},
		{SchemaEntryTrigger, "trigger"},
	}

	for _, tt := range tests {
		if tt.entryType < SchemaEntryTable || tt.entryType > SchemaEntryTrigger {
			t.Errorf("SchemaEntryType %s has invalid value %d", tt.name, tt.entryType)
		}
	}
}

func TestSchemaEntry_Encode(t *testing.T) {
	entry := &SchemaEntry{
		Type:     SchemaEntryTable,
		Name:     "users",
		TableName: "users",
		RootPage: 2,
		SQL:      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
	}

	data := entry.Encode()

	if len(data) == 0 {
		t.Error("Encode() returned empty data")
	}

	// Should contain type byte
	if data[0] != byte(SchemaEntryTable) {
		t.Errorf("Encode() first byte = %d, want %d (table type)", data[0], SchemaEntryTable)
	}
}

func TestSchemaEntry_Decode(t *testing.T) {
	original := &SchemaEntry{
		Type:      SchemaEntryTable,
		Name:      "products",
		TableName: "products",
		RootPage:  5,
		SQL:       "CREATE TABLE products (id INTEGER, price REAL)",
	}

	data := original.Encode()
	decoded, err := DecodeSchemaEntry(data)
	if err != nil {
		t.Fatalf("DecodeSchemaEntry() error = %v", err)
	}

	if decoded.Type != original.Type {
		t.Errorf("Type = %d, want %d", decoded.Type, original.Type)
	}
	if decoded.Name != original.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, original.Name)
	}
	if decoded.TableName != original.TableName {
		t.Errorf("TableName = %q, want %q", decoded.TableName, original.TableName)
	}
	if decoded.RootPage != original.RootPage {
		t.Errorf("RootPage = %d, want %d", decoded.RootPage, original.RootPage)
	}
	if decoded.SQL != original.SQL {
		t.Errorf("SQL = %q, want %q", decoded.SQL, original.SQL)
	}
}

func TestSchemaEntry_DecodeIndex(t *testing.T) {
	original := &SchemaEntry{
		Type:      SchemaEntryIndex,
		Name:      "idx_users_name",
		TableName: "users",
		RootPage:  10,
		SQL:       "CREATE INDEX idx_users_name ON users(name)",
	}

	data := original.Encode()
	decoded, err := DecodeSchemaEntry(data)
	if err != nil {
		t.Fatalf("DecodeSchemaEntry() error = %v", err)
	}

	if decoded.Type != SchemaEntryIndex {
		t.Errorf("Type = %d, want %d (index)", decoded.Type, SchemaEntryIndex)
	}
	if decoded.Name != original.Name {
		t.Errorf("Name = %q, want %q", decoded.Name, original.Name)
	}
	if decoded.TableName != original.TableName {
		t.Errorf("TableName = %q, want %q", decoded.TableName, original.TableName)
	}
}

func TestSchemaEntry_DecodeEmpty(t *testing.T) {
	_, err := DecodeSchemaEntry(nil)
	if err == nil {
		t.Error("DecodeSchemaEntry(nil) should return error")
	}
	if err != ErrSchemaEntryEmpty {
		t.Errorf("error = %v, want ErrSchemaEntryEmpty", err)
	}
}

func TestSchemaEntry_DecodeTooShort(t *testing.T) {
	data := []byte{byte(SchemaEntryTable)} // Just type, no content

	_, err := DecodeSchemaEntry(data)
	if err == nil {
		t.Error("DecodeSchemaEntry() should return error for short data")
	}
	if err != ErrSchemaEntryTooShort {
		t.Errorf("error = %v, want ErrSchemaEntryTooShort", err)
	}
}

func TestMetadataPage_Layout(t *testing.T) {
	// Page 1 is the schema catalog root page
	// It contains a B-tree that stores schema entries
	// The page layout follows SQLite's btree page format

	// First 100 bytes of page 0 is the file header
	// Page 1 starts at offset pageSize (e.g., 4096)
	// Page 1 has:
	// - 8-12 byte page header (btree header)
	// - Cell pointers (2 bytes each)
	// - Cell content (schema entries)

	// For now, we just verify the constants are defined
	if SchemaRootPage != 1 {
		t.Errorf("SchemaRootPage = %d, want 1", SchemaRootPage)
	}
}

func TestSchemaEntry_View(t *testing.T) {
	entry := &SchemaEntry{
		Type:      SchemaEntryView,
		Name:      "active_users",
		TableName: "active_users",
		RootPage:  0, // Views don't have root pages
		SQL:       "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1",
	}

	data := entry.Encode()
	decoded, err := DecodeSchemaEntry(data)
	if err != nil {
		t.Fatalf("DecodeSchemaEntry() error = %v", err)
	}

	if decoded.Type != SchemaEntryView {
		t.Errorf("Type = %d, want %d (view)", decoded.Type, SchemaEntryView)
	}
	if decoded.RootPage != 0 {
		t.Errorf("RootPage = %d, want 0 for view", decoded.RootPage)
	}
}

func TestSchemaEntry_LongSQL(t *testing.T) {
	// Test with a long SQL statement
	longSQL := "CREATE TABLE test_table ("
	for i := 0; i < 50; i++ {
		if i > 0 {
			longSQL += ", "
		}
		longSQL += "column_" + string(rune('a'+i%26)) + " TEXT"
	}
	longSQL += ")"

	entry := &SchemaEntry{
		Type:      SchemaEntryTable,
		Name:      "test_table",
		TableName: "test_table",
		RootPage:  3,
		SQL:       longSQL,
	}

	data := entry.Encode()
	decoded, err := DecodeSchemaEntry(data)
	if err != nil {
		t.Fatalf("DecodeSchemaEntry() error = %v", err)
	}

	if decoded.SQL != longSQL {
		t.Errorf("SQL mismatch for long SQL statement")
	}
}
