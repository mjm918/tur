// pkg/dbfile/metadata.go
// Schema catalog storage on page 1 (first page after header).
// This follows SQLite's sqlite_master table structure.
package dbfile

import (
	"encoding/binary"
	"errors"
)

const (
	// MetadataPageHeaderSize is the size of the metadata page header.
	// This includes btree page header information.
	MetadataPageHeaderSize = 12

	// SchemaRootPage is the page number for the schema catalog.
	// Page 0 contains the file header, page 1 is the schema root.
	SchemaRootPage = 1
)

// SchemaEntryType identifies the type of schema object.
type SchemaEntryType uint8

const (
	// SchemaEntryTable represents a table definition.
	SchemaEntryTable SchemaEntryType = 1
	// SchemaEntryIndex represents an index definition.
	SchemaEntryIndex SchemaEntryType = 2
	// SchemaEntryView represents a view definition.
	SchemaEntryView SchemaEntryType = 3
	// SchemaEntryTrigger represents a trigger definition.
	SchemaEntryTrigger SchemaEntryType = 4
)

// Errors for schema entry operations.
var (
	ErrSchemaEntryEmpty    = errors.New("schema entry data is empty")
	ErrSchemaEntryTooShort = errors.New("schema entry data too short")
	ErrInvalidSchemaType   = errors.New("invalid schema entry type")
)

// SchemaEntry represents a single entry in the schema catalog.
// This corresponds to a row in SQLite's sqlite_master table.
type SchemaEntry struct {
	Type      SchemaEntryType // type: 'table', 'index', 'view', 'trigger'
	Name      string          // name: Name of the object
	TableName string          // tbl_name: Name of table associated with this object
	RootPage  uint32          // rootpage: Root page of the B-tree (0 for views/triggers)
	SQL       string          // sql: Original SQL text used to create the object
}

// SchemaEntry serialization format:
// Offset  Size  Description
// 0       1     Type (1=table, 2=index, 3=view, 4=trigger)
// 1       4     Root page (uint32, little-endian)
// 5       2     Name length (uint16, little-endian)
// 7       N     Name (UTF-8)
// 7+N     2     TableName length (uint16, little-endian)
// 9+N     M     TableName (UTF-8)
// 9+N+M   4     SQL length (uint32, little-endian)
// 13+N+M  S     SQL (UTF-8)

// Encode serializes the schema entry to bytes.
func (e *SchemaEntry) Encode() []byte {
	nameLen := len(e.Name)
	tableNameLen := len(e.TableName)
	sqlLen := len(e.SQL)

	// Total size: 1 (type) + 4 (rootpage) + 2 (name len) + name + 2 (tbl len) + tbl + 4 (sql len) + sql
	size := 1 + 4 + 2 + nameLen + 2 + tableNameLen + 4 + sqlLen
	data := make([]byte, size)

	offset := 0

	// Type (1 byte)
	data[offset] = byte(e.Type)
	offset++

	// Root page (4 bytes)
	binary.LittleEndian.PutUint32(data[offset:], e.RootPage)
	offset += 4

	// Name length and content
	binary.LittleEndian.PutUint16(data[offset:], uint16(nameLen))
	offset += 2
	copy(data[offset:], e.Name)
	offset += nameLen

	// TableName length and content
	binary.LittleEndian.PutUint16(data[offset:], uint16(tableNameLen))
	offset += 2
	copy(data[offset:], e.TableName)
	offset += tableNameLen

	// SQL length and content
	binary.LittleEndian.PutUint32(data[offset:], uint32(sqlLen))
	offset += 4
	copy(data[offset:], e.SQL)

	return data
}

// DecodeSchemaEntry deserializes a schema entry from bytes.
func DecodeSchemaEntry(data []byte) (*SchemaEntry, error) {
	if len(data) == 0 {
		return nil, ErrSchemaEntryEmpty
	}

	// Minimum size: type (1) + rootpage (4) + name len (2) + tbl len (2) + sql len (4) = 13
	if len(data) < 13 {
		return nil, ErrSchemaEntryTooShort
	}

	e := &SchemaEntry{}
	offset := 0

	// Type
	e.Type = SchemaEntryType(data[offset])
	offset++

	// Root page
	e.RootPage = binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Name
	nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2
	if offset+nameLen > len(data) {
		return nil, ErrSchemaEntryTooShort
	}
	e.Name = string(data[offset : offset+nameLen])
	offset += nameLen

	// TableName
	if offset+2 > len(data) {
		return nil, ErrSchemaEntryTooShort
	}
	tableNameLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2
	if offset+tableNameLen > len(data) {
		return nil, ErrSchemaEntryTooShort
	}
	e.TableName = string(data[offset : offset+tableNameLen])
	offset += tableNameLen

	// SQL
	if offset+4 > len(data) {
		return nil, ErrSchemaEntryTooShort
	}
	sqlLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	if offset+sqlLen > len(data) {
		return nil, ErrSchemaEntryTooShort
	}
	e.SQL = string(data[offset : offset+sqlLen])

	return e, nil
}

// MetadataPageHeader represents the header of the schema catalog page.
// This follows the B-tree page header format.
type MetadataPageHeader struct {
	PageType       uint8  // Page type flags
	FreeBlockStart uint16 // Offset to first freeblock
	CellCount      uint16 // Number of cells on this page
	CellContentStart uint16 // Offset to start of cell content area
	FragmentedBytes uint8  // Number of fragmented free bytes
	RightChild     uint32 // Right-most child pointer (interior pages only)
}

// Encode serializes the metadata page header to bytes.
func (h *MetadataPageHeader) Encode() []byte {
	data := make([]byte, MetadataPageHeaderSize)

	data[0] = h.PageType
	binary.LittleEndian.PutUint16(data[1:], h.FreeBlockStart)
	binary.LittleEndian.PutUint16(data[3:], h.CellCount)
	binary.LittleEndian.PutUint16(data[5:], h.CellContentStart)
	data[7] = h.FragmentedBytes
	binary.LittleEndian.PutUint32(data[8:], h.RightChild)

	return data
}

// DecodeMetadataPageHeader deserializes a metadata page header from bytes.
func DecodeMetadataPageHeader(data []byte) (*MetadataPageHeader, error) {
	if len(data) < MetadataPageHeaderSize {
		return nil, ErrSchemaEntryTooShort
	}

	return &MetadataPageHeader{
		PageType:       data[0],
		FreeBlockStart: binary.LittleEndian.Uint16(data[1:]),
		CellCount:      binary.LittleEndian.Uint16(data[3:]),
		CellContentStart: binary.LittleEndian.Uint16(data[5:]),
		FragmentedBytes: data[7],
		RightChild:     binary.LittleEndian.Uint32(data[8:]),
	}, nil
}
