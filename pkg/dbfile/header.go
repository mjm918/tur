// pkg/dbfile/header.go
// Package dbfile implements the TurDB database file format.
// The file format is inspired by SQLite with extensions for vector data.
package dbfile

import (
	"encoding/binary"
	"errors"
)

const (
	// HeaderSize is the size of the database file header in bytes.
	// The first 100 bytes of page 0 contain the file header.
	HeaderSize = 100

	// MagicString identifies a valid TurDB database file.
	// It must be exactly 16 bytes.
	MagicString = "TurDB format 1\x00\x00"

	// DefaultPageSize is the default page size in bytes.
	DefaultPageSize = 4096
)

// Header field offsets (matching SQLite layout where applicable)
const (
	offsetMagic              = 0  // 16 bytes: magic string
	offsetPageSize           = 16 // 2 bytes: page size (1 = 65536)
	offsetFormatWriteVersion = 18 // 1 byte: file format write version
	offsetFormatReadVersion  = 19 // 1 byte: file format read version
	offsetReservedPerPage    = 20 // 1 byte: reserved bytes at end of each page
	offsetMaxPayloadFrac     = 21 // 1 byte: max embedded payload fraction
	offsetMinPayloadFrac     = 22 // 1 byte: min embedded payload fraction
	offsetMinLeafPayloadFrac = 23 // 1 byte: min leaf payload fraction
	offsetChangeCounter      = 24 // 4 bytes: file change counter
	offsetPageCount          = 28 // 4 bytes: size of database in pages
	offsetFreeListHead       = 32 // 4 bytes: first freelist page
	offsetFreeListCount      = 36 // 4 bytes: number of freelist pages
	offsetSchemaCookie       = 40 // 4 bytes: schema cookie
	offsetSchemaVersion      = 44 // 4 bytes: schema format version
	offsetDefaultCacheSize   = 48 // 4 bytes: default page cache size
	offsetLargestRootPage    = 52 // 4 bytes: largest root page (autovacuum)
	offsetTextEncoding       = 56 // 4 bytes: 1=UTF-8, 2=UTF-16le, 3=UTF-16be
	offsetUserVersion        = 60 // 4 bytes: user version
	offsetIncrementalVacuum  = 64 // 4 bytes: incremental vacuum mode
	offsetApplicationID      = 68 // 4 bytes: application ID
	offsetReserved           = 72 // 20 bytes: reserved for expansion
	offsetVersionValidFor    = 92 // 4 bytes: version-valid-for number
	offsetTurDBVersion       = 96 // 4 bytes: TurDB version number
)

// Errors
var (
	ErrInvalidMagic    = errors.New("invalid magic string: not a TurDB database")
	ErrHeaderTooShort  = errors.New("header data too short")
	ErrInvalidPageSize = errors.New("invalid page size")
)

// Header represents the 100-byte database file header.
type Header struct {
	PageSize           uint16 // Page size in bytes (power of 2 between 512 and 65536)
	FormatWriteVersion uint8  // File format write version
	FormatReadVersion  uint8  // File format read version
	ReservedPerPage    uint8  // Reserved bytes at end of each page
	MaxPayloadFrac     uint8  // Max embedded payload fraction (default 64)
	MinPayloadFrac     uint8  // Min embedded payload fraction (default 32)
	MinLeafPayloadFrac uint8  // Min leaf payload fraction (default 32)
	ChangeCounter      uint32 // Incremented on each change
	PageCount          uint32 // Total number of pages in the database
	FreeListHead       uint32 // Page number of first freelist page (0 if none)
	FreeListCount      uint32 // Total number of freelist pages
	SchemaCookie       uint32 // Schema cookie (incremented on schema change)
	SchemaVersion      uint32 // Schema format version
	DefaultCacheSize   uint32 // Suggested cache size
	LargestRootPage    uint32 // Largest root page (for autovacuum)
	TextEncoding       uint32 // Text encoding (1=UTF-8)
	UserVersion        uint32 // User-defined version
	IncrementalVacuum  uint32 // Incremental vacuum mode
	ApplicationID      uint32 // Application ID
	VersionValidFor    uint32 // Change counter at time of version number
	TurDBVersion       uint32 // TurDB version number that created this DB
}

// NewHeader creates a new header with default values.
func NewHeader() *Header {
	return &Header{
		PageSize:           DefaultPageSize,
		FormatWriteVersion: 1,
		FormatReadVersion:  1,
		ReservedPerPage:    0,
		MaxPayloadFrac:     64,
		MinPayloadFrac:     32,
		MinLeafPayloadFrac: 32,
		ChangeCounter:      0,
		PageCount:          1, // Header page itself
		FreeListHead:       0,
		FreeListCount:      0,
		SchemaCookie:       0,
		SchemaVersion:      0,
		DefaultCacheSize:   1000,
		LargestRootPage:    0,
		TextEncoding:       1, // UTF-8
		UserVersion:        0,
		IncrementalVacuum:  0,
		ApplicationID:      0,
		VersionValidFor:    0,
		TurDBVersion:       1, // Version 1
	}
}

// Encode serializes the header to a 100-byte slice.
func (h *Header) Encode() []byte {
	data := make([]byte, HeaderSize)

	// Magic string (16 bytes)
	copy(data[offsetMagic:], MagicString)

	// Page size (2 bytes, little-endian for TurDB)
	binary.LittleEndian.PutUint16(data[offsetPageSize:], h.PageSize)

	// Format versions (1 byte each)
	data[offsetFormatWriteVersion] = h.FormatWriteVersion
	data[offsetFormatReadVersion] = h.FormatReadVersion

	// Page configuration (1 byte each)
	data[offsetReservedPerPage] = h.ReservedPerPage
	data[offsetMaxPayloadFrac] = h.MaxPayloadFrac
	data[offsetMinPayloadFrac] = h.MinPayloadFrac
	data[offsetMinLeafPayloadFrac] = h.MinLeafPayloadFrac

	// Counters and sizes (4 bytes each, little-endian)
	binary.LittleEndian.PutUint32(data[offsetChangeCounter:], h.ChangeCounter)
	binary.LittleEndian.PutUint32(data[offsetPageCount:], h.PageCount)
	binary.LittleEndian.PutUint32(data[offsetFreeListHead:], h.FreeListHead)
	binary.LittleEndian.PutUint32(data[offsetFreeListCount:], h.FreeListCount)
	binary.LittleEndian.PutUint32(data[offsetSchemaCookie:], h.SchemaCookie)
	binary.LittleEndian.PutUint32(data[offsetSchemaVersion:], h.SchemaVersion)
	binary.LittleEndian.PutUint32(data[offsetDefaultCacheSize:], h.DefaultCacheSize)
	binary.LittleEndian.PutUint32(data[offsetLargestRootPage:], h.LargestRootPage)
	binary.LittleEndian.PutUint32(data[offsetTextEncoding:], h.TextEncoding)
	binary.LittleEndian.PutUint32(data[offsetUserVersion:], h.UserVersion)
	binary.LittleEndian.PutUint32(data[offsetIncrementalVacuum:], h.IncrementalVacuum)
	binary.LittleEndian.PutUint32(data[offsetApplicationID:], h.ApplicationID)
	// Reserved bytes (72-91) are left as zeros
	binary.LittleEndian.PutUint32(data[offsetVersionValidFor:], h.VersionValidFor)
	binary.LittleEndian.PutUint32(data[offsetTurDBVersion:], h.TurDBVersion)

	return data
}

// DecodeHeader deserializes a header from a byte slice.
func DecodeHeader(data []byte) (*Header, error) {
	if len(data) < HeaderSize {
		return nil, ErrHeaderTooShort
	}

	// Check magic string
	if string(data[offsetMagic:offsetMagic+16]) != MagicString {
		return nil, ErrInvalidMagic
	}

	h := &Header{
		PageSize:           binary.LittleEndian.Uint16(data[offsetPageSize:]),
		FormatWriteVersion: data[offsetFormatWriteVersion],
		FormatReadVersion:  data[offsetFormatReadVersion],
		ReservedPerPage:    data[offsetReservedPerPage],
		MaxPayloadFrac:     data[offsetMaxPayloadFrac],
		MinPayloadFrac:     data[offsetMinPayloadFrac],
		MinLeafPayloadFrac: data[offsetMinLeafPayloadFrac],
		ChangeCounter:      binary.LittleEndian.Uint32(data[offsetChangeCounter:]),
		PageCount:          binary.LittleEndian.Uint32(data[offsetPageCount:]),
		FreeListHead:       binary.LittleEndian.Uint32(data[offsetFreeListHead:]),
		FreeListCount:      binary.LittleEndian.Uint32(data[offsetFreeListCount:]),
		SchemaCookie:       binary.LittleEndian.Uint32(data[offsetSchemaCookie:]),
		SchemaVersion:      binary.LittleEndian.Uint32(data[offsetSchemaVersion:]),
		DefaultCacheSize:   binary.LittleEndian.Uint32(data[offsetDefaultCacheSize:]),
		LargestRootPage:    binary.LittleEndian.Uint32(data[offsetLargestRootPage:]),
		TextEncoding:       binary.LittleEndian.Uint32(data[offsetTextEncoding:]),
		UserVersion:        binary.LittleEndian.Uint32(data[offsetUserVersion:]),
		IncrementalVacuum:  binary.LittleEndian.Uint32(data[offsetIncrementalVacuum:]),
		ApplicationID:      binary.LittleEndian.Uint32(data[offsetApplicationID:]),
		VersionValidFor:    binary.LittleEndian.Uint32(data[offsetVersionValidFor:]),
		TurDBVersion:       binary.LittleEndian.Uint32(data[offsetTurDBVersion:]),
	}

	return h, nil
}
