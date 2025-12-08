// pkg/dbfile/header_test.go
package dbfile

import (
	"bytes"
	"testing"
)

func TestHeader_MagicString(t *testing.T) {
	// The magic string should be exactly 16 bytes and match TurDB format
	expected := "TurDB format 1\x00\x00"
	if len(MagicString) != 16 {
		t.Errorf("MagicString length = %d, want 16", len(MagicString))
	}
	if MagicString != expected {
		t.Errorf("MagicString = %q, want %q", MagicString, expected)
	}
}

func TestHeader_Size(t *testing.T) {
	// Header should be exactly 100 bytes (like SQLite)
	if HeaderSize != 100 {
		t.Errorf("HeaderSize = %d, want 100", HeaderSize)
	}
}

func TestHeader_NewDefault(t *testing.T) {
	h := NewHeader()

	// Check default values
	if h.PageSize != DefaultPageSize {
		t.Errorf("PageSize = %d, want %d", h.PageSize, DefaultPageSize)
	}
	if h.FormatWriteVersion != 1 {
		t.Errorf("FormatWriteVersion = %d, want 1", h.FormatWriteVersion)
	}
	if h.FormatReadVersion != 1 {
		t.Errorf("FormatReadVersion = %d, want 1", h.FormatReadVersion)
	}
	if h.PageCount != 1 {
		t.Errorf("PageCount = %d, want 1 (header page)", h.PageCount)
	}
}

func TestHeader_Encode(t *testing.T) {
	h := NewHeader()
	h.PageSize = 4096
	h.PageCount = 10
	h.ChangeCounter = 5

	data := h.Encode()

	// Should be exactly 100 bytes
	if len(data) != HeaderSize {
		t.Errorf("Encode() length = %d, want %d", len(data), HeaderSize)
	}

	// Magic string should be at offset 0
	if !bytes.HasPrefix(data, []byte(MagicString)) {
		t.Errorf("Encode() missing magic string at offset 0")
	}
}

func TestHeader_Decode(t *testing.T) {
	// Create and encode a header
	original := NewHeader()
	original.PageSize = 8192
	original.PageCount = 100
	original.ChangeCounter = 42
	original.SchemaVersion = 3
	original.FreeListHead = 5
	original.FreeListCount = 2

	data := original.Encode()

	// Decode it
	decoded, err := DecodeHeader(data)
	if err != nil {
		t.Fatalf("DecodeHeader() error = %v", err)
	}

	// Verify all fields match
	if decoded.PageSize != original.PageSize {
		t.Errorf("PageSize = %d, want %d", decoded.PageSize, original.PageSize)
	}
	if decoded.PageCount != original.PageCount {
		t.Errorf("PageCount = %d, want %d", decoded.PageCount, original.PageCount)
	}
	if decoded.ChangeCounter != original.ChangeCounter {
		t.Errorf("ChangeCounter = %d, want %d", decoded.ChangeCounter, original.ChangeCounter)
	}
	if decoded.SchemaVersion != original.SchemaVersion {
		t.Errorf("SchemaVersion = %d, want %d", decoded.SchemaVersion, original.SchemaVersion)
	}
	if decoded.FreeListHead != original.FreeListHead {
		t.Errorf("FreeListHead = %d, want %d", decoded.FreeListHead, original.FreeListHead)
	}
	if decoded.FreeListCount != original.FreeListCount {
		t.Errorf("FreeListCount = %d, want %d", decoded.FreeListCount, original.FreeListCount)
	}
}

func TestHeader_Decode_InvalidMagic(t *testing.T) {
	data := make([]byte, HeaderSize)
	copy(data, "InvalidMagic1234") // Wrong magic string

	_, err := DecodeHeader(data)
	if err == nil {
		t.Error("DecodeHeader() should return error for invalid magic string")
	}
	if err != ErrInvalidMagic {
		t.Errorf("DecodeHeader() error = %v, want ErrInvalidMagic", err)
	}
}

func TestHeader_Decode_TooShort(t *testing.T) {
	data := make([]byte, 50) // Too short
	copy(data, MagicString)

	_, err := DecodeHeader(data)
	if err == nil {
		t.Error("DecodeHeader() should return error for short data")
	}
	if err != ErrHeaderTooShort {
		t.Errorf("DecodeHeader() error = %v, want ErrHeaderTooShort", err)
	}
}

func TestHeader_FormatVersion(t *testing.T) {
	h := NewHeader()

	// Test that format versions are reasonable
	if h.FormatWriteVersion < 1 || h.FormatWriteVersion > 255 {
		t.Errorf("FormatWriteVersion = %d, should be 1-255", h.FormatWriteVersion)
	}
	if h.FormatReadVersion < 1 || h.FormatReadVersion > 255 {
		t.Errorf("FormatReadVersion = %d, should be 1-255", h.FormatReadVersion)
	}
}
