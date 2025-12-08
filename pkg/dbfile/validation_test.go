// pkg/dbfile/validation_test.go
package dbfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidation_PageSize(t *testing.T) {
	tests := []struct {
		pageSize uint16
		valid    bool
	}{
		{512, true},    // Minimum valid
		{1024, true},
		{2048, true},
		{4096, true},   // Default
		{8192, true},
		{16384, true},
		{32768, true},
		{65535, false}, // Not power of 2
		{100, false},   // Too small
		{256, false},   // Too small
		{5000, false},  // Not power of 2
		{0, false},     // Invalid
	}

	for _, tt := range tests {
		err := ValidatePageSize(tt.pageSize)
		if tt.valid && err != nil {
			t.Errorf("ValidatePageSize(%d) error = %v, want nil", tt.pageSize, err)
		}
		if !tt.valid && err == nil {
			t.Errorf("ValidatePageSize(%d) should return error", tt.pageSize)
		}
	}
}

func TestValidation_Header_FormatVersion(t *testing.T) {
	h := NewHeader()
	h.FormatWriteVersion = 1
	h.FormatReadVersion = 1

	err := ValidateHeader(h)
	if err != nil {
		t.Errorf("ValidateHeader() error = %v for valid header", err)
	}

	// Invalid write version
	h.FormatWriteVersion = 0
	err = ValidateHeader(h)
	if err == nil {
		t.Error("ValidateHeader() should fail for write version 0")
	}
}

func TestValidation_Header_PageSize(t *testing.T) {
	h := NewHeader()
	h.PageSize = 4096

	err := ValidateHeader(h)
	if err != nil {
		t.Errorf("ValidateHeader() error = %v for valid page size", err)
	}

	// Invalid page size
	h.PageSize = 100
	err = ValidateHeader(h)
	if err == nil {
		t.Error("ValidateHeader() should fail for invalid page size")
	}
}

func TestValidation_Header_PageCount(t *testing.T) {
	h := NewHeader()
	h.PageCount = 1 // At least one page (header)

	err := ValidateHeader(h)
	if err != nil {
		t.Errorf("ValidateHeader() error = %v for valid page count", err)
	}

	// Zero page count is invalid
	h.PageCount = 0
	err = ValidateHeader(h)
	if err == nil {
		t.Error("ValidateHeader() should fail for page count 0")
	}
}

func TestValidation_Open_ValidatesHeader(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create a file with valid magic but invalid page size
	data := make([]byte, HeaderSize)
	copy(data, MagicString)
	// Set page size to invalid value (byte 16-17)
	data[16] = 100 // Invalid page size
	data[17] = 0

	if err := os.WriteFile(dbPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Open(dbPath, nil)
	if err == nil {
		t.Error("Open() should fail for invalid page size in header")
	}
	if err != ErrInvalidPageSize {
		t.Errorf("Open() error = %v, want ErrInvalidPageSize", err)
	}
}

func TestValidation_Open_ValidatesVersion(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create a file with valid magic but version 0
	h := NewHeader()
	h.FormatWriteVersion = 0 // Invalid
	data := h.Encode()
	// Ensure magic is correct
	copy(data, MagicString)
	// Set version to 0
	data[offsetFormatWriteVersion] = 0

	if err := os.WriteFile(dbPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Open(dbPath, nil)
	if err == nil {
		t.Error("Open() should fail for invalid format version")
	}
	if err != ErrInvalidFormatVersion {
		t.Errorf("Open() error = %v, want ErrInvalidFormatVersion", err)
	}
}

func TestValidation_FutureVersion(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create header with future version
	h := NewHeader()
	h.FormatReadVersion = 99 // Future version we can't read
	data := h.Encode()

	if err := os.WriteFile(dbPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Open(dbPath, nil)
	if err == nil {
		t.Error("Open() should fail for unsupported future version")
	}
	if err != ErrUnsupportedVersion {
		t.Errorf("Open() error = %v, want ErrUnsupportedVersion", err)
	}
}
