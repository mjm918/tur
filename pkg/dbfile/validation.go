// pkg/dbfile/validation.go
// File format validation for database opening.
package dbfile

import "errors"

// Current supported version
const (
	CurrentFormatVersion = 1
	MaxSupportedVersion  = 1
)

// Validation errors.
var (
	ErrInvalidFormatVersion = errors.New("invalid format version")
	ErrUnsupportedVersion   = errors.New("unsupported database format version")
	ErrInvalidPageCount     = errors.New("invalid page count")
)

// ValidatePageSize checks if the page size is valid.
// Valid page sizes are powers of 2 between 512 and 65536.
func ValidatePageSize(pageSize uint16) error {
	// Must be at least 512
	if pageSize < 512 {
		return ErrInvalidPageSize
	}

	// Must be a power of 2
	if pageSize&(pageSize-1) != 0 {
		return ErrInvalidPageSize
	}

	return nil
}

// ValidateHeader validates all header fields.
func ValidateHeader(h *Header) error {
	// Validate page size
	if err := ValidatePageSize(h.PageSize); err != nil {
		return err
	}

	// Validate format versions
	if h.FormatWriteVersion == 0 {
		return ErrInvalidFormatVersion
	}
	if h.FormatReadVersion == 0 {
		return ErrInvalidFormatVersion
	}

	// Check if we can read this version
	if h.FormatReadVersion > MaxSupportedVersion {
		return ErrUnsupportedVersion
	}

	// Validate page count (must be at least 1)
	if h.PageCount == 0 {
		return ErrInvalidPageCount
	}

	return nil
}

// isPowerOfTwo returns true if n is a power of 2.
func isPowerOfTwo(n uint16) bool {
	return n > 0 && (n&(n-1)) == 0
}
