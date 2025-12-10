// pkg/pager/corruption.go
package pager

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// CorruptionError represents a page corruption detection error
type CorruptionError struct {
	PageNo       uint32
	PageType     PageType
	ExpectedCRC  uint32
	ActualCRC    uint32
	IsTornWrite  bool
	Message      string
}

// Error implements the error interface
func (e *CorruptionError) Error() string {
	if e.IsTornWrite {
		return fmt.Sprintf("torn page write detected on page %d: %s", e.PageNo, e.Message)
	}
	return fmt.Sprintf("page %d corruption: expected CRC %08x, got %08x",
		e.PageNo, e.ExpectedCRC, e.ActualCRC)
}

// PageChecksumSize is the number of bytes used for checksum at the end of each page
const PageChecksumSize = 4

// TornWriteMarkerSize is the size of the torn write detection marker
const TornWriteMarkerSize = 8

// TornWriteMarker is a magic value written at specific offsets to detect partial writes
var TornWriteMarker = []byte{0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55}

// CalculatePageChecksum calculates a CRC32 checksum for page data
// The checksum covers all bytes except the last PageChecksumSize bytes
// (where the checksum itself is stored)
func CalculatePageChecksum(data []byte) uint32 {
	if len(data) <= PageChecksumSize {
		return 0
	}
	// Calculate CRC32 of data excluding the checksum bytes at the end
	return crc32.ChecksumIEEE(data[:len(data)-PageChecksumSize])
}

// WritePageChecksum writes the checksum at the end of the page data
func WritePageChecksum(data []byte) {
	if len(data) <= PageChecksumSize {
		return
	}
	checksum := CalculatePageChecksum(data)
	binary.LittleEndian.PutUint32(data[len(data)-PageChecksumSize:], checksum)
}

// ReadPageChecksum reads the stored checksum from the end of page data
func ReadPageChecksum(data []byte) uint32 {
	if len(data) < PageChecksumSize {
		return 0
	}
	return binary.LittleEndian.Uint32(data[len(data)-PageChecksumSize:])
}

// ChecksumEnabled indicates whether page checksums are enabled for this database
// This is a placeholder for future configuration - currently checksums are optional
var ChecksumEnabled = false

// VerifyPageChecksum verifies the page checksum and returns any corruption error
// Note: This only reports errors when checksums are enabled and the checksum
// was explicitly written. Pages without checksums are considered valid.
func VerifyPageChecksum(pageNo uint32, data []byte) *CorruptionError {
	// Skip verification if checksums are not enabled
	if !ChecksumEnabled {
		return nil
	}

	if len(data) <= PageChecksumSize {
		return nil // Too small to have checksum, skip verification
	}

	expected := ReadPageChecksum(data)
	actual := CalculatePageChecksum(data)

	// A checksum of 0 might indicate an uninitialized page
	// In that case, check if the entire checksum area is zero
	if expected == 0 {
		// Check if page looks uninitialized (all zeros or has no checksum set)
		allZero := true
		for i := len(data) - PageChecksumSize; i < len(data); i++ {
			if data[i] != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			// Uninitialized page, skip checksum verification
			return nil
		}
	}

	if expected != actual {
		return &CorruptionError{
			PageNo:      pageNo,
			PageType:    PageType(data[0]),
			ExpectedCRC: expected,
			ActualCRC:   actual,
			Message:     "checksum mismatch",
		}
	}

	return nil
}

// DetectTornWrite checks for torn page writes by examining marker patterns
// A torn write occurs when a system crash happens during a page write,
// leaving the page partially updated
func DetectTornWrite(pageNo uint32, data []byte, pageSize int) *CorruptionError {
	if len(data) < pageSize {
		return nil
	}

	// Check torn write markers at known offsets
	// Markers should be at: start, middle, and end of page
	markerOffsets := []int{
		0,                   // Start of page (after page type byte)
		pageSize / 2,        // Middle of page
		pageSize - TornWriteMarkerSize - PageChecksumSize, // Before checksum
	}

	// Count valid markers
	validMarkers := 0
	invalidMarkers := 0

	for _, offset := range markerOffsets {
		if offset+TornWriteMarkerSize > len(data) {
			continue
		}

		// Check if this offset has the marker pattern
		hasMarker := true
		for i := 0; i < TornWriteMarkerSize; i++ {
			if data[offset+i] != TornWriteMarker[i] {
				hasMarker = false
				break
			}
		}

		if hasMarker {
			validMarkers++
		} else {
			// Check if this looks like it should have had a marker
			// (non-zero bytes that don't match)
			nonZero := false
			for i := 0; i < TornWriteMarkerSize; i++ {
				if data[offset+i] != 0 {
					nonZero = true
					break
				}
			}
			if nonZero {
				invalidMarkers++
			}
		}
	}

	// If we have some valid markers but also some invalid ones,
	// this could indicate a torn write
	if validMarkers > 0 && invalidMarkers > 0 {
		return &CorruptionError{
			PageNo:      pageNo,
			PageType:    PageType(data[0]),
			IsTornWrite: true,
			Message:     fmt.Sprintf("partial marker pattern: %d valid, %d invalid markers",
				validMarkers, invalidMarkers),
		}
	}

	return nil
}

// CorruptionChecker provides methods to scan for corruption in database pages
type CorruptionChecker struct {
	pager    *Pager
	pageSize int
}

// NewCorruptionChecker creates a new corruption checker for the given pager
func NewCorruptionChecker(p *Pager) *CorruptionChecker {
	return &CorruptionChecker{
		pager:    p,
		pageSize: p.PageSize(),
	}
}

// CheckPage verifies a single page for corruption
func (cc *CorruptionChecker) CheckPage(pageNo uint32) *CorruptionError {
	page, err := cc.pager.Get(pageNo)
	if err != nil {
		return &CorruptionError{
			PageNo:  pageNo,
			Message: fmt.Sprintf("failed to read page: %v", err),
		}
	}
	defer cc.pager.Release(page)

	data := page.Data()

	// First check for torn writes
	if err := DetectTornWrite(pageNo, data, cc.pageSize); err != nil {
		return err
	}

	// Then verify checksum
	if err := VerifyPageChecksum(pageNo, data); err != nil {
		return err
	}

	return nil
}

// CheckAllPages scans all pages in the database for corruption
// Returns a slice of all corruption errors found
func (cc *CorruptionChecker) CheckAllPages() []*CorruptionError {
	var errors []*CorruptionError

	pageCount := cc.pager.PageCount()

	for pageNo := uint32(0); pageNo < pageCount; pageNo++ {
		if err := cc.CheckPage(pageNo); err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

// CheckPageRange checks a range of pages for corruption
func (cc *CorruptionChecker) CheckPageRange(startPage, endPage uint32) []*CorruptionError {
	var errors []*CorruptionError

	pageCount := cc.pager.PageCount()
	if endPage > pageCount {
		endPage = pageCount
	}

	for pageNo := startPage; pageNo < endPage; pageNo++ {
		if err := cc.CheckPage(pageNo); err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}
