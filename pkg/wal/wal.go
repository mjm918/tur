// pkg/wal/wal.go
// Package wal implements a Write-Ahead Log for durability and crash recovery.
//
// # WAL FILE FORMAT
//
// A WAL file consists of a header followed by zero or more "frames".
// Each frame records the revised content of a single page from the
// database file. All changes to the database are recorded by writing
// frames into the WAL. Transactions commit when a frame is written that
// contains a commit marker.
//
// The WAL header is 32 bytes in size and consists of the following
// little-endian values:
//
//	0-3:   Magic number (0x377f0682)
//	4-7:   File format version (3007000)
//	8-11:  Database page size
//	12-15: Checkpoint sequence number
//	16-19: Salt-1 (random, incremented with each checkpoint)
//	20-23: Salt-2 (random, changed with each checkpoint)
//	24-27: Checksum-1 (first part of header checksum)
//	28-31: Checksum-2 (second part of header checksum)
//
// Each frame consists of a 24-byte frame-header followed by page-size bytes
// of page data:
//
//	0-3:   Page number
//	4-7:   For commit records, the size of the database in pages after commit.
//	       For all other records, zero.
//	8-11:  Salt-1 (copied from header)
//	12-15: Salt-2 (copied from header)
//	16-19: Checksum-1
//	20-23: Checksum-2
package wal

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"sync"
)

const (
	// HeaderSize is the size of the WAL header in bytes
	HeaderSize = 32

	// FrameHeaderSize is the size of each frame header in bytes
	FrameHeaderSize = 24

	// MagicNumber identifies a WAL file (little-endian variant)
	MagicNumber = 0x377f0682

	// Version is the WAL file format version
	Version = 3007000
)

var (
	ErrInvalidMagic   = errors.New("invalid WAL magic number")
	ErrInvalidVersion = errors.New("invalid WAL version")
	ErrChecksumFailed = errors.New("WAL checksum verification failed")
	ErrNoFrames       = errors.New("no valid frames in WAL")
	ErrFrameNotFound  = errors.New("frame not found")
	ErrPageNotFound   = errors.New("page not found in WAL")
)

// Frame represents a single WAL frame containing a page
type Frame struct {
	Index    uint32 // 1-based frame index
	PageNo   uint32 // Database page number
	DbSize   uint32 // Database size in pages (non-zero for commit frames)
	Data     []byte // Page data
	IsCommit bool   // True if this is a commit frame
}

// Options configures the WAL
type Options struct {
	PageSize int // Database page size
}

// WAL represents a Write-Ahead Log
type WAL struct {
	mu       sync.RWMutex
	file     *os.File
	pageSize int
	salt1    uint32
	salt2    uint32
	ckptSeq  uint32 // Checkpoint sequence number

	// Running checksum for frame validation
	checksum1 uint32
	checksum2 uint32

	// Frame tracking
	frameCount uint32 // Number of valid frames
}

// Open opens or creates a WAL file
func Open(path string, opts Options) (*WAL, error) {
	pageSize := opts.PageSize
	if pageSize == 0 {
		pageSize = 4096
	}

	// Try to open existing file
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			// Create new WAL file
			return createWAL(path, pageSize)
		}
		return nil, err
	}

	// Read existing WAL
	w := &WAL{
		file:     file,
		pageSize: pageSize,
	}

	if err := w.readHeader(); err != nil {
		// Invalid or empty WAL, reinitialize
		file.Close()
		return createWAL(path, pageSize)
	}

	return w, nil
}

// createWAL creates a new WAL file
func createWAL(path string, pageSize int) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:     file,
		pageSize: pageSize,
		salt1:    rand.Uint32(),
		salt2:    rand.Uint32(),
		ckptSeq:  1,
	}

	if err := w.writeHeader(); err != nil {
		file.Close()
		return nil, err
	}

	return w, nil
}

// writeHeader writes the WAL header to the file
func (w *WAL) writeHeader() error {
	header := make([]byte, HeaderSize)

	// Magic number
	binary.LittleEndian.PutUint32(header[0:4], MagicNumber)
	// Version
	binary.LittleEndian.PutUint32(header[4:8], Version)
	// Page size
	binary.LittleEndian.PutUint32(header[8:12], uint32(w.pageSize))
	// Checkpoint sequence
	binary.LittleEndian.PutUint32(header[12:16], w.ckptSeq)
	// Salt values
	binary.LittleEndian.PutUint32(header[16:20], w.salt1)
	binary.LittleEndian.PutUint32(header[20:24], w.salt2)

	// Compute checksum of first 24 bytes
	w.checksum1, w.checksum2 = walChecksum(header[0:24], 0, 0)
	binary.LittleEndian.PutUint32(header[24:28], w.checksum1)
	binary.LittleEndian.PutUint32(header[28:32], w.checksum2)

	_, err := w.file.WriteAt(header, 0)
	if err != nil {
		return err
	}

	return w.file.Sync()
}

// readHeader reads and validates the WAL header
func (w *WAL) readHeader() error {
	header := make([]byte, HeaderSize)
	n, err := w.file.ReadAt(header, 0)
	if err != nil {
		return err
	}
	if n < HeaderSize {
		return ErrInvalidMagic
	}

	// Verify magic
	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != MagicNumber {
		return ErrInvalidMagic
	}

	// Verify version
	version := binary.LittleEndian.Uint32(header[4:8])
	if version != Version {
		return ErrInvalidVersion
	}

	// Read header values
	w.pageSize = int(binary.LittleEndian.Uint32(header[8:12]))
	w.ckptSeq = binary.LittleEndian.Uint32(header[12:16])
	w.salt1 = binary.LittleEndian.Uint32(header[16:20])
	w.salt2 = binary.LittleEndian.Uint32(header[20:24])

	// Verify checksum
	storedCksum1 := binary.LittleEndian.Uint32(header[24:28])
	storedCksum2 := binary.LittleEndian.Uint32(header[28:32])

	computedCksum1, computedCksum2 := walChecksum(header[0:24], 0, 0)
	if storedCksum1 != computedCksum1 || storedCksum2 != computedCksum2 {
		return ErrChecksumFailed
	}

	w.checksum1 = storedCksum1
	w.checksum2 = storedCksum2

	// Count valid frames by scanning the WAL
	w.frameCount = w.countValidFrames()

	return nil
}

// countValidFrames counts the number of valid frames in the WAL
func (w *WAL) countValidFrames() uint32 {
	info, err := w.file.Stat()
	if err != nil {
		return 0
	}

	fileSize := info.Size()
	frameSize := int64(FrameHeaderSize) + int64(w.pageSize)
	contentSize := fileSize - int64(HeaderSize)

	if contentSize <= 0 {
		return 0
	}

	// Maximum possible frames based on file size
	maxFrames := uint32(contentSize / frameSize)

	// Validate each frame by checking salt values
	validFrames := uint32(0)
	cksum1, cksum2 := w.checksum1, w.checksum2

	for i := uint32(0); i < maxFrames; i++ {
		frameOffset := int64(HeaderSize) + int64(i)*frameSize

		// Read frame header
		frameHeader := make([]byte, FrameHeaderSize)
		_, err := w.file.ReadAt(frameHeader, frameOffset)
		if err != nil {
			break
		}

		// Check salt values
		frameSalt1 := binary.LittleEndian.Uint32(frameHeader[8:12])
		frameSalt2 := binary.LittleEndian.Uint32(frameHeader[12:16])
		if frameSalt1 != w.salt1 || frameSalt2 != w.salt2 {
			break
		}

		// Read page data for checksum verification
		pageData := make([]byte, w.pageSize)
		_, err = w.file.ReadAt(pageData, frameOffset+FrameHeaderSize)
		if err != nil {
			break
		}

		// Verify checksum
		checksumData := make([]byte, 8+w.pageSize)
		copy(checksumData[0:8], frameHeader[0:8])
		copy(checksumData[8:], pageData)

		cksum1, cksum2 = walChecksum(checksumData, cksum1, cksum2)

		storedCksum1 := binary.LittleEndian.Uint32(frameHeader[16:20])
		storedCksum2 := binary.LittleEndian.Uint32(frameHeader[20:24])

		if cksum1 != storedCksum1 || cksum2 != storedCksum2 {
			break
		}

		validFrames++
	}

	// Update running checksum to continue from last valid frame
	w.checksum1, w.checksum2 = cksum1, cksum2

	return validFrames
}

// walChecksum computes the WAL checksum using the SQLite algorithm
// The algorithm uses fibonacci weights in reverse order
func walChecksum(data []byte, s0, s1 uint32) (uint32, uint32) {
	// Pad to 4-byte boundary if needed
	for len(data)%4 != 0 {
		data = append(data, 0)
	}

	for i := 0; i < len(data); i += 8 {
		var x0, x1 uint32
		x0 = binary.LittleEndian.Uint32(data[i : i+4])
		if i+4 < len(data) {
			x1 = binary.LittleEndian.Uint32(data[i+4 : i+8])
		}
		s0 += x0 + s1
		s1 += x1 + s0
	}

	return s0, s1
}

// PageSize returns the database page size
func (w *WAL) PageSize() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.pageSize
}

// FrameCount returns the number of valid frames in the WAL
func (w *WAL) FrameCount() uint32 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.frameCount
}

// WriteFrame writes a page to the WAL
// If isCommit is true, this is a commit frame and dbSize should be set
func (w *WAL) WriteFrame(pageNo uint32, data []byte, isCommit bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(data) != w.pageSize {
		return errors.New("page data size mismatch")
	}

	// Calculate frame offset
	frameOffset := int64(HeaderSize) + int64(w.frameCount)*(int64(FrameHeaderSize)+int64(w.pageSize))

	// Build frame header
	frameHeader := make([]byte, FrameHeaderSize)
	binary.LittleEndian.PutUint32(frameHeader[0:4], pageNo)

	// For commit frames, set the database size (in pages)
	// For now, we'll use a placeholder - actual value comes from pager integration
	if isCommit {
		binary.LittleEndian.PutUint32(frameHeader[4:8], pageNo) // placeholder: dbSize
	} else {
		binary.LittleEndian.PutUint32(frameHeader[4:8], 0)
	}

	// Copy salt values
	binary.LittleEndian.PutUint32(frameHeader[8:12], w.salt1)
	binary.LittleEndian.PutUint32(frameHeader[12:16], w.salt2)

	// Compute checksum over frame header (first 8 bytes) and page data
	// The checksum continues from the previous checksum
	checksumData := make([]byte, 8+len(data))
	copy(checksumData[0:8], frameHeader[0:8])
	copy(checksumData[8:], data)

	w.checksum1, w.checksum2 = walChecksum(checksumData, w.checksum1, w.checksum2)

	binary.LittleEndian.PutUint32(frameHeader[16:20], w.checksum1)
	binary.LittleEndian.PutUint32(frameHeader[20:24], w.checksum2)

	// Write frame header
	_, err := w.file.WriteAt(frameHeader, frameOffset)
	if err != nil {
		return err
	}

	// Write page data
	_, err = w.file.WriteAt(data, frameOffset+FrameHeaderSize)
	if err != nil {
		return err
	}

	w.frameCount++

	// Sync on commit
	if isCommit {
		return w.file.Sync()
	}

	return nil
}

// ReadFrame reads a frame by its 1-based index
func (w *WAL) ReadFrame(frameIndex uint32) (*Frame, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if frameIndex < 1 || frameIndex > w.frameCount {
		return nil, ErrFrameNotFound
	}

	// Calculate frame offset (frameIndex is 1-based)
	frameOffset := int64(HeaderSize) + int64(frameIndex-1)*(int64(FrameHeaderSize)+int64(w.pageSize))

	// Read frame header
	frameHeader := make([]byte, FrameHeaderSize)
	_, err := w.file.ReadAt(frameHeader, frameOffset)
	if err != nil {
		return nil, err
	}

	// Verify salt values match
	frameSalt1 := binary.LittleEndian.Uint32(frameHeader[8:12])
	frameSalt2 := binary.LittleEndian.Uint32(frameHeader[12:16])
	if frameSalt1 != w.salt1 || frameSalt2 != w.salt2 {
		return nil, ErrChecksumFailed
	}

	// Read page data
	pageData := make([]byte, w.pageSize)
	_, err = w.file.ReadAt(pageData, frameOffset+FrameHeaderSize)
	if err != nil {
		return nil, err
	}

	pageNo := binary.LittleEndian.Uint32(frameHeader[0:4])
	dbSize := binary.LittleEndian.Uint32(frameHeader[4:8])

	return &Frame{
		Index:    frameIndex,
		PageNo:   pageNo,
		DbSize:   dbSize,
		Data:     pageData,
		IsCommit: dbSize > 0,
	}, nil
}

// FindPage finds the latest frame for a given page number
// Returns the 1-based frame index, or error if not found
func (w *WAL) FindPage(pageNo uint32) (uint32, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Scan frames from last to first to find the most recent version
	for i := w.frameCount; i >= 1; i-- {
		frameOffset := int64(HeaderSize) + int64(i-1)*(int64(FrameHeaderSize)+int64(w.pageSize))

		// Read just the page number from frame header
		pageNoBuf := make([]byte, 4)
		_, err := w.file.ReadAt(pageNoBuf, frameOffset)
		if err != nil {
			continue
		}

		framePageNo := binary.LittleEndian.Uint32(pageNoBuf)
		if framePageNo == pageNo {
			return i, nil
		}
	}

	return 0, ErrPageNotFound
}

// ForEachFrame iterates over all valid frames in the WAL
func (w *WAL) ForEachFrame(fn func(*Frame) error) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	for i := uint32(1); i <= w.frameCount; i++ {
		frameOffset := int64(HeaderSize) + int64(i-1)*(int64(FrameHeaderSize)+int64(w.pageSize))

		// Read frame header
		frameHeader := make([]byte, FrameHeaderSize)
		_, err := w.file.ReadAt(frameHeader, frameOffset)
		if err != nil {
			return err
		}

		// Read page data
		pageData := make([]byte, w.pageSize)
		_, err = w.file.ReadAt(pageData, frameOffset+FrameHeaderSize)
		if err != nil {
			return err
		}

		pageNo := binary.LittleEndian.Uint32(frameHeader[0:4])
		dbSize := binary.LittleEndian.Uint32(frameHeader[4:8])

		frame := &Frame{
			Index:    i,
			PageNo:   pageNo,
			DbSize:   dbSize,
			Data:     pageData,
			IsCommit: dbSize > 0,
		}

		if err := fn(frame); err != nil {
			return err
		}
	}

	return nil
}

// Checkpoint transfers WAL frames to the database file
// Returns the number of frames checkpointed
func (w *WAL) Checkpoint(dbPath string) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.frameCount == 0 {
		return 0, nil
	}

	// Open database file for writing
	dbFile, err := os.OpenFile(dbPath, os.O_RDWR, 0644)
	if err != nil {
		return 0, err
	}
	defer dbFile.Close()

	// Build a map of page number -> latest frame data
	// We need to apply frames in order but only keep the latest version of each page
	latestPages := make(map[uint32][]byte)
	frameSize := int64(FrameHeaderSize) + int64(w.pageSize)

	for i := uint32(1); i <= w.frameCount; i++ {
		frameOffset := int64(HeaderSize) + int64(i-1)*frameSize

		// Read frame header
		frameHeader := make([]byte, FrameHeaderSize)
		_, err := w.file.ReadAt(frameHeader, frameOffset)
		if err != nil {
			return 0, err
		}

		pageNo := binary.LittleEndian.Uint32(frameHeader[0:4])

		// Read page data
		pageData := make([]byte, w.pageSize)
		_, err = w.file.ReadAt(pageData, frameOffset+FrameHeaderSize)
		if err != nil {
			return 0, err
		}

		// Store latest version (overwrites previous if exists)
		latestPages[pageNo] = pageData
	}

	// Write latest page versions to database
	for pageNo, pageData := range latestPages {
		// Page numbers are 1-based, file offset is 0-based
		offset := int64(pageNo-1) * int64(w.pageSize)
		_, err := dbFile.WriteAt(pageData, offset)
		if err != nil {
			return 0, err
		}
	}

	// Sync the database file
	if err := dbFile.Sync(); err != nil {
		return 0, err
	}

	// Reset WAL for reuse
	checkpointedFrames := int(w.frameCount)
	if err := w.reset(); err != nil {
		return checkpointedFrames, err
	}

	return checkpointedFrames, nil
}

// reset resets the WAL for reuse after checkpoint
func (w *WAL) reset() error {
	// Increment checkpoint sequence and change salt
	w.ckptSeq++
	w.salt1++
	w.salt2 = rand.Uint32()

	// Reset frame count and checksum
	w.frameCount = 0

	// Write new header with updated values
	if err := w.writeHeaderLocked(); err != nil {
		return err
	}

	// Truncate WAL file to just the header
	if err := w.file.Truncate(HeaderSize); err != nil {
		return err
	}

	return w.file.Sync()
}

// writeHeaderLocked writes header without acquiring lock (for internal use)
func (w *WAL) writeHeaderLocked() error {
	header := make([]byte, HeaderSize)

	binary.LittleEndian.PutUint32(header[0:4], MagicNumber)
	binary.LittleEndian.PutUint32(header[4:8], Version)
	binary.LittleEndian.PutUint32(header[8:12], uint32(w.pageSize))
	binary.LittleEndian.PutUint32(header[12:16], w.ckptSeq)
	binary.LittleEndian.PutUint32(header[16:20], w.salt1)
	binary.LittleEndian.PutUint32(header[20:24], w.salt2)

	w.checksum1, w.checksum2 = walChecksum(header[0:24], 0, 0)
	binary.LittleEndian.PutUint32(header[24:28], w.checksum1)
	binary.LittleEndian.PutUint32(header[28:32], w.checksum2)

	_, err := w.file.WriteAt(header, 0)
	return err
}

// Recover applies committed transactions from WAL to database after a crash
// Only frames up to the last commit are applied; uncommitted frames are ignored
// Returns the number of frames applied
func (w *WAL) Recover(dbPath string) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.frameCount == 0 {
		return 0, nil
	}

	// Find the last commit frame
	lastCommitFrame := w.findLastCommitFrame()
	if lastCommitFrame == 0 {
		// No commit frame found - nothing to recover
		return 0, nil
	}

	// Open database file for writing
	dbFile, err := os.OpenFile(dbPath, os.O_RDWR, 0644)
	if err != nil {
		return 0, err
	}
	defer dbFile.Close()

	// Build map of page number -> latest frame data (up to last commit)
	latestPages := make(map[uint32][]byte)
	frameSize := int64(FrameHeaderSize) + int64(w.pageSize)

	for i := uint32(1); i <= lastCommitFrame; i++ {
		frameOffset := int64(HeaderSize) + int64(i-1)*frameSize

		// Read frame header
		frameHeader := make([]byte, FrameHeaderSize)
		_, err := w.file.ReadAt(frameHeader, frameOffset)
		if err != nil {
			return 0, err
		}

		pageNo := binary.LittleEndian.Uint32(frameHeader[0:4])

		// Read page data
		pageData := make([]byte, w.pageSize)
		_, err = w.file.ReadAt(pageData, frameOffset+FrameHeaderSize)
		if err != nil {
			return 0, err
		}

		// Store latest version
		latestPages[pageNo] = pageData
	}

	// Write latest page versions to database
	for pageNo, pageData := range latestPages {
		offset := int64(pageNo-1) * int64(w.pageSize)
		_, err := dbFile.WriteAt(pageData, offset)
		if err != nil {
			return 0, err
		}
	}

	// Sync database
	if err := dbFile.Sync(); err != nil {
		return 0, err
	}

	// Reset WAL
	recoveredFrames := int(lastCommitFrame)
	if err := w.reset(); err != nil {
		return recoveredFrames, err
	}

	return recoveredFrames, nil
}

// findLastCommitFrame finds the frame index of the last commit frame
// Returns 0 if no commit frame is found
func (w *WAL) findLastCommitFrame() uint32 {
	frameSize := int64(FrameHeaderSize) + int64(w.pageSize)
	lastCommit := uint32(0)

	for i := uint32(1); i <= w.frameCount; i++ {
		frameOffset := int64(HeaderSize) + int64(i-1)*frameSize

		// Read frame header - just need the dbSize field
		frameHeader := make([]byte, 8)
		_, err := w.file.ReadAt(frameHeader, frameOffset)
		if err != nil {
			break
		}

		dbSize := binary.LittleEndian.Uint32(frameHeader[4:8])
		if dbSize > 0 {
			// This is a commit frame
			lastCommit = i
		}
	}

	return lastCommit
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			w.file.Close()
			return err
		}
		return w.file.Close()
	}
	return nil
}
