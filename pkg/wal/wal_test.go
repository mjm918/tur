// pkg/wal/wal_test.go
package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALCreate(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")

	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Verify WAL file was created
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file was not created")
	}

	// Verify header is correct
	if w.PageSize() != 4096 {
		t.Errorf("expected page size 4096, got %d", w.PageSize())
	}
}

func TestWALHeader(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")

	// Create WAL
	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	w.Close()

	// Reopen and verify header persisted
	w2, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	if w2.PageSize() != 4096 {
		t.Errorf("expected page size 4096 after reopen, got %d", w2.PageSize())
	}
}

func TestWALHeaderFormat(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")

	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	w.Close()

	// Read raw header and verify magic number
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("failed to read WAL file: %v", err)
	}

	if len(data) < HeaderSize {
		t.Fatalf("WAL file too small: %d bytes", len(data))
	}

	// Verify magic number (first 4 bytes)
	// Using little-endian magic: 0x377f0682
	magic := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	if magic != MagicNumber {
		t.Errorf("expected magic 0x%x, got 0x%x", MagicNumber, magic)
	}
}

func TestWALWriteFrame(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")

	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Create a page of data
	pageData := make([]byte, 4096)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	// Write frame to WAL
	err = w.WriteFrame(1, pageData, false) // Page 1, not a commit
	if err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	// Verify frame count
	if w.FrameCount() != 1 {
		t.Errorf("expected 1 frame, got %d", w.FrameCount())
	}
}

func TestWALWriteMultipleFrames(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")

	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Write several frames
	for i := uint32(1); i <= 5; i++ {
		pageData := make([]byte, 4096)
		pageData[0] = byte(i) // Mark each page differently

		isCommit := i == 5 // Last frame is commit
		err = w.WriteFrame(i, pageData, isCommit)
		if err != nil {
			t.Fatalf("WriteFrame %d failed: %v", i, err)
		}
	}

	if w.FrameCount() != 5 {
		t.Errorf("expected 5 frames, got %d", w.FrameCount())
	}
}

func TestWALWriteFrameCommit(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")

	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	pageData := make([]byte, 4096)

	// Write a commit frame
	err = w.WriteFrame(1, pageData, true)
	if err != nil {
		t.Fatalf("WriteFrame (commit) failed: %v", err)
	}

	// Verify the file size includes header + frame header + page data
	info, _ := os.Stat(walPath)
	expectedSize := int64(HeaderSize + FrameHeaderSize + 4096)
	if info.Size() != expectedSize {
		t.Errorf("expected WAL size %d, got %d", expectedSize, info.Size())
	}
}

func TestWALReadFrame(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")

	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	// Create a page of data with known content
	pageData := make([]byte, 4096)
	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	// Write frame
	err = w.WriteFrame(1, pageData, false)
	if err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}
	w.Close()

	// Reopen and read frame
	w2, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	// Read frame 1
	frame, err := w2.ReadFrame(1)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if frame.PageNo != 1 {
		t.Errorf("expected page number 1, got %d", frame.PageNo)
	}

	// Verify data matches
	for i := 0; i < 4096; i++ {
		expected := byte(i % 256)
		if frame.Data[i] != expected {
			t.Errorf("data mismatch at offset %d: expected %d, got %d", i, expected, frame.Data[i])
			break
		}
	}
}

func TestWALFindPage(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")

	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Write frames for different pages
	for i := uint32(1); i <= 5; i++ {
		pageData := make([]byte, 4096)
		pageData[0] = byte(i)
		w.WriteFrame(i, pageData, false)
	}

	// Write updated frame for page 2
	pageData := make([]byte, 4096)
	pageData[0] = 99 // New value
	w.WriteFrame(2, pageData, true)

	// Find latest frame for page 2
	frameIdx, err := w.FindPage(2)
	if err != nil {
		t.Fatalf("FindPage failed: %v", err)
	}

	// Should return the second frame for page 2 (index 6, which is the 6th frame)
	if frameIdx != 6 {
		t.Errorf("expected frame index 6, got %d", frameIdx)
	}

	// Read the frame and verify it has the updated value
	frame, err := w.ReadFrame(frameIdx)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if frame.Data[0] != 99 {
		t.Errorf("expected updated value 99, got %d", frame.Data[0])
	}
}

func TestWALIterateFrames(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")

	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Write 3 frames
	for i := uint32(1); i <= 3; i++ {
		pageData := make([]byte, 4096)
		pageData[0] = byte(i)
		w.WriteFrame(i, pageData, i == 3)
	}

	// Iterate all frames
	frames := make([]*Frame, 0)
	err = w.ForEachFrame(func(f *Frame) error {
		frames = append(frames, f)
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachFrame failed: %v", err)
	}

	if len(frames) != 3 {
		t.Errorf("expected 3 frames, got %d", len(frames))
	}

	// Verify frame data
	for i, f := range frames {
		expectedPageNo := uint32(i + 1)
		if f.PageNo != expectedPageNo {
			t.Errorf("frame %d: expected page %d, got %d", i, expectedPageNo, f.PageNo)
		}
	}
}

func TestWALCheckpoint(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")
	dbPath := filepath.Join(dir, "test.db")

	// Create a mock database file with 3 pages
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("failed to create db file: %v", err)
	}
	// Initialize database with 3 blank pages
	blankPage := make([]byte, 4096)
	for i := 0; i < 3; i++ {
		dbFile.Write(blankPage)
	}
	dbFile.Close()

	// Create WAL and write frames
	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	// Write modified pages to WAL
	for i := uint32(1); i <= 3; i++ {
		pageData := make([]byte, 4096)
		pageData[0] = byte(i * 10) // Mark each page with distinct value
		w.WriteFrame(i, pageData, i == 3)
	}

	// Checkpoint: transfer WAL frames to database
	checkpointed, err := w.Checkpoint(dbPath)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != 3 {
		t.Errorf("expected 3 frames checkpointed, got %d", checkpointed)
	}

	// Verify WAL is reset
	if w.FrameCount() != 0 {
		t.Errorf("expected 0 frames after checkpoint, got %d", w.FrameCount())
	}

	w.Close()

	// Verify database file has the updated content
	dbData, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read db file: %v", err)
	}

	// Check each page was updated
	for i := 0; i < 3; i++ {
		offset := i * 4096
		expected := byte((i + 1) * 10)
		if dbData[offset] != expected {
			t.Errorf("page %d: expected first byte %d, got %d", i+1, expected, dbData[offset])
		}
	}
}

func TestWALCheckpointPartial(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")
	dbPath := filepath.Join(dir, "test.db")

	// Create database file
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("failed to create db file: %v", err)
	}
	blankPage := make([]byte, 4096)
	for i := 0; i < 5; i++ {
		dbFile.Write(blankPage)
	}
	dbFile.Close()

	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	// Write 5 frames, updating page 2 twice
	w.WriteFrame(1, makePageData(1, 10), false)
	w.WriteFrame(2, makePageData(2, 20), false)
	w.WriteFrame(3, makePageData(3, 30), false)
	w.WriteFrame(2, makePageData(2, 25), false) // Update page 2 again
	w.WriteFrame(4, makePageData(4, 40), true)  // Commit

	// Checkpoint
	checkpointed, err := w.Checkpoint(dbPath)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if checkpointed != 5 {
		t.Errorf("expected 5 frames checkpointed, got %d", checkpointed)
	}

	w.Close()

	// Verify page 2 has the LATEST value (25, not 20)
	dbData, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read db file: %v", err)
	}

	// Page 2 offset (0-indexed, so page 2 is at offset 4096)
	page2Value := dbData[4096]
	if page2Value != 25 {
		t.Errorf("page 2: expected value 25 (latest), got %d", page2Value)
	}
}

// makePageData creates a page with a marker value at the first byte
func makePageData(pageNo uint32, value byte) []byte {
	data := make([]byte, 4096)
	data[0] = value
	return data
}

func TestWALRecoveryAfterCrash(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")
	dbPath := filepath.Join(dir, "test.db")

	// Create database file
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("failed to create db file: %v", err)
	}
	blankPage := make([]byte, 4096)
	for i := 0; i < 5; i++ {
		dbFile.Write(blankPage)
	}
	dbFile.Close()

	// Create WAL and write frames (simulating a transaction)
	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	// Write 3 frames with commit
	w.WriteFrame(1, makePageData(1, 10), false)
	w.WriteFrame(2, makePageData(2, 20), false)
	w.WriteFrame(3, makePageData(3, 30), true) // Commit

	// Simulate crash by NOT doing checkpoint and just closing
	w.Close()

	// "Recover" by reopening WAL and applying to database
	w2, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}

	// WAL should have 3 frames from before crash
	if w2.FrameCount() != 3 {
		t.Errorf("expected 3 frames in WAL after recovery, got %d", w2.FrameCount())
	}

	// Recover by applying WAL to database
	recovered, err := w2.Recover(dbPath)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if recovered != 3 {
		t.Errorf("expected 3 frames recovered, got %d", recovered)
	}

	w2.Close()

	// Verify database has the recovered data
	dbData, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read db file: %v", err)
	}

	// Verify each page
	expectedValues := []byte{10, 20, 30}
	for i, expected := range expectedValues {
		offset := i * 4096
		if dbData[offset] != expected {
			t.Errorf("page %d: expected value %d, got %d", i+1, expected, dbData[offset])
		}
	}
}

func TestWALRecoveryNoCommit(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")
	dbPath := filepath.Join(dir, "test.db")

	// Create database file
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("failed to create db file: %v", err)
	}
	blankPage := make([]byte, 4096)
	for i := 0; i < 5; i++ {
		dbFile.Write(blankPage)
	}
	dbFile.Close()

	// Create WAL and write frames WITHOUT commit (simulating crash during transaction)
	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	// Write frames but NO commit
	w.WriteFrame(1, makePageData(1, 10), false)
	w.WriteFrame(2, makePageData(2, 20), false)
	// No commit frame - simulating crash during transaction

	w.Close()

	// Recover - should NOT apply uncommitted transaction
	w2, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}

	recovered, err := w2.Recover(dbPath)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// No frames should be recovered because there was no commit
	if recovered != 0 {
		t.Errorf("expected 0 frames recovered (no commit), got %d", recovered)
	}

	w2.Close()

	// Verify database is unchanged (all zeros)
	dbData, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read db file: %v", err)
	}

	if dbData[0] != 0 || dbData[4096] != 0 {
		t.Error("database should be unchanged - uncommitted transaction should not be applied")
	}
}

func TestWALRecoveryMultipleTransactions(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")
	dbPath := filepath.Join(dir, "test.db")

	// Create database file
	dbFile, err := os.Create(dbPath)
	if err != nil {
		t.Fatalf("failed to create db file: %v", err)
	}
	blankPage := make([]byte, 4096)
	for i := 0; i < 5; i++ {
		dbFile.Write(blankPage)
	}
	dbFile.Close()

	// Create WAL with multiple committed transactions
	w, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	// Transaction 1: pages 1-2
	w.WriteFrame(1, makePageData(1, 10), false)
	w.WriteFrame(2, makePageData(2, 20), true) // Commit

	// Transaction 2: pages 3-4, update page 1
	w.WriteFrame(3, makePageData(3, 30), false)
	w.WriteFrame(1, makePageData(1, 15), false) // Update page 1
	w.WriteFrame(4, makePageData(4, 40), true)  // Commit

	// Uncommitted transaction (should not be recovered)
	w.WriteFrame(5, makePageData(5, 50), false)

	w.Close()

	// Recover
	w2, err := Open(walPath, Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}

	recovered, err := w2.Recover(dbPath)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Should recover 5 frames (up to last commit, excluding the uncommitted frame)
	if recovered != 5 {
		t.Errorf("expected 5 frames recovered, got %d", recovered)
	}

	w2.Close()

	// Verify database
	dbData, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read db file: %v", err)
	}

	// Page 1 should have value 15 (latest committed)
	if dbData[0] != 15 {
		t.Errorf("page 1: expected 15, got %d", dbData[0])
	}

	// Page 5 should be unchanged (uncommitted)
	if dbData[4*4096] != 0 {
		t.Errorf("page 5: expected 0 (uncommitted), got %d", dbData[4*4096])
	}
}
