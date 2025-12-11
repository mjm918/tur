// pkg/pager/mmap.go
package pager

// MmapFile provides memory-mapped file access
// Platform-specific implementations are in mmap_unix.go and mmap_windows.go
type MmapFile struct {
	file interface{} // *os.File on Unix, windows.Handle on Windows
	data []byte
	size int64
}

// Size returns the current file size
func (m *MmapFile) Size() int64 {
	return m.size
}

// Slice returns a slice of the mapped memory at the given offset and length
func (m *MmapFile) Slice(offset, length int) []byte {
	if offset+length > len(m.data) {
		return nil
	}
	return m.data[offset : offset+length]
}
