// pkg/pager/storage.go
package pager

// Storage defines the interface for page-level storage backends.
// This abstraction allows the pager to work with different storage
// implementations (file-based via mmap, in-memory, etc.).
type Storage interface {
	// Size returns the current size of the storage in bytes.
	Size() int64

	// Slice returns a slice of the storage data at the given offset and length.
	// Returns nil if the requested range is out of bounds.
	Slice(offset, length int) []byte

	// Sync flushes any pending writes to the underlying storage.
	// For in-memory storage, this is a no-op.
	Sync() error

	// Grow extends the storage to the specified size.
	// If newSize is less than or equal to current size, this is a no-op.
	Grow(newSize int64) error

	// Close releases any resources associated with the storage.
	Close() error
}

// MemoryStorage implements Storage using an in-memory byte slice.
// This is used for the :memory: database mode where no disk I/O is performed.
type MemoryStorage struct {
	data []byte
	size int64
}

// NewMemoryStorage creates a new in-memory storage with the specified initial size.
func NewMemoryStorage(initialSize int64) (*MemoryStorage, error) {
	if initialSize <= 0 {
		initialSize = 4096 // Default to one page
	}

	return &MemoryStorage{
		data: make([]byte, initialSize),
		size: initialSize,
	}, nil
}

// Size returns the current size of the storage in bytes.
func (m *MemoryStorage) Size() int64 {
	return m.size
}

// Slice returns a slice of the storage data at the given offset and length.
// Returns nil if the requested range is out of bounds.
func (m *MemoryStorage) Slice(offset, length int) []byte {
	if offset < 0 || length < 0 || offset+length > len(m.data) {
		return nil
	}
	return m.data[offset : offset+length]
}

// Sync is a no-op for in-memory storage since there's no disk to flush to.
func (m *MemoryStorage) Sync() error {
	return nil
}

// Grow extends the storage to the specified size.
// If newSize is less than or equal to current size, this is a no-op.
// Existing data is preserved.
func (m *MemoryStorage) Grow(newSize int64) error {
	if newSize <= m.size {
		return nil
	}

	// Allocate new buffer and copy existing data
	newData := make([]byte, newSize)
	copy(newData, m.data)

	m.data = newData
	m.size = newSize
	return nil
}

// Close releases the memory storage.
// After Close, the storage should not be used.
func (m *MemoryStorage) Close() error {
	m.data = nil
	m.size = 0
	return nil
}
