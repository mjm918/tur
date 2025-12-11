//go:build unix || darwin || linux || freebsd || openbsd || netbsd

// pkg/pager/mmap_unix.go
package pager

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// OpenMmapFile opens or creates a memory-mapped file
// If initialSize > 0 and file doesn't exist or is smaller, it will be extended
func OpenMmapFile(path string, initialSize int64) (*MmapFile, error) {
	// Open or create file
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Get current size
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	size := stat.Size()
	if initialSize > size {
		// Extend file to initial size
		if err := f.Truncate(initialSize); err != nil {
			f.Close()
			return nil, err
		}
		size = initialSize
	}

	if size == 0 {
		// Can't mmap empty file
		f.Close()
		return nil, errors.New("cannot mmap empty file")
	}

	// Memory map the file
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &MmapFile{
		file: f,
		data: data,
		size: size,
	}, nil
}

// Sync flushes changes to disk
func (m *MmapFile) Sync() error {
	return unix.Msync(m.data, unix.MS_SYNC)
}

// Grow extends the file and remaps it
func (m *MmapFile) Grow(newSize int64) error {
	if newSize <= m.size {
		return nil
	}

	// CRITICAL: Sync dirty pages to disk before unmapping.
	// With MAP_SHARED, writes go to the kernel page cache but may not be
	// flushed to disk yet. We must sync to ensure data is persisted before
	// we unmap and remap the region.
	if err := unix.Msync(m.data, unix.MS_SYNC); err != nil {
		return err
	}

	// Unmap current mapping
	if err := syscall.Munmap(m.data); err != nil {
		return err
	}

	// Get the file handle
	f := m.file.(*os.File)

	// Extend file
	if err := f.Truncate(newSize); err != nil {
		return err
	}

	// Remap with new size
	data, err := syscall.Mmap(int(f.Fd()), 0, int(newSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	m.data = data
	m.size = newSize
	return nil
}

// Close unmaps and closes the file
func (m *MmapFile) Close() error {
	var firstErr error

	if m.data != nil {
		if err := syscall.Munmap(m.data); err != nil && firstErr == nil {
			firstErr = err
		}
		m.data = nil
	}

	if m.file != nil {
		f := m.file.(*os.File)
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		m.file = nil
	}

	return firstErr
}
