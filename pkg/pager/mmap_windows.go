//go:build windows

// pkg/pager/mmap_windows.go
package pager

import (
	"errors"
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/windows"
)

// mmapHandle stores Windows-specific handles for memory mapping
type mmapHandle struct {
	file       *os.File
	mapHandle  windows.Handle
	mappedSize int64
}

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

	// Create file mapping
	mapHandle, err := windows.CreateFileMapping(
		windows.Handle(f.Fd()),
		nil,
		windows.PAGE_READWRITE,
		uint32(size>>32),
		uint32(size&0xFFFFFFFF),
		nil,
	)
	if err != nil {
		f.Close()
		return nil, err
	}

	// Map view of file
	addr, err := windows.MapViewOfFile(
		mapHandle,
		windows.FILE_MAP_READ|windows.FILE_MAP_WRITE,
		0, 0,
		uintptr(size),
	)
	if err != nil {
		windows.CloseHandle(mapHandle)
		f.Close()
		return nil, err
	}

	// Create byte slice from mapped memory
	var data []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	header.Data = addr
	header.Len = int(size)
	header.Cap = int(size)

	handle := &mmapHandle{
		file:       f,
		mapHandle:  mapHandle,
		mappedSize: size,
	}

	return &MmapFile{
		file: handle,
		data: data,
		size: size,
	}, nil
}

// Sync flushes changes to disk
func (m *MmapFile) Sync() error {
	if len(m.data) == 0 {
		return nil
	}
	return windows.FlushViewOfFile(uintptr(unsafe.Pointer(&m.data[0])), uintptr(len(m.data)))
}

// Grow extends the file and remaps it
func (m *MmapFile) Grow(newSize int64) error {
	if newSize <= m.size {
		return nil
	}

	handle := m.file.(*mmapHandle)

	// Flush current mapping
	if len(m.data) > 0 {
		if err := windows.FlushViewOfFile(uintptr(unsafe.Pointer(&m.data[0])), uintptr(len(m.data))); err != nil {
			return err
		}
	}

	// Unmap current view
	if len(m.data) > 0 {
		if err := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&m.data[0]))); err != nil {
			return err
		}
	}

	// Close current mapping handle
	if err := windows.CloseHandle(handle.mapHandle); err != nil {
		return err
	}

	// Extend file
	if err := handle.file.Truncate(newSize); err != nil {
		return err
	}

	// Create new file mapping
	mapHandle, err := windows.CreateFileMapping(
		windows.Handle(handle.file.Fd()),
		nil,
		windows.PAGE_READWRITE,
		uint32(newSize>>32),
		uint32(newSize&0xFFFFFFFF),
		nil,
	)
	if err != nil {
		return err
	}

	// Map view of file
	addr, err := windows.MapViewOfFile(
		mapHandle,
		windows.FILE_MAP_READ|windows.FILE_MAP_WRITE,
		0, 0,
		uintptr(newSize),
	)
	if err != nil {
		windows.CloseHandle(mapHandle)
		return err
	}

	// Update byte slice
	var data []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	header.Data = addr
	header.Len = int(newSize)
	header.Cap = int(newSize)

	handle.mapHandle = mapHandle
	handle.mappedSize = newSize
	m.data = data
	m.size = newSize

	return nil
}

// Close unmaps and closes the file
func (m *MmapFile) Close() error {
	var firstErr error

	handle, ok := m.file.(*mmapHandle)
	if !ok || handle == nil {
		return nil
	}

	// Unmap view
	if len(m.data) > 0 {
		if err := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&m.data[0]))); err != nil && firstErr == nil {
			firstErr = err
		}
		m.data = nil
	}

	// Close mapping handle
	if handle.mapHandle != 0 {
		if err := windows.CloseHandle(handle.mapHandle); err != nil && firstErr == nil {
			firstErr = err
		}
		handle.mapHandle = 0
	}

	// Close file
	if handle.file != nil {
		if err := handle.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		handle.file = nil
	}

	m.file = nil
	return firstErr
}
