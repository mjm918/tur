//go:build windows

// pkg/turdb/lock_windows.go
package turdb

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const (
	// LockFileEx flags
	lockfileExclusiveLock   = 0x00000002
	lockfileFailImmediately = 0x00000001
)

// lockFile acquires an exclusive lock on the given file.
// Returns ErrDatabaseLocked if the file is already locked.
func lockFile(f *os.File) error {
	// Lock the first byte of the file exclusively
	var overlapped syscall.Overlapped
	r1, _, err := procLockFileEx.Call(
		uintptr(f.Fd()),
		uintptr(lockfileExclusiveLock|lockfileFailImmediately),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if r1 == 0 {
		// ERROR_LOCK_VIOLATION = 33
		if errno, ok := err.(syscall.Errno); ok && errno == 33 {
			return ErrDatabaseLocked
		}
		return err
	}
	return nil
}

// unlockFile releases the lock on the given file.
func unlockFile(f *os.File) error {
	var overlapped syscall.Overlapped
	r1, _, err := procUnlockFileEx.Call(
		uintptr(f.Fd()),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&overlapped)),
	)
	if r1 == 0 {
		return err
	}
	return nil
}
