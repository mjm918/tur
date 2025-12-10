//go:build !windows

// pkg/turdb/lock_unix.go
package turdb

import (
	"os"

	"golang.org/x/sys/unix"
)

// lockFile acquires an exclusive lock on the given file.
// Returns ErrDatabaseLocked if the file is already locked.
func lockFile(f *os.File) error {
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		if err == unix.EWOULDBLOCK {
			return ErrDatabaseLocked
		}
		return err
	}
	return nil
}

// unlockFile releases the lock on the given file.
func unlockFile(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
