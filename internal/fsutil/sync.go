package fsutil

import (
	"os"
	"path/filepath"
)

// SyncParent persists directory-entry changes for path.
func SyncParent(path string) error {
	return SyncDir(filepath.Dir(path))
}

// SyncDir persists changes to entries in dir.
func SyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()

	return dir.Sync()
}
