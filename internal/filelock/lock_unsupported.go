//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package filelock

import (
	"errors"
	"fmt"
)

var ErrLocked = errors.New("database directory is already locked")

type Lock struct{}

func Acquire(string) (*Lock, error) {
	return nil, fmt.Errorf("database directory locking is unsupported on this platform")
}

func (l *Lock) Close() error {
	return nil
}
