package sstable

import (
	"fmt"
	"math"
)

// =================
//
//	Buffer Helper
//
// =================
func checkedBufferLength(
	length uint64,
	maxLength uint64,
	field string,
) (int, error) {
	if length > maxLength {
		return 0, fmt.Errorf(
			"%w: %s length %d exceeds maximum %d",
			ErrCorruptSSTable,
			field,
			length,
			maxLength,
		)
	}

	if length > uint64(math.MaxInt) {
		return 0, fmt.Errorf(
			"%w: %s length %d exceeds maximum allocation size",
			ErrCorruptSSTable,
			field,
			length,
		)
	}

	return int(length), nil
}
