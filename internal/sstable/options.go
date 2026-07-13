package sstable

import (
	"fmt"

	"github.com/aaw3/hyphadb/internal/compression"
)

type WriteOptions struct {
	BlockSize      int
	Compression    compression.Type
	MinSavingsRate float64
}

func DefaultWriteOptions() WriteOptions {
	return WriteOptions{
		BlockSize:      DefaultBlockSize,
		Compression:    compression.LZ4,
		MinSavingsRate: compression.DefaultMinSavingsRate,
	}
}

func normalizeWriteOptions(opts WriteOptions) (WriteOptions, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}

	if opts.MinSavingsRate < 0 || opts.MinSavingsRate >= 1 {
		return WriteOptions{}, fmt.Errorf(
			"invalid minimum compression savings rate: %f, must be in [0, 1)",
			opts.MinSavingsRate,
		)
	}

	switch opts.Compression {
	case compression.None, compression.LZ4:
		// valid compression types
	default:
		return WriteOptions{}, fmt.Errorf(
			"invalid compression type: %d",
			opts.Compression,
		)
	}

	return opts, nil
}
