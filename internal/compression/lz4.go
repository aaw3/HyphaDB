package compression

import (
	"fmt"

	"github.com/pierrec/lz4/v4"
)

func compressLZ4(
	src []byte,
	minSavingsRate float64,
) ([]byte, Type, error) {
	if len(src) == 0 {
		return src, None, nil
	}

	dst := make([]byte, lz4.CompressBlockBound(len(src)))

	n, err := lz4.CompressBlock(src, dst, nil)
	if err != nil {
		return nil, None, fmt.Errorf(
			"lz4 compression failed: %w",
			err,
		)
	}

	// LZ4 returns 0 if source is not compressible
	if n == 0 {
		return src, None, nil
	}

	dst = dst[:n]

	if !ShouldCompress(len(src), len(dst), minSavingsRate) {
		return src, None, nil
	}

	return dst, LZ4, nil
}

func decompressLZ4(
	src []byte,
	rawLen uint32,
) ([]byte, error) {
	dst := make([]byte, int(rawLen))

	n, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return nil, fmt.Errorf(
			"lz4 decompression failed: %w",
			err,
		)
	}

	if n != int(rawLen) {
		return nil, fmt.Errorf(
			"lz4 decompression returned %d bytes, expected %d",
			n,
			rawLen,
		)
	}

	return dst, nil
}
