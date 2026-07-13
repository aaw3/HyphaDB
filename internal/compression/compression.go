package compression

import "fmt"

type Type byte

const (
	None Type = iota
	LZ4
)

const DefaultMinSavingsRate = 0.125

func Compress(
	src []byte,
	requested Type,
	minSavingsRate float64,
) ([]byte, Type, error) {
	switch requested {
	case None:
		return src, None, nil

	case LZ4:
		return compressLZ4(src, minSavingsRate)

	default:
		return nil, None, fmt.Errorf(
			"unknown compression type: %d",
			requested,
		)
	}
}

func Decompress(
	src []byte,
	rawLen uint32,
	codec Type,
) ([]byte, error) {
	switch codec {
	case None:
		if uint32(len(src)) != rawLen {
			return nil, fmt.Errorf(
				"raw payload length %d does not match expected length %d",
				len(src),
				rawLen,
			)
		}
		return src, nil

	case LZ4:
		return decompressLZ4(src, rawLen)

	default:
		return nil, fmt.Errorf(
			"unknown compression type: %d",
			codec,
		)
	}
}

func ShouldCompress(
	rawSize int,
	compressedSize int,
	minSavingsRate float64,
) bool {
	if rawSize <= 0 {
		return false
	}

	if compressedSize >= rawSize {
		return false
	}

	saved := rawSize - compressedSize
	savingsRate := float64(saved) / float64(rawSize)

	return savingsRate >= minSavingsRate
}
