package compression

import "fmt"

type Type byte

const (
	None Type = iota
	LZ4
)

func Compress(
	src []byte,
	requested Type,
) ([]byte, error) {
	switch requested {
	case None:
		return src, nil

	case LZ4:
		return compressLZ4(src)

	default:
		return nil, fmt.Errorf(
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
