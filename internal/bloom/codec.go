package bloom

import (
	"encoding/binary"
	"fmt"
)

type HashAlgorithm byte

const (
	HashUnknown HashAlgorithm = iota
	HashXXH3_128
)

type ProbeAlgorithm byte

const (
	ProbeUnknown ProbeAlgorithm = iota
	ProbeEnhancedDoubleHash
)

const (
	currentVersion     byte   = 1
	encodedHeaderSize         = 12
	maxEncodedBitCount uint64 = 256 << 20 * 8 // 256MB in bits
)

func (f *Filter) Encode() ([]byte, error) {
	if f == nil {
		return nil, fmt.Errorf(
			"%w: cannot encode nil filter",
			ErrInvalidFilter,
		)
	}

	if f.hashCount == 0 {
		return nil, fmt.Errorf(
			"%w: cannot encode filter with zero hash functions",
			ErrInvalidFilter,
		)
	}

	if f.bitCount == 0 || f.bitCount%8 != 0 {
		return nil, fmt.Errorf(
			"%w: cannot encode filter with invalid bit count %d",
			ErrInvalidFilter,
			f.bitCount,
		)
	}

	if uint64(len(f.bits)) != f.bitCount/8 {
		return nil, fmt.Errorf(
			"%w: cannot encode filter with mismatched bit count %d and bitset length %d",
			ErrInvalidFilter,
			len(f.bits),
			f.bitCount/8,
		)
	}

	buf := make([]byte, encodedHeaderSize+len(f.bits))

	buf[0] = currentVersion
	buf[1] = byte(HashXXH3_128)
	buf[2] = byte(ProbeEnhancedDoubleHash)
	buf[3] = f.hashCount
	binary.LittleEndian.PutUint64(buf[4:12], f.bitCount)

	copy(buf[encodedHeaderSize:], f.bits)

	return buf, nil
}

func Decode(buf []byte) (*Filter, error) {
	if len(buf) < encodedHeaderSize {
		return nil, fmt.Errorf(
			"%w: buffer too small to decode filter header",
			ErrInvalidFilter,
		)
	}

	version := buf[0]
	HashAlgorithm := HashAlgorithm(buf[1])
	ProbeAlgorithm := ProbeAlgorithm(buf[2])
	hashCount := buf[3]
	bitCount := binary.LittleEndian.Uint64(buf[4:12])

	if version != currentVersion {
		return nil, fmt.Errorf(
			"%w: unsupported filter version %d",
			ErrInvalidFilter,
			version,
		)
	}

	if HashAlgorithm != HashXXH3_128 {
		return nil, fmt.Errorf(
			"%w: unsupported hash algorithm %d",
			ErrInvalidFilter,
			HashAlgorithm,
		)
	}

	if ProbeAlgorithm != ProbeEnhancedDoubleHash {
		return nil, fmt.Errorf(
			"%w: unsupported probe algorithm %d",
			ErrInvalidFilter,
			ProbeAlgorithm,
		)
	}

	if hashCount == 0 || hashCount > 30 {
		return nil, fmt.Errorf(
			"%w: invalid hash count %d",
			ErrInvalidFilter,
			hashCount,
		)
	}

	if bitCount == 0 || bitCount%8 != 0 {
		return nil, fmt.Errorf(
			"%w: invalid bit count %d",
			ErrInvalidFilter,
			bitCount,
		)
	}

	if bitCount > maxEncodedBitCount {
		return nil, fmt.Errorf(
			"%w: bit count %d exceeds maximum %d",
			ErrInvalidFilter,
			bitCount,
			maxEncodedBitCount,
		)
	}

	expectedBytes := bitCount / 8
	actualBytes := uint64(len(buf) - encodedHeaderSize)

	if actualBytes != expectedBytes {
		return nil, fmt.Errorf(
			"%w: expected %d bytes for bitset, got %d",
			ErrInvalidFilter,
			expectedBytes,
			actualBytes,
		)
	}

	bits := make([]byte, expectedBytes)
	copy(bits, buf[encodedHeaderSize:])

	return &Filter{
		bits:      bits,
		bitCount:  bitCount,
		hashCount: hashCount,
	}, nil
}
