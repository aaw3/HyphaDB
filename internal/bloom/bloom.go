package bloom

import (
	"errors"
	"fmt"
	"math"

	"github.com/zeebo/xxh3"
)

var ErrInvalidFilter = errors.New("invalid bloom filter")

const (
	DefaultFalsePositiveRate = 0.01
)

type Filter struct {
	bits      []byte
	bitCount  uint64
	hashCount uint8
}

func New(expectedItems int, falsePositiveRate float64) (*Filter, error) {
	if expectedItems <= 0 {
		return nil, fmt.Errorf("%w: expectedItems must be greater than 0",
			ErrInvalidFilter,
		)
	}

	if falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		return nil, fmt.Errorf("%w: falsePositiveRate must be in (0, 1)",
			ErrInvalidFilter,
		)
	}

	n := float64(expectedItems)
	p := falsePositiveRate

	// Memory Allocation / required bit array size equation
	// m = -(n * ln(p)) / (ln(2)^2)
	bitCount := uint64(math.Ceil(-n * math.Log(p) / (math.Ln2 * math.Ln2)))

	// Optimal Number of Hash Functions
	// k = (m / n) * ln(2)
	hashCount := int(math.Round((float64(bitCount) / n) * math.Ln2))

	// require at least 1 hash function
	if hashCount < 1 {
		hashCount = 1
	}

	// cap at 30 hash functions to prevent CPU overhead
	if hashCount > 30 {
		hashCount = 30
	}

	byteCount := (bitCount + 7) / 8 // round up to nearest byte
	bitCount = byteCount * 8

	return &Filter{
		bits:      make([]byte, byteCount),
		bitCount:  bitCount,
		hashCount: uint8(hashCount),
	}, nil
}

// return a hash pair from xxh3.Hash128 split into low and high 64-bit values
func hashPair(key []byte) (uint64, uint64) {
	hash := xxh3.Hash128(key)

	var high = hash.Hi
	var low = hash.Lo

	// high is 0, use fibonacci hashing for step to avoid clustering
	if hash.Hi == 0 {
		high = 0x9e3779b97f4a7c15
	}

	return low, high
}

// Insert a key into the Bloom filter
func (f *Filter) Add(key []byte) {
	h1, h2 := hashPair(key)

	position := h1
	increment := h2

	// iterate over the number of hash functions
	for i := uint64(0); i < uint64(f.hashCount); i++ {
		// use modulo to wrap around bit array since f.bitCount may not be a power of 2
		bit := position % f.bitCount

		// Find the specific byte and set the bit within it to 1
		f.bits[bit/8] |= byte(1) << (bit % 8)

		// Use Enhanced Double Hashing (Dillinger & Manolios)
		// defined by h1 + i*h2 + (i^3-i)/6
		// Reduces cyclic dependencies and acts as k independent hash functions
		position += increment
		increment += i + 1
	}
}

// Check if key may exist in the Bloom filter
func (f *Filter) MayContain(key []byte) bool {
	// treat an empty filter as "may exist" to avoid false negatives
	if f == nil || f.bitCount == 0 {
		return true
	}

	h1, h2 := hashPair(key)

	position := h1
	increment := h2

	for i := uint64(0); i < uint64(f.hashCount); i++ {
		bit := position % f.bitCount

		// exit early if any bit is not set (true negative)
		// stop generating hashes and exit to save CPU cycles
		if f.bits[bit/8]&(byte(1)<<(bit%8)) == 0 {
			return false
		}

		// advance to next probe position using cubic recurrence relation
		position += increment
		increment += i + 1
	}

	// All bits set, key may exist
	return true
}
