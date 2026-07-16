package bloom

import (
	"encoding/binary"
	"errors"
	"testing"
)

// ================
// Test helpers
// ================

func mustEncodedFilter(t *testing.T) []byte {
	t.Helper()

	filter, err := New(100, 0.01)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	filter.Add([]byte("apple"))

	encoded, err := filter.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	return encoded
}

// ================
// Tests
// ================

func TestEncodeDecodeRoundTrip(t *testing.T) {
	filter, err := New(100, 0.01)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
	}

	for _, key := range keys {
		filter.Add(key)
	}

	encoded, err := filter.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.bitCount != filter.bitCount {
		t.Fatalf(
			"bitCount = %d, want %d",
			decoded.bitCount,
			filter.bitCount,
		)
	}

	if decoded.hashCount != filter.hashCount {
		t.Fatalf(
			"hashCount = %d, want %d",
			decoded.hashCount,
			filter.hashCount,
		)
	}

	for _, key := range keys {
		if !decoded.MayContain(key) {
			t.Fatalf("false negative after round trip for %q", key)
		}
	}
}

func TestEncodeRejectsNilFilter(t *testing.T) {
	var filter *Filter

	_, err := filter.Encode()
	if !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrInvalidFilter,
		)
	}
}

func TestDecodeRejectsTruncatedHeader(t *testing.T) {
	_, err := Decode(make([]byte, encodedHeaderSize-1))

	if !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("error = %v, want %v", err,
			ErrInvalidFilter,
		)
	}
}

func TestDecodeRejectsUnknownVersion(t *testing.T) {
	filter := mustEncodedFilter(t)
	filter[0] = 99

	_, err := Decode(filter)

	if !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrInvalidFilter,
		)
	}
}

func TestDecodeRejectsUnknownHashAlgorithm(t *testing.T) {
	filter := mustEncodedFilter(t)
	filter[1] = 99

	_, err := Decode(filter)

	if !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrInvalidFilter,
		)
	}
}

func TestDecodeRejectsUnknownProbeAlgorithm(t *testing.T) {
	filter := mustEncodedFilter(t)
	filter[2] = 99

	_, err := Decode(filter)

	if !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrInvalidFilter,
		)
	}
}

func TestDecodeRejectsZeroHashCount(t *testing.T) {
	filter := mustEncodedFilter(t)
	filter[3] = 0

	_, err := Decode(filter)

	if !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrInvalidFilter,
		)
	}
}

func TestDecodeRejectsBitsetLengthMismatch(t *testing.T) {
	filter := mustEncodedFilter(t)

	bitCount := binary.LittleEndian.Uint64(filter[4:12])
	binary.LittleEndian.PutUint64(filter[4:12], bitCount+8)

	_, err := Decode(filter)

	if !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrInvalidFilter,
		)
	}
}

func TestDecodeCopiesBitset(t *testing.T) {
	encoded := mustEncodedFilter(t)

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	before := decoded.bits[0]
	encoded[encodedHeaderSize] ^= 0xff

	if decoded.bits[0] != before {
		t.Fatal("decoded filter aliases encoded input")
	}
}
