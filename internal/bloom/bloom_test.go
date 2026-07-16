package bloom

import (
	"errors"
	"fmt"
	"testing"
)

func TestNewRejectsInvalidExpectedItems(t *testing.T) {
	_, err := New(0, 0.01)

	if !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf(
			"error = %v, want %v",
			err,
			ErrInvalidFilter,
		)
	}
}

func TestNewRejectsInvalidFalsePositiveRate(t *testing.T) {
	for _, rate := range []float64{0, 1} {
		t.Run(fmt.Sprintf("%g", rate), func(t *testing.T) {
			_, err := New(100, rate)

			if !errors.Is(err, ErrInvalidFilter) {
				t.Fatalf(
					"error = %v, want %v",
					err,
					ErrInvalidFilter,
				)
			}
		})
	}
}

func TestAddedKeysMayContain(t *testing.T) {
	filter, err := New(100, 0.01)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("dragonfruit"),
	}

	for _, key := range keys {
		filter.Add(key)
	}

	for _, key := range keys {
		if !filter.MayContain(key) {
			t.Fatalf("false negative for key %q", key)
		}
	}
}

func TestMissingKeysAreUsuallyRejected(t *testing.T) {
	filter, err := New(1_000, 0.01)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for i := 0; i < 1_000; i++ {
		filter.Add([]byte(fmt.Sprintf("present-%d", i)))
	}

	rejected := 0

	for i := 0; i < 1_000; i++ {
		if !filter.MayContain([]byte(fmt.Sprintf("missing-%d", i))) {
			rejected++
		}
	}

	// Do not require every absent key to be rejected; Bloom filters permit
	// false positives. This only verifies the filter is rejecting a reasonable
	// number of absent keys.
	if rejected < 900 {
		t.Fatalf("rejected %d missing keys, want at least 900", rejected)
	}
}

func TestNilFilterMayContain(t *testing.T) {
	var filter *Filter

	if !filter.MayContain([]byte("anything")) {
		t.Fatal("nil filter must conservatively return true")
	}
}

func TestRepeatedAddDoesNotLoseMembership(t *testing.T) {
	filter, err := New(10, 0.01)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for i := 0; i < 100; i++ {
		filter.Add([]byte("apple"))
	}

	if !filter.MayContain([]byte("apple")) {
		t.Fatal("false negative after repeated insertion")
	}
}
