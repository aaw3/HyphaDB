package compression

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestCompressNoneReturnsOriginalPayload(t *testing.T) {
	src := []byte("hello world")

	got, codec, err := Compress(src, None, DefaultMinSavingsRate)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	if codec != None {
		t.Fatalf("codec = %d, want %d",
			codec,
			None,
		)
	}

	if !bytes.Equal(got, src) {
		t.Fatalf("payload = %q, want %q", got, src)
	}
}

func TestDecompressNoneReturnsOriginalPayload(t *testing.T) {
	src := []byte("hello world")

	got, err := Decompress(src, uint32(len(src)), None)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}

	if !bytes.Equal(got, src) {
		t.Fatalf("payload = %q, want %q", got, src)
	}
}

func TestDecompressNoneRejectsLengthMismatch(t *testing.T) {
	src := []byte("hello")

	_, err := Decompress(src, uint32(len(src))+1, None)
	if err == nil {
		t.Fatal("Decompress succeeded, want length mismatch error")
	}
}

func TestCompressLZ4CompressiblePayload(t *testing.T) {
	src := bytes.Repeat(
		[]byte("hyphadb-compressible-data-"),
		4096,
	)

	compressed, codec, err := Compress(
		src,
		LZ4,
		0.125,
	)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	if codec != LZ4 {
		t.Fatalf("codec = %d, want LZ4", codec)
	}

	if len(compressed) >= len(src) {
		t.Fatalf(
			"compressed length = %d, raw length = %d",
			len(compressed),
			len(src),
		)
	}

	got, err := Decompress(
		compressed,
		uint32(len(src)),
		codec,
	)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if !bytes.Equal(got, src) {
		t.Fatal("decompressed payload does not match source")
	}
}

func TestCompressLZ4FallsBackForIncompressiblePayload(t *testing.T) {
	src := make([]byte, 64*1024)

	rng := rand.New(rand.NewSource(1))
	if _, err := rng.Read(src); err != nil {
		t.Fatalf("generate random bytes: %v", err)
	}

	stored, codec, err := Compress(
		src,
		LZ4,
		0.125,
	)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	if codec != None {
		t.Fatalf("codec = %d, want %d",
			codec,
			None,
		)
	}

	if !bytes.Equal(stored, src) {
		t.Fatal("fallback payload does not match source")
	}
}

func TestCompressionSavingsThreshold(t *testing.T) {
	tests := []struct {
		name           string
		rawSize        int
		compressedSize int
		minSavings     float64
		want           bool
	}{
		{
			name:           "exact threshold",
			rawSize:        100,
			compressedSize: 87,
			minSavings:     0.13,
			want:           true,
		},
		{
			name:           "below threshold",
			rawSize:        100,
			compressedSize: 90,
			minSavings:     0.125,
			want:           false,
		},
		{
			name:           "compressed larger than raw",
			rawSize:        100,
			compressedSize: 110,
			minSavings:     0,
			want:           false,
		},
		{
			name:           "empty input",
			rawSize:        0,
			compressedSize: 0,
			minSavings:     0,
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShouldCompress(
				tt.rawSize,
				tt.compressedSize,
				tt.minSavings,
			)

			if got != tt.want {
				t.Fatalf(
					"ShouldCompress(%d, %d, %f) = %v, want %v",
					tt.rawSize,
					tt.compressedSize,
					tt.minSavings,
					got,
					tt.want,
				)
			}
		})
	}
}

func TestCompressRejectsUnknownCodec(t *testing.T) {
	_, _, err := Compress(
		[]byte("data"),
		Type(99),
		0.125,
	)

	if err == nil {
		t.Fatal("Compress succeeded, want unknown codec error")
	}
}

func TestDecompressRejectsUnknownCodec(t *testing.T) {
	_, err := Decompress(
		[]byte("data"),
		4,
		Type(99),
	)

	if err == nil {
		t.Fatal("Decompress succeeded, want unknown codec error")
	}
}
