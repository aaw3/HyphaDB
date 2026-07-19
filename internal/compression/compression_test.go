package compression

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestCompressNoneReturnsOriginalPayload(t *testing.T) {
	src := []byte("hello world")

	got, err := Compress(src, None)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
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

	compressed, err := Compress(
		src,
		LZ4,
	)
	if err != nil {
		t.Fatalf("Compress: %v", err)
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
		LZ4,
	)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if !bytes.Equal(got, src) {
		t.Fatal("decompressed payload does not match source")
	}
}

func TestCompressLZ4IncompressiblePayloadStillRoundTrips(t *testing.T) {
	src := make([]byte, 64*1024)

	rng := rand.New(rand.NewSource(1))
	if _, err := rng.Read(src); err != nil {
		t.Fatalf("generate random bytes: %v", err)
	}

	compressed, err := Compress(src, LZ4)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	got, err := Decompress(
		compressed,
		uint32(len(src)),
		LZ4,
	)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if len(compressed) < len(src) {
		t.Fatalf(
			"compressed incompressible payload unexpectedly shrank: got %d, raw %d",
			len(compressed),
			len(src),
		)
	}

	if !bytes.Equal(got, src) {
		t.Fatal("decompressed payload does not match source")
	}
}

func TestCompressRejectsUnknownCodec(t *testing.T) {
	_, err := Compress(
		[]byte("data"),
		Type(99),
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
