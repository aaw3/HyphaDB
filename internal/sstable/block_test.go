package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/rand"
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/compression"
	"github.com/aaw3/hyphadb/internal/record"
)

// ===============
//
//	Test helpers
//
// ===============
func requireErrorIs(t *testing.T, err, target error) {
	t.Helper()

	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}

func rewriteBlockChecksum(physical []byte) {
	checksumOffset := len(physical) - blockTrailerSize

	checksum := crc32.Checksum(
		physical[:checksumOffset],
		crc32cTable,
	)

	binary.LittleEndian.PutUint32(
		physical[checksumOffset:],
		checksum,
	)
}

// ===============
//  Tests
// ===============

func makeLogicalBlock(t *testing.T, records []record.Record) []byte {
	t.Helper()

	var buf bytes.Buffer

	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(records)))

	if _, err := buf.Write(count[:]); err != nil {
		t.Fatalf("write record count: %v", err)
	}

	for _, rec := range records {
		if err := record.EncodeBinary(&buf, rec); err != nil {
			t.Fatalf("encode record: %v", err)
		}
	}

	return buf.Bytes()
}

func TestPhysicalBlockRoundTrip(t *testing.T) {
	want := []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
		{Key: "carrot", Seq: 3, Entry: record.Entry{Value: []byte("orange")}},
	}

	logical := makeLogicalBlock(t, want)

	physical, err := encodePhysicalBlock(logical, compression.None, DefaultMinCompressionSavingsRate)
	if err != nil {
		t.Fatalf("encodePhysicalBlock failed: %v", err)
	}

	got, err := decodeBlock(physical)
	if err != nil {
		t.Fatalf("decodeBlock failed: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("records = %+v, want %+v", got, want)
	}
}

func TestDecodePhysicalBlockRejectsCorruptPayload(t *testing.T) {
	logical := makeLogicalBlock(t, []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
	})

	physical, err := encodePhysicalBlock(logical, compression.None, DefaultMinCompressionSavingsRate)
	if err != nil {
		t.Fatalf("encodePhysicalBlock failed: %v", err)
	}

	payloadOffset := blockHeaderSize
	physical[payloadOffset] ^= 0xFF // corrupt first payload byte with bitwise XOR

	_, err = decodePhysicalBlock(physical)
	requireErrorIs(t, err, ErrCorruptSSTable)
}

func TestDecodePhysicalBlockRejectsCorruptChecksum(t *testing.T) {
	logical := makeLogicalBlock(t, nil)

	physical, err := encodePhysicalBlock(
		logical,
		compression.None,
		DefaultMinCompressionSavingsRate,
	)
	if err != nil {
		t.Fatalf("encodePhysicalBlock failed: %v", err)
	}

	physical[len(physical)-1] ^= 0xff // corrupt last byte of checksum

	_, err = decodePhysicalBlock(physical)
	requireErrorIs(t, err, ErrCorruptSSTable)
}

func TestDecodePhysicalBlockRejectsUnknownCodec(t *testing.T) {
	logical := makeLogicalBlock(t, nil)

	physical, err := encodePhysicalBlock(
		logical,
		compression.None,
		DefaultMinCompressionSavingsRate,
	)
	if err != nil {
		t.Fatalf("encodePhysicalBlock failed: %v", err)
	}

	physical[0] = 99 // set codec byte to an unknown value
	rewriteBlockChecksum(physical)

	_, err = decodePhysicalBlock(physical)
	requireErrorIs(t, err, ErrCorruptSSTable)
}

func TestDecodePhysicalBlockRejectsRawLengthMismatch(t *testing.T) {
	logical := makeLogicalBlock(t, nil)

	physical, err := encodePhysicalBlock(
		logical,
		compression.None,
		DefaultMinCompressionSavingsRate,
	)
	if err != nil {
		t.Fatalf("encodePhysicalBlock failed: %v", err)
	}

	rawLen := binary.LittleEndian.Uint32(physical[1:5])
	binary.LittleEndian.PutUint32(
		physical[1:5],
		rawLen+1,
	)

	rewriteBlockChecksum(physical)

	_, err = decodePhysicalBlock(physical)
	requireErrorIs(t, err, ErrCorruptSSTable)
}

func TestDecodePhysicalBlockRejectsTruncatedBlock(t *testing.T) {
	physical := make([]byte, blockHeaderSize+blockTrailerSize-1)

	_, err := decodePhysicalBlock(physical)
	requireErrorIs(t, err, ErrCorruptSSTable)
}

func TestDecodeLogicalBlockRejectsTrailingBytes(t *testing.T) {
	logical := makeLogicalBlock(t, []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
	})

	logical = append(logical, 0xFF)

	_, err := decodeLogicalBlock(logical)
	requireErrorIs(t, err, ErrCorruptSSTable)
}

func TestDecodeLogicalBlockRejectsImpossibleRecordCount(t *testing.T) {
	var logical [4]byte

	binary.LittleEndian.PutUint32(
		logical[:],
		100,
	)

	_, err := decodeLogicalBlock(logical[:])
	requireErrorIs(t, err, ErrCorruptSSTable)
}

func TestEncodePhysicalBlockUsesLZ4ForCompressibleData(t *testing.T) {
	logical := bytes.Repeat([]byte("aaaaaaaaaaaaaaaa"), 4096)

	physical, err := encodePhysicalBlock(
		logical,
		compression.LZ4,
		DefaultMinCompressionSavingsRate,
	)
	if err != nil {
		t.Fatalf("encodePhysicalBlock error: %v", err)
	}

	codec := compression.Type(physical[0])
	if codec != compression.LZ4 {
		t.Fatalf("codec = %d, want %d",
			codec,
			compression.LZ4,
		)
	}

	got, err := decodePhysicalBlock(physical)
	if err != nil {
		t.Fatalf("decodePhysicalBlock error: %v", err)
	}

	if !bytes.Equal(got, logical) {
		t.Fatal("decoded logical block does not match input")
	}
}

func TestEncodePhysicalBlockFallsBackForIncompressibleData(t *testing.T) {
	logical := make([]byte, 64*1024)

	rng := rand.New(rand.NewSource(1))
	if _, err := rng.Read(logical); err != nil {
		t.Fatalf("random data: %v", err)
	}

	physical, err := encodePhysicalBlock(
		logical,
		compression.LZ4,
		DefaultMinCompressionSavingsRate,
	)
	if err != nil {
		t.Fatalf("encodePhysicalBlock error: %v", err)
	}

	codec := compression.Type(physical[0])
	if codec != compression.None {
		t.Fatalf("codec = %d, want %d",
			codec,
			compression.None,
		)
	}

	got, err := decodePhysicalBlock(physical)
	if err != nil {
		t.Fatalf("decodePhysicalBlock error: %v", err)
	}

	if !bytes.Equal(got, logical) {
		t.Fatal("decoded logical block does not match input")
	}
}
