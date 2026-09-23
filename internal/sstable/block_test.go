package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

	var restartOffsets []uint32
	for i, rec := range records {
		if i%DefaultRestartInterval == 0 {
			restartOffsets = append(restartOffsets, uint32(buf.Len()))
		}
		if err := record.EncodeBinary(&buf, rec); err != nil {
			t.Fatalf("encode record: %v", err)
		}
	}

	var metadata [4]byte
	for _, offset := range restartOffsets {
		binary.LittleEndian.PutUint32(metadata[:], offset)
		buf.Write(metadata[:])
	}
	binary.LittleEndian.PutUint32(metadata[:], DefaultRestartInterval)
	buf.Write(metadata[:])
	binary.LittleEndian.PutUint32(metadata[:], uint32(len(restartOffsets)))
	buf.Write(metadata[:])

	return buf.Bytes()
}

func makeLegacyLogicalBlock(t *testing.T, records []record.Record) []byte {
	t.Helper()

	var buf bytes.Buffer
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(records)))
	buf.Write(count[:])
	for _, rec := range records {
		if err := record.EncodeBinary(&buf, rec); err != nil {
			t.Fatalf("encode legacy record: %v", err)
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

func TestLogicalBlockCursorReturnsRecordViews(t *testing.T) {
	logical := makeLogicalBlock(t, []record.Record{
		{Key: "apple", Seq: 7, Entry: record.Entry{Value: []byte("green")}},
		{Key: "banana", Seq: 4, Entry: record.Entry{Deleted: true}},
	})

	cursor, err := newLogicalBlockCursor(logical)
	if err != nil {
		t.Fatalf("newLogicalBlockCursor: %v", err)
	}

	first, ok := cursor.next()
	if !ok {
		t.Fatalf("first record missing: %v", cursor.err)
	}
	if first.Key != "apple" || first.Seq != 7 || string(first.Value) != "green" {
		t.Fatalf("first record = %+v", first)
	}

	valueOffset := 4 + record.HeaderSize + len(first.Key)
	logical[valueOffset] = 'G'
	if got := string(first.Value); got != "Green" {
		t.Fatalf("value view = %q, want %q", got, "Green")
	}

	second, ok := cursor.next()
	if !ok {
		t.Fatalf("second record missing: %v", cursor.err)
	}
	if second.Key != "banana" || second.Seq != 4 || !second.Deleted {
		t.Fatalf("second record = %+v", second)
	}

	if _, ok := cursor.next(); ok {
		t.Fatal("cursor returned an unexpected third record")
	}
	if cursor.err != nil {
		t.Fatalf("cursor error: %v", cursor.err)
	}
}

func TestLogicalBlockCursorSeeksFromRestartPoint(t *testing.T) {
	records := make([]record.Record, 64)
	for i := range records {
		records[i] = record.Record{
			Key:   fmt.Sprintf("key/%04d", i),
			Seq:   uint64(i + 1),
			Entry: record.Entry{Value: []byte("value")},
		}
	}

	cursor, err := newLogicalBlockCursor(makeLogicalBlock(t, records))
	if err != nil {
		t.Fatalf("newLogicalBlockCursor: %v", err)
	}
	if err := cursor.seek("key/0031"); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if cursor.index != 16 {
		t.Fatalf("cursor index after seek = %d, want restart index 16", cursor.index)
	}

	decoded := 0
	for {
		rec, ok := cursor.next()
		if !ok {
			t.Fatalf("target record missing: %v", cursor.err)
		}
		decoded++
		if rec.Key >= "key/0031" {
			if rec.Key != "key/0031" {
				t.Fatalf("seek landed on %q, want key/0031", rec.Key)
			}
			break
		}
	}
	if decoded != 16 {
		t.Fatalf("records decoded after seek = %d, want 16", decoded)
	}
}

func TestLogicalBlockCursorReadsLegacyFormat(t *testing.T) {
	want := []record.Record{
		{Key: "apple", Seq: 2, Entry: record.Entry{Value: []byte("green")}},
		{Key: "banana", Seq: 1, Entry: record.Entry{Value: []byte("yellow")}},
	}
	cursor, err := newLogicalBlockCursorForVersion(
		makeLegacyLogicalBlock(t, want),
		legacyFormatVersion,
	)
	if err != nil {
		t.Fatalf("new legacy cursor: %v", err)
	}

	var got []record.Record
	for {
		rec, ok := cursor.next()
		if !ok {
			break
		}
		got = append(got, rec)
	}
	if cursor.err != nil {
		t.Fatalf("legacy cursor: %v", cursor.err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy records = %+v, want %+v", got, want)
	}
}

func TestLogicalBlockCursorDoesNotAllocatePerRecord(t *testing.T) {
	logical := makeLogicalBlock(t, []record.Record{
		{Key: "apple", Seq: 3, Entry: record.Entry{Value: []byte("green")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
		{Key: "carrot", Seq: 1, Entry: record.Entry{Value: []byte("orange")}},
	})

	var sequenceSum uint64
	allocations := testing.AllocsPerRun(1000, func() {
		cursor, err := newLogicalBlockCursor(logical)
		if err != nil {
			panic(err)
		}

		var sum uint64
		for {
			rec, ok := cursor.next()
			if !ok {
				break
			}
			sum += rec.Seq
		}
		if cursor.err != nil {
			panic(cursor.err)
		}
		sequenceSum = sum
	})

	if sequenceSum != 6 {
		t.Fatalf("sequence sum = %d, want 6", sequenceSum)
	}
	if allocations != 0 {
		t.Fatalf("cursor allocations = %f, want 0", allocations)
	}
}

func TestLogicalBlockCursorRejectsCorruptRecord(t *testing.T) {
	logical := makeLogicalBlock(t, []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
	})
	logical[4+16] = 1 << 7

	cursor, err := newLogicalBlockCursor(logical)
	if err != nil {
		t.Fatalf("newLogicalBlockCursor: %v", err)
	}
	if _, ok := cursor.next(); ok {
		t.Fatal("cursor accepted a record with unknown flags")
	}
	requireErrorIs(t, cursor.err, ErrCorruptSSTable)
}

func TestLogicalBlockCursorRejectsTrailingBytes(t *testing.T) {
	logical := makeLogicalBlock(t, []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
	})
	logical = append(logical, 0xff)

	cursor, err := newLogicalBlockCursor(logical)
	if err != nil {
		requireErrorIs(t, err, ErrCorruptSSTable)
		return
	}
	if _, ok := cursor.next(); !ok {
		t.Fatalf("record missing: %v", cursor.err)
	}
	if _, ok := cursor.next(); ok {
		t.Fatal("cursor returned a record from trailing bytes")
	}
	requireErrorIs(t, cursor.err, ErrCorruptSSTable)
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

func TestShouldCompress(t *testing.T) {
	tests := []struct {
		name           string
		rawSize        int
		compressedSize int
		minSavingsRate float64
		want           bool
	}{
		{
			name:           "exact threshold",
			rawSize:        100,
			compressedSize: 87,
			minSavingsRate: 0.13,
			want:           true,
		},
		{
			name:           "above threshold",
			rawSize:        100,
			compressedSize: 80,
			minSavingsRate: 0.125,
			want:           true,
		},
		{
			name:           "below threshold",
			rawSize:        100,
			compressedSize: 90,
			minSavingsRate: 0.125,
			want:           false,
		},
		{
			name:           "same size",
			rawSize:        100,
			compressedSize: 100,
			minSavingsRate: 0,
			want:           false,
		},
		{
			name:           "compressed larger than raw",
			rawSize:        100,
			compressedSize: 110,
			minSavingsRate: 0,
			want:           false,
		},
		{
			name:           "empty input",
			rawSize:        0,
			compressedSize: 0,
			minSavingsRate: 0,
			want:           false,
		},
		{
			name:           "negative raw size",
			rawSize:        -1,
			compressedSize: 0,
			minSavingsRate: 0,
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldCompress(
				tt.rawSize,
				tt.compressedSize,
				tt.minSavingsRate,
			)

			if got != tt.want {
				t.Fatalf(
					"shouldCompress(%d, %d, %f) = %v, want %v",
					tt.rawSize,
					tt.compressedSize,
					tt.minSavingsRate,
					got,
					tt.want,
				)
			}
		})
	}
}
