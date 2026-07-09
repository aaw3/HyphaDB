package record

import (
	"bytes"
	"strings"
	"testing"
)

func TestEncodeDecodeBinaryRecord(t *testing.T) {
	want := Record{
		Key: "apple",
		Seq: 1,
		Entry: Entry{
			Value:   []byte("fruit"),
			Deleted: false,
		},
	}

	var buf bytes.Buffer
	if err := EncodeBinary(&buf, want); err != nil {
		t.Fatalf("EncodeBinary failed: %v", err)
	}

	got, err := DecodeBinary(&buf)
	if err != nil {
		t.Fatalf("DecodeBinary failed: %v", err)
	}

	if got.Key != want.Key {
		t.Errorf("Key mismatch: got %q, want %q", got.Key, want.Key)
	}

	if got.Seq != want.Seq {
		t.Errorf("Seq mismatch: got %d, want %d", got.Seq, want.Seq)
	}

	if string(got.Value) != string(want.Value) {
		t.Errorf("Value mismatch: got %q, want %q", got.Value, want.Value)
	}

	if got.Deleted != want.Deleted {
		t.Errorf("Deleted mismatch: got %v, want %v", got.Deleted, want.Deleted)
	}
}

func TestEncodeDecodeBinaryTombstone(t *testing.T) {
	want := Record{
		Key: "banana",
		Seq: 2,
		Entry: Entry{
			Value:   nil,
			Deleted: true,
		},
	}

	var buf bytes.Buffer
	if err := EncodeBinary(&buf, want); err != nil {
		t.Fatalf("EncodeBinary failed: %v", err)
	}

	got, err := DecodeBinary(&buf)
	if err != nil {
		t.Fatalf("DecodeBinary failed: %v", err)
	}

	if got.Key != want.Key {
		t.Errorf("Key mismatch: got %q, want %q", got.Key, want.Key)
	}

	if got.Seq != want.Seq {
		t.Errorf("Seq mismatch: got %d, want %d", got.Seq, want.Seq)
	}

	if !got.Deleted {
		t.Errorf("Deleted mismatch: got %v, want %v", got.Deleted, want.Deleted)
	}

	if len(got.Value) != 0 {
		t.Errorf("Value mismatch: got %q, want empty", got.Value)
	}
}

func TestDecodeBinaryRejectsUnknownFlags(t *testing.T) {
	rec := Record{
		Key: "cherry",
		Seq: 3,
		Entry: Entry{
			Value:   []byte("fruit"),
			Deleted: false,
		},
	}

	var buf bytes.Buffer
	if err := EncodeBinary(&buf, rec); err != nil {
		t.Fatalf("EncodeBinary failed: %v", err)
	}

	// Corrupt the flags byte to include an unknown flag
	data := buf.Bytes()
	data[16] = 1 << 7

	_, err := DecodeBinary(bytes.NewReader(data))
	if err == nil {
		t.Fatalf("DecodeBinary did not reject unknown flags")
	}

	if !strings.Contains(err.Error(), "unknown record flags") {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestEncodedSizeMatchesWrittenBytes(t *testing.T) {
	rec := Record{
		Key: "date",
		Seq: 4,
		Entry: Entry{
			Value:   []byte("fruit"),
			Deleted: false,
		},
	}

	var buf bytes.Buffer
	if err := EncodeBinary(&buf, rec); err != nil {
		t.Fatalf("EncodeBinary failed: %v", err)
	}

	if got, want := buf.Len(), EncodedSize(rec); got != want {
		t.Errorf("Encoded size mismatch: got %d, want %d", got, want)
	}
}
