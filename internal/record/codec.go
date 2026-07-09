package record

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	FlagDeleted byte = 1 << 0
)

func EncodedSize(rec Record) int {
	// 4 bytes for key length
	// 4 bytes for value length
	// 8 bytes for sequence number
	// 1 byte for flags
	return 4 + 4 + 8 + 1 + len(rec.Key) + len(rec.Value)
}

func EncodeBinary(w io.Writer, rec Record) error {
	var header [17]byte

	// write the lengths and sequence number in little-endian format
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(rec.Key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(rec.Value)))
	binary.LittleEndian.PutUint64(header[8:16], rec.Seq)

	if rec.Deleted {
		header[16] = FlagDeleted
	}

	//write header to the writer
	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	//write the record key to the writer
	if _, err := io.WriteString(w, rec.Key); err != nil {
		return err
	}

	// write the record value to the writer
	if len(rec.Value) > 0 {
		if _, err := w.Write(rec.Value); err != nil {
			return err
		}
	}

	return nil
}

func DecodeBinary(r io.Reader) (Record, error) {
	var header [17]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Record{}, err
	}

	// extract the lengths and sequence number from the header
	keyLen := binary.LittleEndian.Uint32(header[0:4])
	valueLen := binary.LittleEndian.Uint32(header[4:8])
	seq := binary.LittleEndian.Uint64(header[8:16])
	flags := header[16]

	// read the key and value from the reader
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return Record{}, err
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return Record{}, err
	}

	// determine if other flags are set
	if flags&^FlagDeleted != 0 {
		return Record{}, fmt.Errorf("unknown record flags: %08b", flags)
	}

	return Record{
		Key: string(key),
		Seq: seq,
		Entry: Entry{
			Value:   value,
			Deleted: flags&FlagDeleted != 0,
		},
	}, nil
}
