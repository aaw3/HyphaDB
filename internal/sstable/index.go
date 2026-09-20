package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"
)

type IndexEntry struct {
	FirstKey string
	Offset   uint64
	Length   uint32
}

// firstPossibleBlock returns the earliest block that may contain key. The
// preceding block must be included because a key can begin near the end of one
// block and continue into later blocks whose FirstKey is the same key.
func firstPossibleBlock(index []IndexEntry, key string) int {
	if len(index) == 0 {
		return -1
	}

	i := sort.Search(len(index), func(i int) bool {
		return index[i].FirstKey >= key
	})
	if i == len(index) {
		return len(index) - 1
	}
	if i > 0 {
		return i - 1
	}
	return 0
}

func writeIndex(w io.Writer, index []IndexEntry) error {
	var buf bytes.Buffer
	var count [4]byte

	binary.LittleEndian.PutUint32(count[:], uint32(len(index)))
	buf.Write(count[:])

	for _, entry := range index {
		var header [16]byte

		binary.LittleEndian.PutUint32(header[0:4], uint32(len(entry.FirstKey)))
		binary.LittleEndian.PutUint64(header[4:12], entry.Offset)
		binary.LittleEndian.PutUint32(header[12:16], entry.Length)

		buf.Write(header[:])
		buf.WriteString(entry.FirstKey)
	}

	_, err := w.Write(buf.Bytes())
	return err
}

func decodeIndex(buf []byte) ([]IndexEntry, error) {
	r := bytes.NewReader(buf)

	var countBuf [4]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, err
	}

	count := binary.LittleEndian.Uint32(countBuf[:])
	index := make([]IndexEntry, 0, count)

	// read each index entry
	for i := uint32(0); i < count; i++ {
		var header [16]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return nil, err
		}

		keyLen := binary.LittleEndian.Uint32(header[0:4])
		offset := binary.LittleEndian.Uint64(header[4:12])
		length := binary.LittleEndian.Uint32(header[12:16])

		if uint64(r.Len()) < uint64(keyLen) {
			return nil, errors.New("invalid index key length")
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, err
		}

		// add the index entry to the slice
		index = append(index, IndexEntry{
			FirstKey: string(key),
			Offset:   offset,
			Length:   length,
		})
	}

	return index, nil
}
