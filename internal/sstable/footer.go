package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	footerSize = 40

	currentFormatVersion = 2
)

const tableMagic = "HYPSST"

type footerMetadata struct {
	indexOffset  uint64
	indexLength  uint64
	filterOffset uint64
	filterLength uint64
}

func writeFooter(
	w io.Writer,
	indexOffset uint64,
	indexLength uint64,
	filterOffset uint64,
	filterLength uint64,
) error {
	var footer [footerSize]byte

	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], indexLength)
	binary.LittleEndian.PutUint64(footer[16:24], filterOffset)
	binary.LittleEndian.PutUint64(footer[24:32], filterLength)

	copy(footer[32:38], tableMagic[:])
	footer[38] = currentFormatVersion
	footer[39] = 0 // reserved flags byte

	_, err := w.Write(footer[:])
	return err
}

func readFooter(file *os.File) (footerMetadata, error) {
	info, err := file.Stat()
	if err != nil {
		return footerMetadata{}, err
	}

	fileSize := uint64(info.Size())

	if fileSize < footerSize {
		return footerMetadata{}, fmt.Errorf("%w: SSTable too small",
			ErrCorruptSSTable,
		)
	}

	if _, err := file.Seek(-footerSize, io.SeekEnd); err != nil {
		return footerMetadata{}, err
	}

	var footer [footerSize]byte
	if _, err := io.ReadFull(file, footer[:]); err != nil {
		return footerMetadata{}, fmt.Errorf(
			"%w: read footer: %v",
			ErrCorruptSSTable,
			err,
		)
	}

	if string(footer[32:38]) != tableMagic[:] {
		return footerMetadata{}, fmt.Errorf(
			"%w: invalid SSTable magic string",
			ErrCorruptSSTable,
		)
	}

	version := footer[38]
	if version != currentFormatVersion {
		return footerMetadata{}, fmt.Errorf(
			"%w: unsupported SSTable format version: %d",
			ErrCorruptSSTable,
			version,
		)
	}

	flags := footer[39]
	if flags != 0 { // reserved for future use, only zero is supported currently
		return footerMetadata{}, fmt.Errorf(
			"%w: unsupported sstable footer flags: %#x",
			ErrCorruptSSTable,
			flags,
		)
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexLength := binary.LittleEndian.Uint64(footer[8:16])
	filterOffset := binary.LittleEndian.Uint64(footer[16:24])
	filterLength := binary.LittleEndian.Uint64(footer[24:32])

	dataEnd := fileSize - uint64(footerSize)

	if indexOffset > dataEnd {
		return footerMetadata{}, fmt.Errorf(
			"%w: index offset %d exceeds metadata boundary %d",
			ErrCorruptSSTable,
			indexOffset,
			dataEnd,
		)
	}

	if indexLength > dataEnd-indexOffset {
		return footerMetadata{}, fmt.Errorf(
			"%w: index length %d exceeds data end %d minus index offset %d",
			ErrCorruptSSTable,
			indexLength,
			dataEnd,
			indexOffset,
		)
	}

	indexEnd := indexOffset + indexLength

	if filterLength == 0 {
		if filterOffset != 0 {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter length is zero but filter offset is non-zero (%d)",
				ErrCorruptSSTable,
				filterOffset,
			)
		}

		if indexEnd != dataEnd {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter length is zero but index end %d does not match data end %d",
				ErrCorruptSSTable,
				indexEnd,
				dataEnd,
			)
		}
	} else {
		if filterOffset != indexEnd {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter offset %d does not match index end %d",
				ErrCorruptSSTable,
				filterOffset,
				indexEnd,
			)
		}

		if filterOffset > dataEnd {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter offset %d exceeds metadata boundary %d",
				ErrCorruptSSTable,
				filterOffset,
				dataEnd,
			)
		}

		if filterLength > dataEnd-filterOffset {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter length %d exceeds data end %d minus filter offset %d",
				ErrCorruptSSTable,
				filterLength,
				dataEnd,
				filterOffset,
			)
		}

		if filterOffset+filterLength != dataEnd {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter end %d does not match data end %d",
				ErrCorruptSSTable,
				filterOffset+filterLength,
				dataEnd,
			)
		}
	}

	return footerMetadata{
		indexOffset,
		indexLength,
		filterOffset,
		filterLength,
	}, nil
}
