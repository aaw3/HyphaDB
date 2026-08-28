package manifest

import (
	"encoding/gob"
	"os"
)

type Manifest struct {
	NextSSTableID    uint64
	NextWALSegmentID uint64
	SSTables         []SSTableMetadata
}

type SSTableMetadata struct {
	ID          uint64
	Path        string
	Level       uint32
	SmallestKey string
	LargestKey  string
}

func Read(path string) (*Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manifest{
				NextSSTableID:    0,
				NextWALSegmentID: 0,
				SSTables:         []SSTableMetadata{},
			}, nil
		}
		return nil, err
	}
	defer file.Close()

	var manifest Manifest
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}

	ensureSafeNextSSTableID(&manifest)

	return &manifest, nil
}

func ensureSafeNextSSTableID(m *Manifest) {
	var maxID uint64
	hasTables := false

	for _, table := range m.SSTables {
		if !hasTables || table.ID > maxID {
			maxID = table.ID
			hasTables = true
		}
	}

	if hasTables && m.NextSSTableID <= maxID {
		m.NextSSTableID = maxID + 1
	}
}

func Write(path string, manifest *Manifest) error {
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(manifest); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return os.Rename(tmpPath, path)
}
