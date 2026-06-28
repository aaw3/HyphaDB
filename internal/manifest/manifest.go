package manifest

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Manifest struct {
	NextSSTableID int
	SSTablePaths  []string
}

func Read(path string) (*Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manifest{
				NextSSTableID: 0,
				SSTablePaths:  []string{},
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
	maxID := -1

	for _, path := range m.SSTablePaths {
		id, ok := extractSSTableID(path)
		if ok && id > maxID {
			maxID = id
		}
	}

	if m.NextSSTableID <= maxID {
		m.NextSSTableID = maxID + 1
	}
}

func extractSSTableID(path string) (int, bool) {
	base := filepath.Base(path)

	parts := strings.Split(base, "-")
	if len(parts) != 2 {
		return 0, false
	}

	idPart := strings.TrimSuffix(parts[1], ".sst")
	id, err := strconv.Atoi(idPart)
	if err != nil {
		return 0, false
	}

	return id, true
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
