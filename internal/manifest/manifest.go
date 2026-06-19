package manifest

import (
	"encoding/gob"
	"os"
)

type Manifest struct {
	SSTablePaths []string
}

func Read(path string) (*Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manifest{SSTablePaths: []string{}}, nil
		}
		return nil, err
	}
	defer file.Close()

	var manifest Manifest
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

func Write(path string, manifest *Manifest) error {
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	// Rename temp file to target path
	return os.Rename(tmpPath, path)
}
