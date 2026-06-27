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
