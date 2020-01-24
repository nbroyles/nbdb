package util

import (
	"os"
	"path"

	log "github.com/sirupsen/logrus"
)

// CreateFile is a helper for WAL, SSTable, Manifest and other classes that need to create files
// for on disk output
func CreateFile(filename string, dbName string, dataDir string) *os.File {
	tablePath := path.Join(dataDir, dbName, filename)
	if _, err := os.Stat(tablePath); !os.IsNotExist(err) {
		if err != nil {
			log.Panicf("failure checking for %s existence: %v", tablePath, err)
		} else {
			log.Panicf("attempting to create %s but already exists", tablePath)
		}
	}

	file, err := os.Create(tablePath)
	if err != nil {
		log.Panicf("could not create %s file: %v", tablePath, err)
	}

	return file
}
