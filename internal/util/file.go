package util

import (
	"fmt"
	"os"
	"path"
	"time"

	log "github.com/sirupsen/logrus"
)

// CreateFile is a helper for WAL, SSTable, Manifest and other classes that need to create files
// for on disk output
func CreateFile(prefix string, dbName string, dataDir string) *os.File {
	name := fmt.Sprintf("%s_%s_%d", prefix, dbName, time.Now().UnixNano()/1_000_000_000)

	tablePath := path.Join(dataDir, dbName, name)
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
