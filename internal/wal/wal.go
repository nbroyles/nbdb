package wal

import (
	"fmt"
	"log"
	"os"
	"path"
	"time"
)

// WAL is the structure representing the writeahead log. All updates (incl. deletes)
// are first written to the writeahead log before being stored anywhere else (i.e. memtable,
//  sstables). This ensures that upon crash, memtable that was in memory can be regenerated
// from the writeahead log
type WAL struct {
	dbName  string
	name    string
	codec   Codec
	logFile *os.File
	size    uint32
}

// New creates a new writeahead log and returns a reference to it
func New(dbName string, datadir string) *WAL {
	name := fmt.Sprintf("wal_%s_%d", dbName, time.Now().UnixNano()/1_000_000_000)

	logPath := path.Join(datadir, dbName, name)
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		if err != nil {
			log.Panicf("failure checking for WAL existence: %v", err)
		} else {
			log.Panicf("attempting to create new WAL %s but already exists", logPath)
		}
	}

	logFile, err := os.Create(logPath)
	if err != nil {
		log.Panicf("could not create WAL file: %v", err)
	}

	return &WAL{dbName: dbName, name: name, codec: Codec{}, logFile: logFile}
}

// Write writes the record to the writeahead log
func (w *WAL) Write(record *Record) {
	data, err := w.codec.Encode(record)
	if err != nil {
		log.Panicf("failed encoding data to write to log: %v", err)
	}

	if n, err := w.logFile.Write(data); n != len(data) {
		log.Panicf("failed to write entirety of data to log, bytes written=%d, expected=%d",
			n, len(data))
	} else if err != nil {
		log.Panicf("failed to write data to log: %v", err)
	}

	// update current size of WAL
	w.size += uint32(len(data))

	if err := w.logFile.Sync(); err != nil {
		// TODO: warn here. add logrus to get log levels
		log.Printf("failed syncing data to disk: %v", err)
	}
}

func (w *WAL) Size() uint32 {
	return w.size
}

// TODO: Think about restore mechanism for WAL. When to perform? How would it work?
