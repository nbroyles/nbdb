package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/nbroyles/nbdb/internal/memtable"
	"github.com/nbroyles/nbdb/internal/storage"
	"github.com/nbroyles/nbdb/internal/util"
)

// WAL is the structure representing the writeahead log. All updates (incl. deletes)
// are first written to the writeahead log before being stored anywhere else (i.e. memtable,
//  sstables). This ensures that upon crash, memtable that was in memory can be regenerated
// from the writeahead log
type WAL struct {
	codec   storage.Codec
	logFile *os.File
	size    uint32
}

const (
	walPrefix  = "wal"
	uint32size = 4
)

// New creates a new writeahead log and returns a reference to it
func New(file *os.File) *WAL {
	return &WAL{codec: storage.Codec{}, logFile: file, size: 0}
}

func CreateFile(dbName string, dataDir string) (*os.File, error) {
	return util.CreateFile(fmt.Sprintf("%s_%s_%d", walPrefix, dbName, time.Now().UnixNano()/1_000_000_000),
		dbName, dataDir)
}

// FindExisting returns true and the WAL filename if an existing WAL is fine. Otherwise, returns false
func FindExisting(dbName string, dataDir string) (bool, *WAL, error) {
	search := path.Join(dataDir, dbName, fmt.Sprintf("%s_%s_*", walPrefix, dbName))
	matches, err := filepath.Glob(search)
	if err != nil {
		return false, nil, fmt.Errorf("error loading WAL file: %w", err)
	} else if len(matches) == 0 {
		return false, nil, nil
	} else if len(matches) > 1 {
		return false, nil, fmt.Errorf("multiple WAL files detected: %v", matches)
	}

	file, err := os.OpenFile(matches[0], os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return false, nil, fmt.Errorf("error opening existing WAL file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return false, nil, fmt.Errorf("error retrieving file info for WAL: %w", err)
	}

	wal := New(file)
	wal.size = uint32(info.Size())

	return true, wal, nil
}

// Write writes the record to the writeahead log
func (w *WAL) Write(record *storage.Record) error {
	data, err := w.codec.Encode(record)
	if err != nil {
		return fmt.Errorf("failed encoding data to write to log: %w", err)
	}

	if n, err := w.logFile.Write(data); n != len(data) {
		return fmt.Errorf("failed to write entirety of data to log, bytes written=%d, expected=%d, err=%w",
			n, len(data), err)
	} else if err != nil {
		return fmt.Errorf("failed to write data to log: %w", err)
	}

	// update current size of WAL
	w.size += uint32(len(data))

	if err := w.logFile.Sync(); err != nil {
		return fmt.Errorf("failed syncing data to disk: %w", err)
	}

	return nil
}

func (w *WAL) Size() uint32 {
	return w.size
}

func (w *WAL) Restore(mem *memtable.MemTable) error {
	for {
		data := make([]byte, uint32size)
		if n, err := w.logFile.Read(data); err == io.EOF {
			break
		} else if n != len(data) {
			return fmt.Errorf("failed to read expected amount of data from WAL."+
				" read=%d, expected=%d", n, len(data))
		} else if err != nil {
			return fmt.Errorf("failed to read record: %w", err)
		}

		rLen := binary.BigEndian.Uint32(data)

		recBytes := make([]byte, rLen)
		if n, err := w.logFile.Read(recBytes); uint32(n) != rLen {
			return fmt.Errorf("failed to read expected amount of record data from WAL."+
				" read=%d, expected=%d", n, rLen)
		} else if err != nil {
			return fmt.Errorf("failed to read record: %w", err)
		}

		record, err := w.codec.Decode(recBytes)
		if err != nil {
			return fmt.Errorf("failed to decoding record: %w", err)
		}

		if record.Type == storage.RecordUpdate {
			mem.Put(record.Key, record.Value)
		} else {
			mem.Delete(record.Key)
		}
	}

	return nil
}

func (w *WAL) Close() error {
	if err := w.logFile.Close(); err != nil {
		return fmt.Errorf("failed attempting to close WAL log file: %w", err)
	}

	// TODO: if this fails, the log file is closed and future calls to Close will error
	// on the os.File#Close call. Could leave an old WAL around
	if err := os.Remove(w.logFile.Name()); err != nil {
		w.logFile.Close()
		return fmt.Errorf("failed attempting to remove WAL file: %w", err)
	}

	return nil
}
