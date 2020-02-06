package pkg

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/nbroyles/nbdb/internal/manifest"
	"github.com/nbroyles/nbdb/internal/memtable"
	"github.com/nbroyles/nbdb/internal/sstable"
	"github.com/nbroyles/nbdb/internal/storage"
	"github.com/nbroyles/nbdb/internal/wal"
	log "github.com/sirupsen/logrus"
)

// TODO: copy keys and values passed as arguments
// TODO: check key and value size and fail if > threshold

// DB represents the API for database access
// One process can have a database open at a time
// Calls to Get, Put, Delete are thread-safe
type DB struct {
	name    string
	dataDir string

	mutex    sync.RWMutex
	memTable *memtable.MemTable
	walog    *wal.WAL
	manifest *manifest.Manifest

	compactingMemTable *memtable.MemTable
	compactingWAL      *wal.WAL
	compact            chan bool
	stopWatching       chan bool
	mtSizeLimit        uint32
}

// TODO: allow configuration via options provided to constructor
const (
	// Makes sense on Mac OS X, may not elsewhere
	datadir  = "/usr/local/var/nbdb"
	lockFile = "__DB_LOCK__"
	// Limit memtable to 4 MBs before flushing
	mtSizeLimit = uint32(4194304)
)

type DBOpts struct {
	dataDir     string
	mtSizeLimit uint32
}

func (o *DBOpts) applyDefaults() {
	if o.dataDir == "" {
		o.dataDir = datadir
	}

	if o.mtSizeLimit == 0 {
		o.mtSizeLimit = mtSizeLimit
	}
}

// New creates a new database based on the name provided.
// New fails if the database already exists
func New(name string, opts DBOpts) (*DB, error) {
	opts.applyDefaults()

	if err := os.MkdirAll(opts.dataDir, 0755); err != nil {
		return nil, fmt.Errorf("could not create data dir %s: %w", opts.dataDir, err)
	}

	dbPath := path.Join(opts.dataDir, name)

	if exists, err := exists(name, opts.dataDir); !exists {
		if err := os.Mkdir(dbPath, 0755); err != nil {
			return nil, fmt.Errorf("failed creating data directory for database %s: %w", name, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("could not create new database: %w", err)
	} else {
		return nil, fmt.Errorf("database %s already exists. use DB#Open instead", name)
	}

	return Open(name, opts)
}

func lock(name string, dataDir string) error {
	pid := os.Getpid()
	lockPath := path.Join(dataDir, name, lockFile)

	lock, err := os.Open(lockPath)
	// Database is not currently locked, attempt to acquire
	if os.IsNotExist(err) {
		if lockFile, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666); os.IsExist(err) {
			return fmt.Errorf("cannot lock database. already locked by another process")
		} else if err != nil {
			return fmt.Errorf("failure attempting to lock database: %w", err)
		} else {
			pidBytes := []byte(strconv.Itoa(pid))
			if n, err := lockFile.Write(pidBytes); n < len(pidBytes) {
				return fmt.Errorf("failure writing owner pid to lock file. wrote %d bytes, expected %d",
					n, len(pidBytes))
			} else if err != nil {
				return fmt.Errorf("failure writing owner pid to lock file: %w", err)
			}
			return nil
		}
	} else if err != nil {
		return fmt.Errorf("failure attempting to lock database: %w", err)
	} else {
		// Database currently locked, see if it's me
		scanner := bufio.NewScanner(lock)
		scanner.Scan()
		lockPid, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return fmt.Errorf("failed attempting to read lockfile: %w", err)
		}

		if lockPid == pid {
			return nil
		} else {
			return fmt.Errorf("cannot lock database. already locked by another process (%d)", lockPid)
		}
	}
}

// Open opens a database of the name provided. Open fails
// if the database does not exist
func Open(name string, opts DBOpts) (*DB, error) {
	opts.applyDefaults()

	if exists, err := exists(name, opts.dataDir); !exists {
		if err == nil {
			return nil, fmt.Errorf("failed opening database %s. does not exist", name)
		} else {
			return nil, fmt.Errorf("failed opening database %s: %v", name, err)
		}
	}

	if err := lock(name, opts.dataDir); err != nil {
		return nil, fmt.Errorf("could not lock database: %w", err)
	}

	mem := memtable.New()

	// Attempt to load WAL if exists. Otherwise create a new one
	found, walog, err := wal.FindExisting(name, opts.dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed attempting to look for existing WAL file: %w", err)
	}

	if !found {
		waf, err := wal.CreateFile(name, opts.dataDir)
		if err != nil {
			return nil, fmt.Errorf("could not create WAL file: %w", err)
		}
		walog = wal.New(waf)
	} else {
		if err = walog.Restore(mem); err != nil {
			return nil, fmt.Errorf("failed attempting to restore WAL: %w", err)
		}
	}

	found, man, err := manifest.LoadLatest(name, opts.dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed attempting to load manifest file: %w", err)
	} else if !found {
		maf, err := manifest.CreateManifestFile(name, opts.dataDir)
		if err != nil {
			return nil, fmt.Errorf("could not create manifest file: %w", err)
		}
		man = manifest.NewManifest(maf)
	}

	db := &DB{
		memTable:     mem,
		walog:        walog,
		manifest:     man,
		name:         name,
		dataDir:      opts.dataDir,
		compact:      make(chan bool, 1),
		stopWatching: make(chan bool),
		mtSizeLimit:  opts.mtSizeLimit,
	}

	go db.compactionWatcher()

	return db, nil
}

func exists(name string, datadir string) (bool, error) {
	dbPath := path.Join(datadir, name)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failure checking to see if database already exists: %w", err)
	} else {
		return true, nil
	}
}

// OpenOrNew opens the DB if it exists or creates it if it doesn't
func OpenOrNew(name string, opts DBOpts) (*DB, error) {
	opts.applyDefaults()

	dbExists, err := exists(name, opts.dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed checking if database %s already exists: %v", name, err)
	}

	if dbExists {
		return Open(name, opts)
	} else {
		return New(name, opts)
	}
}

// Close ensures that any resources used by the DB are tidied up
func (d *DB) Close() error {
	close(d.stopWatching)
	return d.unlock()
}

func (d *DB) unlock() error {
	lockPath := path.Join(d.dataDir, d.name, lockFile)
	return os.Remove(lockPath)
}

// Get returns the value associated with the key. If key is not found then
// the value returned is nil
func (d *DB) Get(key []byte) ([]byte, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	val := d.memTable.Get(key)
	if val == nil && d.compactingMemTable != nil {
		val = d.compactingMemTable.Get(key)
	}

	// TODO: add a bloom filter to reduce need to potentially check every level
	// TODO: can we unlock during this search? issue to solve is sstables getting compacted while searching
	if val == nil {
		// 255 == uint8 max == max number of levels based on value used for encoding level information on disk
	levelTraversal:
		for i := 0; i < 255; i++ {
			for _, meta := range d.manifest.MetadataForLevel(i) {
				if meta == nil {
					break levelTraversal
				}
				if meta.ContainsKey(key) {
					val, err := d.searchSSTable(key, meta)
					if err != nil {
						return nil, fmt.Errorf("failed attempting to scan sstable for key %s: %w", string(key), err)
					}

					if val != nil {
						return val, nil
					}
				}
			}
		}
	}

	return val, nil
}

func (d *DB) searchSSTable(key []byte, meta *sstable.Metadata) ([]byte, error) {
	// TODO: cache this instead of opening and closing every time
	sstHandle, err := os.Open(path.Join(d.dataDir, d.name, meta.Filename))
	if err != nil {
		return nil, fmt.Errorf("failed attempting to open sstable for reading: %w", err)
	}
	defer sstHandle.Close()

	return sstable.Search(key, sstHandle)
}

// Put inserts or updates the value if the key already exists
func (d *DB) Put(key []byte, value []byte) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if err := d.walog.Write(storage.NewRecord(key, value, false)); err != nil {
		return fmt.Errorf("failed attempting write put to WAL: %w", err)
	}

	d.memTable.Put(key, value)

	// compactingMemTable not being nil indicating that a compaction is already underway
	if d.memTable.Size() > d.mtSizeLimit && d.compactingMemTable == nil {
		d.compactingMemTable = d.memTable
		d.compactingWAL = d.walog

		d.memTable = memtable.New()

		waf, err := wal.CreateFile(d.name, d.dataDir)
		if err != nil {
			// Abort compaction attempt
			d.memTable = d.compactingMemTable
			d.walog = d.compactingWAL

			d.compactingMemTable = nil
			d.compactingWAL = nil

			return fmt.Errorf("could not create WAL file: %w", err)
		}
		d.walog = wal.New(waf)

		d.compact <- true
	}

	return nil
}

// Deletes the specified key from the data store
func (d *DB) Delete(key []byte) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if err := d.walog.Write(storage.NewRecord(key, nil, true)); err != nil {
		return fmt.Errorf("failed attempting write delete to WAL: %w", err)
	}
	d.memTable.Delete(key)

	return nil
}

func (d *DB) compactionWatcher() {
	for {
		select {
		case <-d.compact:
			if err := d.doCompaction(); err != nil {
				log.Errorf("error performing compaction: %v", err)
			}
		case <-d.stopWatching:
			return
		}
	}
}

func (d *DB) flushMemTable(tableName string, writer io.Writer) error {
	iter := d.compactingMemTable.InternalIterator()

	builder := sstable.NewBuilder(tableName, iter, writer)
	metadata, err := builder.WriteLevel0Table()
	if err != nil {
		return fmt.Errorf("could not write memtable to level 0 sstable: %w", err)
	}

	return d.manifest.AddEntry(manifest.NewEntry(metadata, false))
}

func (d *DB) doCompaction() error {
	file, err := sstable.CreateFile(d.name, d.dataDir)
	if err != nil {
		return fmt.Errorf("failed attempt to create new sstable file: %w", err)
	}
	defer file.Close()

	err = d.flushMemTable(filepath.Base(file.Name()), file)

	if err == nil {
		if err = file.Sync(); err != nil {
			return fmt.Errorf("error flushing sstable to disk: %w", err)
		}

		d.mutex.Lock()
		defer d.mutex.Unlock()

		if err = d.compactingWAL.Close(); err != nil {
			return fmt.Errorf("failed attempt to close WAL: %w", err)
		}

		d.compactingMemTable = nil
		d.compactingWAL = nil
	}

	return nil
}
