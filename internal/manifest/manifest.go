package manifest

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/nbroyles/nbdb/internal/sstable"
	"github.com/nbroyles/nbdb/internal/util"
	log "github.com/sirupsen/logrus"
)

type Manifest struct {
	entries []*Entry
	writer  io.Writer
	codec   Codec
}

type Entry struct {
	// nolint: unused,structcheck
	metadata *sstable.Metadata
	deleted  bool
}

const (
	manifestPrefix = "manifest"
	uint32size     = 4
)

func NewManifest(writer io.Writer) *Manifest {
	return &Manifest{writer: writer}
}

func CreateManifestFile(dbName string, dataDir string) *os.File {
	return util.CreateFile(fmt.Sprintf("%s_%s_%d", manifestPrefix, dbName, time.Now().UnixNano()/1_000_000_000),
		dbName, dataDir)
}

func LoadLatest(dbName string, dataDir string) *Manifest {
	search := path.Join(dataDir, dbName, fmt.Sprintf("%s_%s_*", manifestPrefix, dbName))
	matches, err := filepath.Glob(search)
	if err != nil {
		log.Panicf("error loading manifest file: %v", err)
	} else if len(matches) == 0 {
		log.Panicf("could not find manfiest files matching %s: %v", search, err)
	}

	sort.Strings(matches)
	latest := matches[len(matches)-1]

	file, err := os.Open(latest)
	if err != nil {
		log.Panicf("could not open latest manifest file: %v", err)
	}

	m := NewManifest(file)

	for {
		data := make([]byte, uint32size)
		if n, err := file.Read(data); err == io.EOF {
			break
		} else if n != len(data) {
			log.Panicf("failed to read expected amount of data from manifest."+
				" read=%d, expected=%d", n, len(data))
		} else if err != nil {
			log.Panicf("failed to read record: %v", err)
		}

		eLen := binary.BigEndian.Uint32(data)

		entryBytes := make([]byte, eLen)
		if n, err := file.Read(entryBytes); uint32(n) != eLen {
			log.Panicf("failed to read expected amount of entry data from manifest."+
				" read=%d, expected=%d", n, eLen)
		} else if err != nil {
			log.Panicf("failed to read record: %v", err)
		}

		entry, err := m.codec.DecodeEntry(entryBytes)
		if err != nil {
			log.Panicf("failure decoding manifest entry: %v", err)
		}

		m.entries = append(m.entries, entry)
	}

	return m
}

func (m *Manifest) AddEntry(entry *Entry) {
	m.entries = append(m.entries, entry)

	bytes, err := m.codec.EncodeEntry(entry)
	if err != nil {
		log.Panicf("failed encoding manifest entry %v: %v", entry, err)
	}

	if written, err := m.writer.Write(bytes); written < len(bytes) {
		log.Panicf("failed writing to manifest. wrote %d bytes, expected %d bytes", written, len(bytes))
	} else if err != nil {
		log.Panicf("failed writing to manifest: %v", err)
	}
}
