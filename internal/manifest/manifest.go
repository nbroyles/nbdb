package manifest

import (
	"io"

	"github.com/nbroyles/nbdb/internal/sstable"
	log "github.com/sirupsen/logrus"
)

type Manifest struct {
	entries []*Entry
	writer  io.Writer
	codec   Codec
}

type Entry struct {
	metadata *sstable.Metadata
	deleted  bool
}

func NewManifest(writer io.Writer) *Manifest {
	return &Manifest{writer: writer}
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
