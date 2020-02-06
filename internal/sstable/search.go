package sstable

import (
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/nbroyles/nbdb/internal/storage"
)

// Search searches for a key in the provided io. If key not found, then
// returns nil
func Search(key []byte, readSeeker io.ReadSeeker) ([]byte, error) {
	// Seek to footer start
	_, err := readSeeker.Seek(-footerLen, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("could not seek to footer in sstable: %w", err)
	}

	sCodec := storage.Codec{}
	footer, err := sCodec.DecodeFooter(readSeeker)
	if err != nil {
		return nil, fmt.Errorf("failed to decode footer from sstable. %w", err)
	}

	// Seek to index start
	_, err = readSeeker.Seek(int64(footer.IndexStartByte), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("could not seek to index portion of sstable: %w", err)
	}

	startPtr, err := sCodec.DecodePointer(readSeeker)
	if err != nil {
		return nil, fmt.Errorf("failed to decode index from sstable. %w", err)
	}

	// Iterate until we've found an entry point in list of indices where our key is > index start key
	for i := 1; i < int(footer.IndexEntries) && bytes.Compare(startPtr.Key, key) > 0; i++ {
		startPtr, err = sCodec.DecodePointer(readSeeker)
		if err != nil {
			return nil, fmt.Errorf("failed to decode index from sstable. %w", err)
		}
	}

	if bytes.Compare(startPtr.Key, key) > 0 {
		log.Panicf("failed to find an appropriate index in sstable for key %s. this should not happen!",
			string(key))
	}

	// Seek to entry point specified by index block
	_, err = readSeeker.Seek(int64(startPtr.StartByte), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("could not seek to key-value portion of sstable: %w", err)
	}

	for i := 0; i < indexCount; i++ {
		curPos, err := readSeeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("failure getting current position in file: %w", err)
		} else if curPos == int64(footer.IndexStartByte) {
			// We searched the last set of records and ended up at the index section of the file, so we didn't find the
			// key
			return nil, nil
		}

		record, err := sCodec.DecodeFromReader(readSeeker)
		if err != nil {
			return nil, fmt.Errorf("failed decoding record in sstable: %w", err)
		}

		if bytes.Equal(record.Key, key) {
			if record.Type == storage.RecordDelete {
				return nil, nil
			} else {
				return record.Value, nil
			}
		}
	}

	return nil, nil
}
