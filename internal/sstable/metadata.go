package sstable

type Metadata struct {
	Level    uint8
	Filename string
	StartKey []byte
	EndKey   []byte
}
