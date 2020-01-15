package wal

type RecordType int8

const (
	recordUpdate RecordType = iota // indicates that this record was an update
	// nolint
	recordDelete // indicates that this record was a delete
)

// Record represents and entry in the WAL
type Record struct {
	key   []byte
	value []byte
	rType RecordType
}

func NewRecord(key []byte, value []byte, delete bool) *Record {
	var rType RecordType
	if delete {
		rType = recordDelete
	} else {
		rType = recordUpdate
	}

	return &Record{
		key:   key,
		value: value,
		rType: rType,
	}
}
