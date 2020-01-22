package storage

type RecordType int8

const (
	RecordUpdate RecordType = iota // indicates that this record was an update
	RecordDelete                   // indicates that this record was a delete
)

// Record is an in-memory representation of an update on the datastore
type Record struct {
	Key   []byte
	Value []byte
	Type  RecordType
}

// RecordPointer is a pointer to a Record on disk
type RecordPointer struct {
	StartByte uint32
	Length    uint32
}

func NewRecord(key []byte, value []byte, delete bool) *Record {
	var rType RecordType
	if delete {
		rType = RecordDelete
	} else {
		rType = RecordUpdate
	}

	return &Record{
		Key:   key,
		Value: value,
		Type:  rType,
	}
}
