package record

type BatchKind uint8

const (
	BatchNone BatchKind = iota
	BatchBegin
	BatchOperation
	BatchCommit
)

type Entry struct {
	Value   []byte
	Deleted bool
}

type Record struct {
	Key string
	Seq uint64
	Entry
	BatchID   uint64
	BatchKind BatchKind
}
