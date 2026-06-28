package record

type Entry struct {
	Value   []byte
	Deleted bool
}

type Record struct {
	Key string
	Seq uint64
	Entry
}
