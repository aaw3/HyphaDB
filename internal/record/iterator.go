package record

type Iterator interface {
	Next() bool
	Record() Record
	Err() error
	Close() error
}
