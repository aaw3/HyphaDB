package record

type Iterator interface {
	Next() bool
	Record() Record
	Err() error
	Close() error
}

type SeekableIterator interface {
	Iterator

	// Seek positions the iterator so the next call to Next returns the first
	// record whose key is greater than or equal to key.
	Seek(key string) error
}
