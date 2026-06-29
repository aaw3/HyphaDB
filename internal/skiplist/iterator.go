package skiplist

import "github.com/aaw3/hyphadb/internal/record"

type Iterator struct {
	current *node
}

func (s *SkipList) Iterator() *Iterator {
	return &Iterator{
		current: s.head.next[0],
	}
}

func (it *Iterator) Valid() bool {
	return it.current != nil
}

func (it *Iterator) Next() {
	if it.Valid() {
		it.current = it.current.next[0]
	}
}

func (it *Iterator) Record() record.Record {
	return it.current.record
}

func (s *SkipList) Records() []record.Record {
	var records []record.Record

	it := s.Iterator()
	for it.Valid() {
		records = append(records, it.Record())
		it.Next()
	}

	return records
}
