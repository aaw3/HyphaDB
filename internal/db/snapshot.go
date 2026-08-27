package db

import "errors"

var ErrSnapshotClosed = errors.New("snapshot is closed")

// Snapshot provides a consistent view of the database at one sequence.
// Snapshots and iterators must be closed when no longer needed.
type Snapshot struct {
	db       *DB
	sequence uint64
	closed   bool
}

// NewSnapshot captures the latest completed database sequence.
func (db *DB) NewSnapshot() (*Snapshot, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrClosed
	}

	sequence, err := db.currentSequenceLocked()
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{db: db, sequence: sequence}
	db.registerReaderLocked(snapshot.sequence)
	return snapshot, nil
}

func (s *Snapshot) Get(key string) ([]byte, error) {
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	if s.closed {
		return nil, ErrSnapshotClosed
	}
	if s.db.closed {
		return nil, ErrClosed
	}

	return s.db.getAt(key, s.sequence)
}

func (s *Snapshot) NewIterator(opts IteratorOptions) (*Iterator, error) {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	if s.closed {
		return nil, ErrSnapshotClosed
	}
	if s.db.closed {
		return nil, ErrClosed
	}

	return s.db.newIteratorLocked(opts, s.sequence)
}

func (s *Snapshot) ScanPrefix(prefix string) (*Iterator, error) {
	return s.NewIterator(IteratorOptions{
		Start: prefix,
		End:   PrefixEnd(prefix),
	})
}

func (s *Snapshot) Sequence() uint64 {
	return s.sequence
}

func (s *Snapshot) Close() error {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true
	s.db.unregisterReaderLocked(s.sequence)
	return nil
}
