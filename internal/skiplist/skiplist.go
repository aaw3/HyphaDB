package skiplist

import (
	"math/rand"
	"time"

	"github.com/aaw3/hyphadb/internal/record"
)

type SkipList struct {
	head     *node
	level    int
	maxLevel int
	rng      *rand.Rand // use a per-instance rng for better randomness
	count    int
	keyCount int
}

const defaultMaxLevel = 16

func New() *SkipList {
	return NewWithMaxLevel(defaultMaxLevel)
}

func NewWithMaxLevel(maxLevel int) *SkipList {
	if maxLevel <= 0 {
		panic("skiplist: maxLevel must be greater than 0")
	}

	return &SkipList{
		head: &node{
			next: make([]*node, maxLevel),
		},
		level:    1,
		maxLevel: maxLevel,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func NewWithMaxLevelAndSeed(maxLevel int, seed int64) *SkipList {
	if maxLevel <= 0 {
		panic("skiplist: maxLevel must be greater than 0")
	}

	return &SkipList{
		head: &node{
			next: make([]*node, maxLevel),
		},
		level:    1,
		maxLevel: maxLevel,
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (s *SkipList) randomLevel() int {
	level := 1

	// Use the instance rng and use bitwise AND for p = 0.5 probability
	// randomness modeled as (1/2)^level
	for level < s.maxLevel && s.rng.Int31()&1 == 0 {
		level++
	}

	return level
}

func (s *SkipList) Put(rec record.Record) {
	update := make([]*node, s.maxLevel)
	x := s.head

	// find the position to insert the new node
	for i := s.level - 1; i >= 0; i-- {
		// Records are ordered by key ascending and sequence descending.
		for x.next[i] != nil && less(x.next[i].record, rec) {
			x = x.next[i]
		}
		update[i] = x
	}
	keyExists := (x != s.head && x.key == rec.Key) ||
		(x.next[0] != nil && x.next[0].key == rec.Key)

	// move to bottom level to check if the key already exists
	x = x.next[0]

	// An identical key and sequence is a replacement, not a new version.
	if x != nil && x.key == rec.Key && x.record.Seq == rec.Seq {
		x.record = rec
		return
	}
	if !keyExists {
		s.keyCount++
	}

	newLevel := s.randomLevel()

	// if new level is taller than current level, initialize update for the new levels
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			update[i] = s.head
		}
		// update the level of the skip list
		s.level = newLevel
	}

	// update the skip list's active height
	newNode := &node{
		key:    rec.Key,
		record: rec,
		next:   make([]*node, newLevel),
	}

	// insert the new node into each level of the skip list
	for i := 0; i < newLevel; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	s.count++
}

func (s *SkipList) Get(key string) (record.Record, bool) {
	x := s.lowerBound(key)
	if x != nil && x.key == key {
		return x.record, true
	}
	return record.Record{}, false
}

// GetAt returns the newest version of key visible at maxSeq.
func (s *SkipList) GetAt(key string, maxSeq uint64) (record.Record, bool) {
	for x := s.lowerBound(key); x != nil && x.key == key; x = x.next[0] {
		if x.record.Seq <= maxSeq {
			return x.record, true
		}
	}

	return record.Record{}, false
}

func (s *SkipList) Len() int {
	return s.keyCount
}

func (s *SkipList) lowerBound(key string) *node {
	x := s.head

	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}

	return x.next[0]
}

func less(a, b record.Record) bool {
	if a.Key != b.Key {
		return a.Key < b.Key
	}

	return a.Seq > b.Seq
}
