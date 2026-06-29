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
		// iterate until we find a node with a greater key or reach the end of the list
		for x.next[i] != nil && x.next[i].key < rec.Key {
			x = x.next[i]
		}
		update[i] = x
	}

	// move to bottom level to check if the key already exists
	x = x.next[0]

	// if key exists, update the record
	if x != nil && x.key == rec.Key {
		x.record = rec
		return
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
}

func (s *SkipList) Get(key string) (record.Record, bool) {
	x := s.head

	// search for the key starting from the top level
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}

	// move to the bottom level to check if the key exists, if it does, return the record
	x = x.next[0]
	if x != nil && x.key == key {
		return x.record, true
	}
	return record.Record{}, false
}
