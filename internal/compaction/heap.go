package compaction

import (
	"github.com/aaw3/hyphadb/internal/sstable"
)

type HeapItem[K comparable, V any] struct {
	Pair         sstable.Pair[K, V]
	SSTableIndex int
}

type MinHeap[K comparable, V any] []*HeapItem[K, V]

func (h MinHeap[K, V]) Len() int {
	return len(h)
}

func (h MinHeap[K, V]) Less(i, j int) bool {
	// Compare keys as strings for simplicity
	keyI := any(h[i].Pair.Key).(string)
	keyJ := any(h[j].Pair.Key).(string)

	if keyI != keyJ {
		return keyI < keyJ
	}

	// if keys are equal, use newer SSTable to break the tie
	return h[i].SSTableIndex > h[j].SSTableIndex

}

func (h MinHeap[K, V]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MinHeap[K, V]) Push(x any) {
	*h = append(*h, x.(*HeapItem[K, V]))
}

func (h *MinHeap[K, V]) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // zero ref to prevent memory leak
	*h = old[0 : n-1]
	return item
}
