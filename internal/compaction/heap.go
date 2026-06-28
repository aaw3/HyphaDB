package compaction

import "github.com/aaw3/hyphadb/internal/record"

type HeapItem struct {
	Record       record.Record
	SSTableIndex int
}

type MinHeap []*HeapItem

func (h MinHeap) Len() int {
	return len(h)
}

func (h MinHeap) Less(i, j int) bool {
	// Compare keys as strings for simplicity
	keyI := any(h[i].Record.Key).(string)
	keyJ := any(h[j].Record.Key).(string)

	if keyI != keyJ {
		return keyI < keyJ
	}

	// if keys are equal, use newer SSTable to break the tie
	return h[i].SSTableIndex > h[j].SSTableIndex

}

func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *MinHeap) Push(x any) {
	*h = append(*h, x.(*HeapItem))
}

func (h *MinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // zero ref to prevent memory leak
	*h = old[0 : n-1]
	return item
}
