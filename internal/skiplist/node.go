package skiplist

import "github.com/aaw3/hyphadb/internal/record"

type node struct {
	key    string
	record record.Record
	next   []*node
}
