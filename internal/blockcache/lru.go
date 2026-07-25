package blockcache

import (
	"container/list"
	"sync"
)

type entry struct {
	key   Key
	block []byte
	size  int
}

type LRU struct {
	mu sync.RWMutex

	capacityBytes int
	usedBytes     int

	entries map[Key]*list.Element
	order   *list.List
}

func NewLRU(capacityBytes int) *LRU {
	if capacityBytes <= 0 {
		capacityBytes = 0
	}

	return &LRU{
		capacityBytes: capacityBytes,
		entries:       make(map[Key]*list.Element),
		order:         list.New(),
	}
}

func (c *LRU) Get(key Key) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		return nil, false
	}

	c.order.MoveToFront(elem)
	item := elem.Value.(*entry)
	return item.block, true
}

func (c *LRU) Set(key Key, block []byte) {
	if len(block) == 0 {
		return
	}

	size := len(block)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Skip trying to cache blocks that are larger than the cache capacity
	if c.capacityBytes == 0 || size > c.capacityBytes {
		c.deleteLocked(key)
		return
	}

	if elem, ok := c.entries[key]; ok {
		item := elem.Value.(*entry)

		c.usedBytes -= item.size

		item.block = block
		item.size = size

		c.usedBytes += size
		c.order.MoveToFront(elem)
	} else {
		item := &entry{
			key:   key,
			block: block,
			size:  size,
		}

		elem := c.order.PushFront(item)
		c.entries[key] = elem
		c.usedBytes += size
	}

	c.evictLocked()
}

func (c *LRU) Delete(key Key) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.deleteLocked(key)
}

func (c *LRU) PurgeTable(tableID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, elem := range c.entries {
		if key.TableID != tableID {
			continue
		}

		item := elem.Value.(*entry)

		c.usedBytes -= item.size
		c.order.Remove(elem)
		delete(c.entries, key)
	}
}

func (c *LRU) deleteLocked(key Key) {
	elem, ok := c.entries[key]
	if !ok {
		return
	}

	item := elem.Value.(*entry)

	c.usedBytes -= item.size
	c.order.Remove(elem)
	delete(c.entries, key)
}

func (c *LRU) evictLocked() {
	for c.usedBytes > c.capacityBytes {
		elem := c.order.Back()
		if elem == nil {
			return
		}

		item := elem.Value.(*entry)

		c.usedBytes -= item.size
		c.order.Remove(elem)
		delete(c.entries, item.key)
	}
}

func (c *LRU) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.entries)
}

func (c *LRU) UsedBytes() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.usedBytes
}

func (c *LRU) CapacityBytes() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.capacityBytes
}
