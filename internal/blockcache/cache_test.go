package blockcache

import (
	"bytes"
	"sync"
	"testing"
)

func TestLRUSetAndGet(t *testing.T) {
	cache := NewLRU(1024)

	key := Key{
		TableID: 1,
		Offset:  100,
	}
	want := []byte("block data")

	cache.Set(key, want)

	got, ok := cache.Get(key)
	if !ok {
		t.Fatal("expected cache hit")
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("Get() = %q, want %q", got, want)
	}

	if cache.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", cache.Len())
	}

	if cache.UsedBytes() != len(want) {
		t.Fatalf(
			"UsedBytes() = %d, want %d",
			cache.UsedBytes(),
			len(want),
		)
	}
}

func TestLRUGetMissingKey(t *testing.T) {
	cache := NewLRU(1024)

	got, ok := cache.Get(Key{
		TableID: 1,
		Offset:  100,
	})

	if ok {
		t.Fatal("expected cache miss")
	}

	if got != nil {
		t.Fatalf("Get() = %v, want nil", got)
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	cache := NewLRU(6)

	keyA := Key{TableID: 1, Offset: 0}
	keyB := Key{TableID: 1, Offset: 10}
	keyC := Key{TableID: 1, Offset: 20}

	cache.Set(keyA, []byte("aaa"))
	cache.Set(keyB, []byte("bbb"))

	// A becomes most recently used, making B least recently used.
	if _, ok := cache.Get(keyA); !ok {
		t.Fatal("expected key A to exist")
	}

	cache.Set(keyC, []byte("ccc"))

	if _, ok := cache.Get(keyB); ok {
		t.Fatal("expected key B to be evicted")
	}

	if _, ok := cache.Get(keyA); !ok {
		t.Fatal("expected key A to remain")
	}

	if _, ok := cache.Get(keyC); !ok {
		t.Fatal("expected key C to remain")
	}

	if cache.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", cache.Len())
	}

	if cache.UsedBytes() != 6 {
		t.Fatalf("UsedBytes() = %d, want 6", cache.UsedBytes())
	}
}

func TestLRUGetRefreshesRecency(t *testing.T) {
	cache := NewLRU(4)

	keyA := Key{TableID: 1, Offset: 0}
	keyB := Key{TableID: 1, Offset: 10}
	keyC := Key{TableID: 1, Offset: 20}

	cache.Set(keyA, []byte("aa"))
	cache.Set(keyB, []byte("bb"))

	// A was initially least recently used, but this refreshes it.
	if _, ok := cache.Get(keyA); !ok {
		t.Fatal("expected key A to exist")
	}

	cache.Set(keyC, []byte("cc"))

	if _, ok := cache.Get(keyB); ok {
		t.Fatal("expected key B to be evicted")
	}

	if _, ok := cache.Get(keyA); !ok {
		t.Fatal("expected refreshed key A to remain")
	}
}

func TestLRUReplaceExistingEntry(t *testing.T) {
	cache := NewLRU(10)

	key := Key{TableID: 1, Offset: 0}

	cache.Set(key, []byte("abc"))
	cache.Set(key, []byte("12345"))

	got, ok := cache.Get(key)
	if !ok {
		t.Fatal("expected cache hit")
	}

	if !bytes.Equal(got, []byte("12345")) {
		t.Fatalf("Get() = %q, want %q", got, "12345")
	}

	if cache.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", cache.Len())
	}

	if cache.UsedBytes() != 5 {
		t.Fatalf("UsedBytes() = %d, want 5", cache.UsedBytes())
	}
}

func TestLRUReplacementCanCauseEviction(t *testing.T) {
	cache := NewLRU(6)

	keyA := Key{TableID: 1, Offset: 0}
	keyB := Key{TableID: 1, Offset: 10}

	cache.Set(keyA, []byte("aa"))
	cache.Set(keyB, []byte("bb"))

	// B is most recently used. Replacing it also keeps it most recent.
	cache.Set(keyB, []byte("bbbbb"))

	if _, ok := cache.Get(keyA); ok {
		t.Fatal("expected key A to be evicted")
	}

	got, ok := cache.Get(keyB)
	if !ok {
		t.Fatal("expected key B to remain")
	}

	if !bytes.Equal(got, []byte("bbbbb")) {
		t.Fatalf("Get() = %q, want %q", got, "bbbbb")
	}

	if cache.UsedBytes() != 5 {
		t.Fatalf("UsedBytes() = %d, want 5", cache.UsedBytes())
	}
}

func TestLRURejectsOversizedEntry(t *testing.T) {
	cache := NewLRU(4)

	key := Key{TableID: 1, Offset: 0}
	cache.Set(key, []byte("12345"))

	if _, ok := cache.Get(key); ok {
		t.Fatal("oversized entry should not be cached")
	}

	if cache.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", cache.Len())
	}

	if cache.UsedBytes() != 0 {
		t.Fatalf("UsedBytes() = %d, want 0", cache.UsedBytes())
	}
}

func TestLRUOversizedReplacementRemovesExistingEntry(t *testing.T) {
	cache := NewLRU(4)

	key := Key{TableID: 1, Offset: 0}

	cache.Set(key, []byte("abc"))
	cache.Set(key, []byte("12345"))

	if _, ok := cache.Get(key); ok {
		t.Fatal("expected existing entry to be removed")
	}

	if cache.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", cache.Len())
	}

	if cache.UsedBytes() != 0 {
		t.Fatalf("UsedBytes() = %d, want 0", cache.UsedBytes())
	}
}

func TestLRUZeroCapacityDisablesCaching(t *testing.T) {
	cache := NewLRU(0)

	key := Key{TableID: 1, Offset: 0}
	cache.Set(key, []byte("data"))

	if _, ok := cache.Get(key); ok {
		t.Fatal("zero-capacity cache should not store entries")
	}

	if cache.CapacityBytes() != 0 {
		t.Fatalf(
			"CapacityBytes() = %d, want 0",
			cache.CapacityBytes(),
		)
	}
}

func TestLRUNegativeCapacityBecomesZero(t *testing.T) {
	cache := NewLRU(-1)

	if cache.CapacityBytes() != 0 {
		t.Fatalf(
			"CapacityBytes() = %d, want 0",
			cache.CapacityBytes(),
		)
	}
}

func TestLRUDoesNotCacheEmptyBlock(t *testing.T) {
	cache := NewLRU(1024)

	key := Key{TableID: 1, Offset: 0}
	cache.Set(key, nil)

	if _, ok := cache.Get(key); ok {
		t.Fatal("empty block should not be cached")
	}

	cache.Set(key, []byte{})

	if _, ok := cache.Get(key); ok {
		t.Fatal("zero-length block should not be cached")
	}
}

func TestLRUDelete(t *testing.T) {
	cache := NewLRU(1024)

	key := Key{TableID: 1, Offset: 0}
	cache.Set(key, []byte("data"))

	cache.Delete(key)

	if _, ok := cache.Get(key); ok {
		t.Fatal("deleted key should not remain cached")
	}

	if cache.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", cache.Len())
	}

	if cache.UsedBytes() != 0 {
		t.Fatalf("UsedBytes() = %d, want 0", cache.UsedBytes())
	}
}

func TestLRUDeleteMissingKey(t *testing.T) {
	cache := NewLRU(1024)

	cache.Delete(Key{
		TableID: 1,
		Offset:  0,
	})

	if cache.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", cache.Len())
	}

	if cache.UsedBytes() != 0 {
		t.Fatalf("UsedBytes() = %d, want 0", cache.UsedBytes())
	}
}

func TestLRUPurgeTable(t *testing.T) {
	cache := NewLRU(1024)

	tableOneA := Key{TableID: 1, Offset: 0}
	tableOneB := Key{TableID: 1, Offset: 100}
	tableTwo := Key{TableID: 2, Offset: 0}

	cache.Set(tableOneA, []byte("aaa"))
	cache.Set(tableOneB, []byte("bbbb"))
	cache.Set(tableTwo, []byte("cc"))

	cache.PurgeTable(1)

	if _, ok := cache.Get(tableOneA); ok {
		t.Fatal("expected first table-one block to be purged")
	}

	if _, ok := cache.Get(tableOneB); ok {
		t.Fatal("expected second table-one block to be purged")
	}

	got, ok := cache.Get(tableTwo)
	if !ok {
		t.Fatal("expected table-two block to remain")
	}

	if !bytes.Equal(got, []byte("cc")) {
		t.Fatalf("Get() = %q, want %q", got, "cc")
	}

	if cache.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", cache.Len())
	}

	if cache.UsedBytes() != 2 {
		t.Fatalf("UsedBytes() = %d, want 2", cache.UsedBytes())
	}
}

func TestLRUPurgeMissingTable(t *testing.T) {
	cache := NewLRU(1024)

	key := Key{TableID: 1, Offset: 0}
	cache.Set(key, []byte("data"))

	cache.PurgeTable(999)

	if _, ok := cache.Get(key); !ok {
		t.Fatal("unrelated entry should remain")
	}

	if cache.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", cache.Len())
	}
}

func TestLRUSeparatesEntriesByTableID(t *testing.T) {
	cache := NewLRU(1024)

	first := Key{
		TableID: 1,
		Offset:  100,
	}
	second := Key{
		TableID: 2,
		Offset:  100,
	}

	cache.Set(first, []byte("table-one"))
	cache.Set(second, []byte("table-two"))

	firstValue, ok := cache.Get(first)
	if !ok {
		t.Fatal("expected first entry")
	}

	secondValue, ok := cache.Get(second)
	if !ok {
		t.Fatal("expected second entry")
	}

	if !bytes.Equal(firstValue, []byte("table-one")) {
		t.Fatalf(
			"first value = %q, want %q",
			firstValue,
			"table-one",
		)
	}

	if !bytes.Equal(secondValue, []byte("table-two")) {
		t.Fatalf(
			"second value = %q, want %q",
			secondValue,
			"table-two",
		)
	}
}

func TestLRUSeparatesEntriesByOffset(t *testing.T) {
	cache := NewLRU(1024)

	first := Key{
		TableID: 1,
		Offset:  100,
	}
	second := Key{
		TableID: 1,
		Offset:  200,
	}

	cache.Set(first, []byte("first"))
	cache.Set(second, []byte("second"))

	firstValue, ok := cache.Get(first)
	if !ok {
		t.Fatal("expected first entry")
	}

	secondValue, ok := cache.Get(second)
	if !ok {
		t.Fatal("expected second entry")
	}

	if !bytes.Equal(firstValue, []byte("first")) {
		t.Fatalf("first value = %q, want %q", firstValue, "first")
	}

	if !bytes.Equal(secondValue, []byte("second")) {
		t.Fatalf("second value = %q, want %q", secondValue, "second")
	}
}

func TestLRUEvictsMultipleEntriesWhenNeeded(t *testing.T) {
	cache := NewLRU(10)

	keyA := Key{TableID: 1, Offset: 0}
	keyB := Key{TableID: 1, Offset: 10}
	keyC := Key{TableID: 1, Offset: 20}
	keyD := Key{TableID: 1, Offset: 30}

	cache.Set(keyA, []byte("aaa"))
	cache.Set(keyB, []byte("bbb"))
	cache.Set(keyC, []byte("ccc"))

	// Adding 8 bytes requires evicting all three existing 3-byte entries.
	cache.Set(keyD, []byte("12345678"))

	if _, ok := cache.Get(keyA); ok {
		t.Fatal("expected key A to be evicted")
	}

	if _, ok := cache.Get(keyB); ok {
		t.Fatal("expected key B to be evicted")
	}

	if _, ok := cache.Get(keyC); ok {
		t.Fatal("expected key C to be evicted")
	}

	if _, ok := cache.Get(keyD); !ok {
		t.Fatal("expected key D to remain")
	}

	if cache.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", cache.Len())
	}

	if cache.UsedBytes() != 8 {
		t.Fatalf("UsedBytes() = %d, want 8", cache.UsedBytes())
	}
}

func TestLRUConcurrentAccess(t *testing.T) {
	cache := NewLRU(64 * 1024)

	const goroutines = 16
	const operations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for worker := 0; worker < goroutines; worker++ {
		worker := worker

		go func() {
			defer wg.Done()

			for i := 0; i < operations; i++ {
				key := Key{
					TableID: uint64(worker % 4),
					Offset:  uint64(i % 128),
				}

				cache.Set(key, []byte{byte(worker), byte(i)})
				cache.Get(key)

				if i%25 == 0 {
					cache.Delete(key)
				}

				if i%200 == 0 {
					cache.PurgeTable(uint64((worker + 1) % 4))
				}
			}
		}()
	}

	wg.Wait()

	if cache.UsedBytes() < 0 {
		t.Fatalf(
			"UsedBytes() = %d, must not be negative",
			cache.UsedBytes(),
		)
	}

	if cache.UsedBytes() > cache.CapacityBytes() {
		t.Fatalf(
			"UsedBytes() = %d, exceeds capacity %d",
			cache.UsedBytes(),
			cache.CapacityBytes(),
		)
	}

	if cache.Len() < 0 {
		t.Fatalf("Len() = %d, must not be negative", cache.Len())
	}
}
