package hyphadb

import (
	"context"
	"errors"
	"testing"
)

func TestStorageServiceCRUDAndPaginatedScan(t *testing.T) {
	database, err := Open(Options{
		DataDir:    t.TempDir(),
		Memtable:   MemtableOptions{MaxEntries: 100},
		Compaction: CompactionOptions{TableCountThreshold: 100},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	service := NewService(database)
	ctx := context.Background()
	for _, entry := range []struct {
		key   string
		value string
	}{
		{key: "apple", value: "red"},
		{key: "banana", value: "yellow"},
		{key: "cherry", value: "red"},
	} {
		if err := service.Put(ctx, PutRequest{
			Key:   entry.key,
			Value: []byte(entry.value),
			Sync:  true,
		}); err != nil {
			t.Fatalf("Put %q: %v", entry.key, err)
		}
	}

	response, err := service.Get(ctx, GetRequest{Key: "apple"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(response.Value) != "red" {
		t.Fatalf("apple = %q, want red", response.Value)
	}

	first, err := service.Scan(ctx, ScanRequest{Start: "apple", End: "zebra", Limit: 2})
	if err != nil {
		t.Fatalf("first Scan: %v", err)
	}
	if len(first.Entries) != 2 || first.NextPageToken == "" {
		t.Fatalf("first page = %+v, want two entries and continuation", first)
	}
	if first.Entries[0].Key != "apple" || first.Entries[1].Key != "banana" {
		t.Fatalf("first page keys = %+v, want apple and banana", first.Entries)
	}

	second, err := service.Scan(ctx, ScanRequest{
		End:       "zebra",
		Limit:     2,
		PageToken: first.NextPageToken,
	})
	if err != nil {
		t.Fatalf("second Scan: %v", err)
	}
	if len(second.Entries) != 1 || second.Entries[0].Key != "cherry" {
		t.Fatalf("second page = %+v, want cherry", second.Entries)
	}

	if err := service.Delete(ctx, DeleteRequest{Key: "banana", Sync: true}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := service.Get(ctx, GetRequest{Key: "banana"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted Get error = %v, want ErrNotFound", err)
	}
}

func TestStorageServiceRejectsCanceledRequest(t *testing.T) {
	database, err := Open(Options{
		DataDir:    t.TempDir(),
		Memtable:   MemtableOptions{MaxEntries: 100},
		Compaction: CompactionOptions{TableCountThreshold: 100},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	service := NewService(database)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Get(ctx, GetRequest{Key: "apple"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Get error = %v, want context.Canceled", err)
	}
}
