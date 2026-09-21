package hyphadb

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"

	hyphadbv1 "github.com/aaw3/hyphadb/gen/hyphadb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const grpcBufferSize = 1024 * 1024

func newBufconnClient(t *testing.T) (hyphadbv1.StorageServiceClient, func()) {
	t.Helper()

	db, err := Open(Options{
		DataDir: t.TempDir(),
		Memtable: MemtableOptions{
			MaxEntries: 100,
		},
		Compaction: CompactionOptions{
			TableCountThreshold: 100,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	service := NewService(db)
	listener := bufconn.Listen(grpcBufferSize)
	server := grpc.NewServer()
	hyphadbv1.RegisterStorageServiceServer(server, NewGRPCServer(service))

	go func() {
		if err := server.Serve(listener); err != nil {
			// Stop and Close intentionally make Serve return during cleanup.
		}
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(
		func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		},
	), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		_ = listener.Close()
		_ = db.Close()
		t.Fatalf("DialContext: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
		_ = db.Close()
	}
	return hyphadbv1.NewStorageServiceClient(conn), cleanup
}

func TestGRPCServerCRUDAndBatch(t *testing.T) {
	client, cleanup := newBufconnClient(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.Put(ctx, &hyphadbv1.PutRequest{
		Key:   "document/1",
		Value: []byte("body"),
		Sync:  true,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	response, err := client.Get(ctx, &hyphadbv1.GetRequest{Key: "document/1"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(response.GetValue()) != "body" {
		t.Fatalf("Get value = %q, want body", response.GetValue())
	}

	_, err = client.WriteBatch(ctx, &hyphadbv1.WriteBatchRequest{
		Mutations: []*hyphadbv1.Mutation{
			{Type: hyphadbv1.Mutation_PUT, Key: "index/body/1", Value: []byte("document/1")},
			{Type: hyphadbv1.Mutation_DELETE, Key: "document/1"},
		},
		Sync: true,
	})
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	response, err = client.Get(ctx, &hyphadbv1.GetRequest{Key: "index/body/1"})
	if err != nil {
		t.Fatalf("Get batch value: %v", err)
	}
	if string(response.GetValue()) != "document/1" {
		t.Fatalf("batch value = %q, want document/1", response.GetValue())
	}

	_, err = client.Get(ctx, &hyphadbv1.GetRequest{Key: "document/1"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("deleted Get code = %v, want %v", status.Code(err), codes.NotFound)
	}
}

func TestGRPCServerRejectsInvalidBatchWithoutApplyingOperations(t *testing.T) {
	client, cleanup := newBufconnClient(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.WriteBatch(ctx, &hyphadbv1.WriteBatchRequest{
		Mutations: []*hyphadbv1.Mutation{
			{Type: hyphadbv1.Mutation_PUT, Key: "partial", Value: []byte("must not persist")},
			{Key: "invalid"},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid batch code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}

	_, err = client.Get(ctx, &hyphadbv1.GetRequest{Key: "partial"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("partial operation code = %v, want %v", status.Code(err), codes.NotFound)
	}
}

func TestGRPCServerStreamsBoundedScan(t *testing.T) {
	client, cleanup := newBufconnClient(t)
	defer cleanup()
	ctx := context.Background()

	for _, entry := range []struct {
		key   string
		value string
	}{
		{key: "b", value: "2"},
		{key: "a", value: "1"},
		{key: "c", value: "3"},
	} {
		if _, err := client.Put(ctx, &hyphadbv1.PutRequest{Key: entry.key, Value: []byte(entry.value)}); err != nil {
			t.Fatalf("Put %q: %v", entry.key, err)
		}
	}

	stream, err := client.Scan(ctx, &hyphadbv1.ScanRequest{Start: "a", End: "d", Limit: 2})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	var entries []string
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Scan Recv: %v", err)
		}
		entries = append(entries, entry.GetKey()+"="+string(entry.GetValue()))
	}
	want := []string{"a=1", "b=2"}
	if len(entries) != len(want) {
		t.Fatalf("scan entries = %v, want %v", entries, want)
	}
	for i := range want {
		if entries[i] != want[i] {
			t.Errorf("scan entry %d = %q, want %q", i, entries[i], want[i])
		}
	}
}

func TestGRPCServerMapsCanceledRequests(t *testing.T) {
	client, cleanup := newBufconnClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Get(ctx, &hyphadbv1.GetRequest{Key: "key"})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("canceled Get code = %v, want %v", status.Code(err), codes.Canceled)
	}
}

func TestGRPCServerMapsInputLimitsToInvalidArgument(t *testing.T) {
	client, cleanup := newBufconnClient(t)
	defer cleanup()

	_, err := client.Put(context.Background(), &hyphadbv1.PutRequest{
		Key:   strings.Repeat("k", 64*1024+1),
		Value: []byte("value"),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized key code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}
