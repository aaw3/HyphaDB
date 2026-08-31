package hyphadb

import (
	"context"
	"errors"

	hyphadbv1 "github.com/aaw3/hyphadb/gen/hyphadb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer adapts the public storage service to the generated gRPC API.
type GRPCServer struct {
	hyphadbv1.UnimplementedStorageServiceServer
	service *Service
}

// NewGRPCServer creates a gRPC server backed by service.
func NewGRPCServer(service *Service) *GRPCServer {
	return &GRPCServer{service: service}
}

var _ hyphadbv1.StorageServiceServer = (*GRPCServer)(nil)

func (s *GRPCServer) Get(ctx context.Context, req *hyphadbv1.GetRequest) (*hyphadbv1.GetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := contextError(ctx); err != nil {
		return nil, grpcError(err)
	}

	response, err := s.service.Get(ctx, GetRequest{Key: req.GetKey()})
	if err != nil {
		return nil, grpcError(err)
	}
	return &hyphadbv1.GetResponse{Value: response.Value}, nil
}

func (s *GRPCServer) Put(ctx context.Context, req *hyphadbv1.PutRequest) (*hyphadbv1.PutResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := contextError(ctx); err != nil {
		return nil, grpcError(err)
	}

	err := s.service.Put(ctx, PutRequest{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Sync:  req.GetSync(),
	})
	if err != nil {
		return nil, grpcError(err)
	}
	return &hyphadbv1.PutResponse{}, nil
}

func (s *GRPCServer) Delete(ctx context.Context, req *hyphadbv1.DeleteRequest) (*hyphadbv1.DeleteResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := contextError(ctx); err != nil {
		return nil, grpcError(err)
	}

	err := s.service.Delete(ctx, DeleteRequest{
		Key:  req.GetKey(),
		Sync: req.GetSync(),
	})
	if err != nil {
		return nil, grpcError(err)
	}
	return &hyphadbv1.DeleteResponse{}, nil
}

func (s *GRPCServer) WriteBatch(ctx context.Context, req *hyphadbv1.WriteBatchRequest) (*hyphadbv1.WriteBatchResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := contextError(ctx); err != nil {
		return nil, grpcError(err)
	}

	batch := s.service.db.NewBatch()
	for _, mutation := range req.GetMutations() {
		if err := contextError(ctx); err != nil {
			_ = batch.Cancel()
			return nil, grpcError(err)
		}
		if mutation == nil {
			_ = batch.Cancel()
			return nil, status.Error(codes.InvalidArgument, "mutation is required")
		}

		var err error
		switch mutation.GetType() {
		case hyphadbv1.Mutation_PUT:
			err = batch.Put(mutation.GetKey(), mutation.GetValue())
		case hyphadbv1.Mutation_DELETE:
			err = batch.Delete(mutation.GetKey())
		default:
			_ = batch.Cancel()
			return nil, status.Error(codes.InvalidArgument, "mutation type is required")
		}
		if err != nil {
			_ = batch.Cancel()
			return nil, grpcError(err)
		}
	}

	if err := batch.Commit(WriteOptions{Sync: req.GetSync()}); err != nil {
		return nil, grpcError(err)
	}
	return &hyphadbv1.WriteBatchResponse{}, nil
}

func (s *GRPCServer) Scan(req *hyphadbv1.ScanRequest, stream hyphadbv1.StorageService_ScanServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetLimit() < 0 {
		return status.Error(codes.InvalidArgument, "limit cannot be negative")
	}
	if err := contextError(stream.Context()); err != nil {
		return grpcError(err)
	}

	iterator, err := s.service.db.NewIterator(IteratorOptions{
		Start: req.GetStart(),
		End:   req.GetEnd(),
	})
	if err != nil {
		return grpcError(err)
	}
	defer iterator.Close()

	var sent int32
	for iterator.Next() {
		if err := contextError(stream.Context()); err != nil {
			return grpcError(err)
		}
		if req.GetLimit() > 0 && sent >= req.GetLimit() {
			break
		}

		if err := stream.Send(&hyphadbv1.ScanEntry{
			Key:   iterator.Key(),
			Value: iterator.Value(),
		}); err != nil {
			return err
		}
		sent++
	}
	if err := iterator.Err(); err != nil {
		return grpcError(err)
	}
	return nil
}

func grpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrClosed), errors.Is(err, ErrSnapshotClosed):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
