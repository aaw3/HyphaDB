package hyphadb

import (
	"context"
	"encoding/base64"
	"errors"
)

var ErrInvalidRequest = errors.New("invalid request")

type StorageService interface {
	Get(context.Context, GetRequest) (GetResponse, error)
	Put(context.Context, PutRequest) error
	Delete(context.Context, DeleteRequest) error
	Scan(context.Context, ScanRequest) (ScanResponse, error)
}

type Service struct {
	db *DB
}

func NewService(db *DB) *Service {
	return &Service{db: db}
}

var _ StorageService = (*Service)(nil)

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value []byte
}

type PutRequest struct {
	Key   string
	Value []byte
	Sync  bool
}

type DeleteRequest struct {
	Key  string
	Sync bool
}

type ScanRequest struct {
	// Start is inclusive and End is exclusive. An empty boundary is unbounded.
	Start string
	End   string
	Limit int
	// PageToken resumes strictly after the key returned in the previous page.
	PageToken string
}

type ScanEntry struct {
	Key   string
	Value []byte
}

type ScanResponse struct {
	Entries       []ScanEntry
	NextPageToken string
}

func (s *Service) Get(ctx context.Context, req GetRequest) (GetResponse, error) {
	if err := contextError(ctx); err != nil {
		return GetResponse{}, err
	}
	value, err := s.db.Get(req.Key)
	if err != nil {
		return GetResponse{}, err
	}
	return GetResponse{Value: cloneBytes(value)}, nil
}

func (s *Service) Put(ctx context.Context, req PutRequest) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	if err := batch.Put(req.Key, req.Value); err != nil {
		return err
	}
	return batch.Commit(WriteOptions{Sync: req.Sync})
}

func (s *Service) Delete(ctx context.Context, req DeleteRequest) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	if err := batch.Delete(req.Key); err != nil {
		return err
	}
	return batch.Commit(WriteOptions{Sync: req.Sync})
}

func (s *Service) Scan(ctx context.Context, req ScanRequest) (ScanResponse, error) {
	if err := contextError(ctx); err != nil {
		return ScanResponse{}, err
	}
	if req.Limit < 0 {
		return ScanResponse{}, ErrInvalidRequest
	}

	start := req.Start
	if req.PageToken != "" {
		pageKey, err := decodePageToken(req.PageToken)
		if err != nil {
			return ScanResponse{}, ErrInvalidRequest
		}
		start = pageKey
	}

	iterator, err := s.db.NewIterator(IteratorOptions{
		Start: start,
		End:   req.End,
	})
	if err != nil {
		return ScanResponse{}, err
	}
	defer iterator.Close()

	response := ScanResponse{}
	for iterator.Next() {
		if err := contextError(ctx); err != nil {
			return ScanResponse{}, err
		}

		key := iterator.Key()
		if req.PageToken != "" && key == start {
			continue
		}
		response.Entries = append(response.Entries, ScanEntry{
			Key:   key,
			Value: cloneBytes(iterator.Value()),
		})

		if req.Limit > 0 && len(response.Entries) >= req.Limit {
			if iterator.Next() {
				response.NextPageToken = encodePageToken(key)
			}
			break
		}
	}
	if err := iterator.Err(); err != nil {
		return ScanResponse{}, err
	}
	return response, nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func encodePageToken(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

func decodePageToken(token string) (string, error) {
	key, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}
	return string(key), nil
}
