package server

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wb-test-task/search-trends/internal/aggregator"
	appmetrics "github.com/wb-test-task/search-trends/internal/metrics"
	searchtrendspb "github.com/wb-test-task/search-trends/proto/search_trends/v1"
)

type TopProvider interface {
	Snapshot(ctx context.Context, limit int) (aggregator.Snapshot, error)
}

type StopWords interface {
	Add(word string) bool
	Remove(word string) bool
	List() []string
	Size() int
}

type SearchTrendsHandler struct {
	searchtrendspb.UnimplementedSearchTrendsServiceServer

	topProvider TopProvider
	stopWords   StopWords
	metrics     *appmetrics.Metrics
	logger      *zap.Logger
}

func NewSearchTrendsHandler(topProvider TopProvider, stopWords StopWords, metrics *appmetrics.Metrics, logger *zap.Logger) (*SearchTrendsHandler, error) {
	if topProvider == nil {
		return nil, errors.New("top provider is required")
	}
	if stopWords == nil {
		return nil, errors.New("stop words manager is required")
	}
	if logger == nil {
		return nil, errors.New("logger is required")
	}

	return &SearchTrendsHandler{
		topProvider: topProvider,
		stopWords:   stopWords,
		metrics:     metrics,
		logger:      logger,
	}, nil
}

func (h *SearchTrendsHandler) GetTop(ctx context.Context, req *searchtrendspb.GetTopRequest) (*searchtrendspb.GetTopResponse, error) {
	limit := 0
	if req != nil {
		if uint64(req.GetLimit()) > uint64(maxInt()) {
			return nil, status.Error(codes.InvalidArgument, "limit is too large")
		}
		limit = int(req.GetLimit())
	}

	snapshot, err := h.topProvider.Snapshot(ctx, limit)
	if err != nil {
		return nil, toStatusError(err)
	}

	items := make([]*searchtrendspb.TopItem, 0, len(snapshot.Items))
	for _, item := range snapshot.Items {
		if item.Count <= 0 {
			continue
		}

		items = append(items, &searchtrendspb.TopItem{
			Query: item.Query,
			Count: uint64(item.Count),
		})
	}

	return &searchtrendspb.GetTopResponse{
		Items:         items,
		GeneratedAt:   timestamppb.New(snapshot.GeneratedAt),
		WindowSeconds: uint32(snapshot.WindowSeconds),
	}, nil
}

func (h *SearchTrendsHandler) AddStopWord(ctx context.Context, req *searchtrendspb.StopWordRequest) (*searchtrendspb.StopWordResponse, error) {
	select {
	case <-ctx.Done():
		return nil, toStatusError(ctx.Err())
	default:
	}

	word, err := validateStopWordRequest(req)
	if err != nil {
		h.observeStopListOperation("add", "failed")
		return nil, err
	}

	applied := h.stopWords.Add(word)
	message := "added"
	operationStatus := "success"
	if !applied {
		message = "already exists"
		operationStatus = "failed"
	}
	h.observeStopListOperation("add", operationStatus)
	h.setStopListSize()

	return &searchtrendspb.StopWordResponse{
		Word:    word,
		Applied: applied,
		Message: message,
	}, nil
}

func (h *SearchTrendsHandler) RemoveStopWord(ctx context.Context, req *searchtrendspb.StopWordRequest) (*searchtrendspb.StopWordResponse, error) {
	select {
	case <-ctx.Done():
		return nil, toStatusError(ctx.Err())
	default:
	}

	word, err := validateStopWordRequest(req)
	if err != nil {
		h.observeStopListOperation("remove", "failed")
		return nil, err
	}

	applied := h.stopWords.Remove(word)
	message := "removed"
	operationStatus := "success"
	if !applied {
		message = "not found"
		operationStatus = "failed"
	}
	h.observeStopListOperation("remove", operationStatus)
	h.setStopListSize()

	return &searchtrendspb.StopWordResponse{
		Word:    word,
		Applied: applied,
		Message: message,
	}, nil
}

func (h *SearchTrendsHandler) ListStopWords(ctx context.Context, _ *searchtrendspb.ListStopWordsRequest) (*searchtrendspb.ListStopWordsResponse, error) {
	select {
	case <-ctx.Done():
		return nil, toStatusError(ctx.Err())
	default:
	}

	words := h.stopWords.List()
	h.observeStopListOperation("list", "success")
	h.setStopListSize()

	return &searchtrendspb.ListStopWordsResponse{
		Words: words,
	}, nil
}

func (h *SearchTrendsHandler) HealthCheck(ctx context.Context, _ *searchtrendspb.HealthCheckRequest) (*searchtrendspb.HealthCheckResponse, error) {
	select {
	case <-ctx.Done():
		return nil, toStatusError(ctx.Err())
	default:
	}

	return &searchtrendspb.HealthCheckResponse{
		Status:    "SERVING",
		CheckedAt: timestamppb.New(time.Now().UTC()),
	}, nil
}

func validateStopWordRequest(req *searchtrendspb.StopWordRequest) (string, error) {
	if req == nil {
		return "", status.Error(codes.InvalidArgument, "request is required")
	}

	word := strings.TrimSpace(req.GetWord())
	if word == "" {
		return "", status.Error(codes.InvalidArgument, "word is required")
	}

	normalized, err := aggregator.NormalizeQuery(word, 256)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "invalid word: %v", err)
	}

	return normalized, nil
}

func toStatusError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func (h *SearchTrendsHandler) observeStopListOperation(operation string, status string) {
	if h.metrics != nil {
		h.metrics.ObserveStopListOperation(operation, status)
	}
}

func (h *SearchTrendsHandler) setStopListSize() {
	if h.metrics != nil {
		h.metrics.SetStopListSize(h.stopWords.Size())
	}
}
