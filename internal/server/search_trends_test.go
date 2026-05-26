package server

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wb-test-task/search-trends/internal/aggregator"
	"github.com/wb-test-task/search-trends/internal/stoplist"
	searchtrendspb "github.com/wb-test-task/search-trends/proto/search_trends/v1"
)

func TestSearchTrendsHandlerGetTop(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	provider := &fakeTopProvider{
		snapshot: aggregator.Snapshot{
			GeneratedAt:   generatedAt,
			WindowSeconds: 300,
			Items: []aggregator.TopItem{
				{Query: "iphone", Count: 10},
				{Query: "case", Count: 7},
			},
		},
	}

	handler := newTestSearchTrendsHandler(t, provider, stoplist.NewManager(nil))

	resp, err := handler.GetTop(context.Background(), &searchtrendspb.GetTopRequest{Limit: 2})
	if err != nil {
		t.Fatalf("GetTop() error = %v", err)
	}
	if provider.limit != 2 {
		t.Fatalf("provider limit = %d, want 2", provider.limit)
	}
	if resp.GetWindowSeconds() != 300 {
		t.Fatalf("WindowSeconds = %d, want 300", resp.GetWindowSeconds())
	}
	if got := resp.GetGeneratedAt().AsTime(); !got.Equal(generatedAt) {
		t.Fatalf("GeneratedAt = %v, want %v", got, generatedAt)
	}

	want := []*searchtrendspb.TopItem{
		{Query: "iphone", Count: 10},
		{Query: "case", Count: 7},
	}
	if !reflect.DeepEqual(resp.GetItems(), want) {
		t.Fatalf("Items = %#v, want %#v", resp.GetItems(), want)
	}
}

func TestSearchTrendsHandlerStopWords(t *testing.T) {
	t.Parallel()

	manager := stoplist.NewManager(nil)
	handler := newTestSearchTrendsHandler(t, &fakeTopProvider{}, manager)

	addResp, err := handler.AddStopWord(context.Background(), &searchtrendspb.StopWordRequest{Word: "  SPAM\tWord  "})
	if err != nil {
		t.Fatalf("AddStopWord() error = %v", err)
	}
	if !addResp.GetApplied() || addResp.GetWord() != "spam word" {
		t.Fatalf("AddStopWord() = %#v, want applied spam word", addResp)
	}

	duplicateResp, err := handler.AddStopWord(context.Background(), &searchtrendspb.StopWordRequest{Word: "spam word"})
	if err != nil {
		t.Fatalf("AddStopWord duplicate error = %v", err)
	}
	if duplicateResp.GetApplied() {
		t.Fatalf("duplicate AddStopWord applied = true, want false")
	}

	listResp, err := handler.ListStopWords(context.Background(), &searchtrendspb.ListStopWordsRequest{})
	if err != nil {
		t.Fatalf("ListStopWords() error = %v", err)
	}
	if !reflect.DeepEqual(listResp.GetWords(), []string{"spam word"}) {
		t.Fatalf("ListStopWords() = %#v, want [spam word]", listResp.GetWords())
	}

	removeResp, err := handler.RemoveStopWord(context.Background(), &searchtrendspb.StopWordRequest{Word: "spam word"})
	if err != nil {
		t.Fatalf("RemoveStopWord() error = %v", err)
	}
	if !removeResp.GetApplied() {
		t.Fatalf("RemoveStopWord applied = false, want true")
	}
}

func TestSearchTrendsHandlerRejectsEmptyStopWord(t *testing.T) {
	t.Parallel()

	handler := newTestSearchTrendsHandler(t, &fakeTopProvider{}, stoplist.NewManager(nil))

	_, err := handler.AddStopWord(context.Background(), &searchtrendspb.StopWordRequest{Word: " \t "})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AddStopWord() code = %s, want %s", status.Code(err), codes.InvalidArgument)
	}
}

func TestSearchTrendsHandlerHealthCheck(t *testing.T) {
	t.Parallel()

	handler := newTestSearchTrendsHandler(t, &fakeTopProvider{}, stoplist.NewManager(nil))

	resp, err := handler.HealthCheck(context.Background(), &searchtrendspb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck() error = %v", err)
	}
	if resp.GetStatus() != "SERVING" {
		t.Fatalf("Status = %q, want SERVING", resp.GetStatus())
	}
	if resp.GetCheckedAt() == nil {
		t.Fatal("CheckedAt is nil")
	}
}

func newTestSearchTrendsHandler(t *testing.T, provider TopProvider, words StopWords) *SearchTrendsHandler {
	t.Helper()

	handler, err := NewSearchTrendsHandler(provider, words, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("NewSearchTrendsHandler() error = %v", err)
	}

	return handler
}

type fakeTopProvider struct {
	limit    int
	snapshot aggregator.Snapshot
	err      error
}

func (p *fakeTopProvider) Snapshot(_ context.Context, limit int) (aggregator.Snapshot, error) {
	p.limit = limit
	return p.snapshot, p.err
}
