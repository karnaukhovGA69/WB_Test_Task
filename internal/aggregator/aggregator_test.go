package aggregator

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAggregatorAddRebuildAndGetTop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	agg := newTestAggregator(t, now)

	events := []SearchEvent{
		{Query: " iPhone 15 ", Timestamp: now},
		{Query: "iphone   15", Timestamp: now},
		{Query: "case", Timestamp: now},
		{Query: "bag", Timestamp: now},
		{Query: "bag", Timestamp: now},
		{Query: "bag", Timestamp: now},
	}

	for _, event := range events {
		if err := agg.Add(ctx, event); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	if err := agg.RebuildTop(ctx); err != nil {
		t.Fatalf("RebuildTop() error = %v", err)
	}

	items, generatedAt, err := agg.GetTop(ctx, 2)
	if err != nil {
		t.Fatalf("GetTop() error = %v", err)
	}
	if !generatedAt.Equal(now) {
		t.Fatalf("generatedAt = %v, want %v", generatedAt, now)
	}

	want := []TopItem{
		{Query: "bag", Count: 3},
		{Query: "iphone 15", Count: 2},
	}
	if !reflect.DeepEqual(items, want) {
		t.Fatalf("GetTop() = %#v, want %#v", items, want)
	}
}

func TestAggregatorTieSortsByQueryAscending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	agg := newTestAggregator(t, now)

	for _, query := range []string{"zebra", "apple", "middle"} {
		if err := agg.Add(ctx, SearchEvent{Query: query, Timestamp: now}); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}
	if err := agg.RebuildTop(ctx); err != nil {
		t.Fatalf("RebuildTop() error = %v", err)
	}

	items, _, err := agg.GetTop(ctx, 10)
	if err != nil {
		t.Fatalf("GetTop() error = %v", err)
	}

	want := []TopItem{
		{Query: "apple", Count: 1},
		{Query: "middle", Count: 1},
		{Query: "zebra", Count: 1},
	}
	if !reflect.DeepEqual(items, want) {
		t.Fatalf("GetTop() = %#v, want %#v", items, want)
	}
}

func TestAggregatorExpiresOldBuckets(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	agg := newTestAggregator(t, now)

	if err := agg.Add(ctx, SearchEvent{Query: "old", Timestamp: now.Add(-299 * time.Second)}); err != nil {
		t.Fatalf("Add(old) error = %v", err)
	}
	if err := agg.Add(ctx, SearchEvent{Query: "new", Timestamp: now}); err != nil {
		t.Fatalf("Add(new) error = %v", err)
	}
	if err := agg.RebuildTop(ctx); err != nil {
		t.Fatalf("RebuildTop() error = %v", err)
	}

	agg.now = func() time.Time { return now.Add(2 * time.Second) }
	if err := agg.RebuildTop(ctx); err != nil {
		t.Fatalf("RebuildTop() after time shift error = %v", err)
	}

	items, _, err := agg.GetTop(ctx, 10)
	if err != nil {
		t.Fatalf("GetTop() error = %v", err)
	}

	want := []TopItem{{Query: "new", Count: 1}}
	if !reflect.DeepEqual(items, want) {
		t.Fatalf("GetTop() = %#v, want %#v", items, want)
	}
}

func TestAggregatorIgnoresTooOldEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	agg := newTestAggregator(t, now)

	if err := agg.Add(ctx, SearchEvent{Query: "old", Timestamp: now.Add(-6 * time.Minute)}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := agg.RebuildTop(ctx); err != nil {
		t.Fatalf("RebuildTop() error = %v", err)
	}

	items, _, err := agg.GetTop(ctx, 10)
	if err != nil {
		t.Fatalf("GetTop() error = %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("GetTop() len = %d, want 0: %#v", len(items), items)
	}
}

func TestAggregatorValidatesEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	agg := newTestAggregator(t, now)

	tests := []struct {
		name    string
		event   SearchEvent
		wantErr error
	}{
		{
			name:    "empty query",
			event:   SearchEvent{Query: " \x00 ", Timestamp: now},
			wantErr: ErrEmptyQuery,
		},
		{
			name:    "too long query",
			event:   SearchEvent{Query: strings.Repeat("я", 65), Timestamp: now},
			wantErr: ErrQueryTooLong,
		},
		{
			name:    "missing timestamp",
			event:   SearchEvent{Query: "iphone"},
			wantErr: ErrInvalidTimestamp,
		},
		{
			name:    "future timestamp",
			event:   SearchEvent{Query: "iphone", Timestamp: now.Add(10 * time.Second)},
			wantErr: ErrEventFromFuture,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := agg.Add(ctx, tt.event)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Add() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func newTestAggregator(t *testing.T, now time.Time) *InMemoryAggregator {
	t.Helper()

	agg, err := New(Config{
		Window:             5 * time.Minute,
		BucketSize:         time.Second,
		DefaultLimit:       10,
		MaxLimit:           10,
		TopRefreshInterval: time.Hour,
		MaxQueryRunes:      64,
		AllowedFutureSkew:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	agg.now = func() time.Time { return now }
	return agg
}
