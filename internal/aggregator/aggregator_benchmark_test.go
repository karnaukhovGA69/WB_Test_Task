package aggregator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkAggregatorAdd(b *testing.B) {
	ctx := context.Background()
	now := benchmarkNow()
	agg := newBenchmarkAggregator(b, now, 100)
	events := benchmarkSearchEvents(100_000, now)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := agg.Add(ctx, events[i%len(events)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAggregatorGetTop(b *testing.B) {
	ctx := context.Background()
	now := benchmarkNow()
	agg := newBenchmarkAggregator(b, now, 100)
	seedAggregator(b, agg, 100_000, now)

	if err := agg.RebuildTop(ctx); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, _, err := agg.GetTop(ctx, 10); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAggregatorRebuildTop(b *testing.B) {
	for _, uniqueQueries := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("unique_queries_%d", uniqueQueries), func(b *testing.B) {
			ctx := context.Background()
			now := benchmarkNow()
			agg := newBenchmarkAggregator(b, now, 100)
			seedAggregator(b, agg, uniqueQueries, now)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				agg.mu.Lock()
				agg.dirty = true
				agg.mu.Unlock()

				if err := agg.RebuildTop(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAggregatorConcurrentReadWrite(b *testing.B) {
	for _, readers := range []int{10, 25, 50} {
		b.Run(fmt.Sprintf("readers_%d", readers), func(b *testing.B) {
			ctx := context.Background()
			now := benchmarkNow()
			agg := newBenchmarkAggregator(b, now, 100)
			events := benchmarkSearchEvents(100_000, now)
			seedAggregator(b, agg, 20_000, now)

			if err := agg.RebuildTop(ctx); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()

			var readOps atomic.Int64
			var writeOps atomic.Int64
			var stopWriter atomic.Bool
			var wg sync.WaitGroup

			wg.Add(1)
			go func() {
				defer wg.Done()
				for !stopWriter.Load() {
					index := int(writeOps.Add(1)-1) % len(events)
					if err := agg.Add(ctx, events[index]); err != nil {
						b.Error(err)
						return
					}
				}
			}()

			wg.Add(readers)
			for reader := 0; reader < readers; reader++ {
				go func() {
					defer wg.Done()
					for {
						if int(readOps.Add(1)) > b.N {
							return
						}
						if _, _, err := agg.GetTop(ctx, 10); err != nil {
							b.Error(err)
							return
						}
					}
				}()
			}

			for readOps.Load() <= int64(b.N) {
				time.Sleep(time.Millisecond)
			}
			stopWriter.Store(true)
			wg.Wait()
		})
	}
}

func newBenchmarkAggregator(b *testing.B, now time.Time, maxLimit int) *InMemoryAggregator {
	b.Helper()

	agg, err := New(Config{
		Window:             5 * time.Minute,
		BucketSize:         time.Second,
		DefaultLimit:       10,
		MaxLimit:           maxLimit,
		TopRefreshInterval: time.Hour,
		MaxQueryRunes:      256,
		AllowedFutureSkew:  5 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}

	agg.now = func() time.Time { return now }
	return agg
}

func seedAggregator(b *testing.B, agg *InMemoryAggregator, count int, now time.Time) {
	b.Helper()

	ctx := context.Background()
	events := benchmarkSearchEvents(count, now)
	for _, event := range events {
		if err := agg.Add(ctx, event); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSearchEvents(count int, now time.Time) []SearchEvent {
	events := make([]SearchEvent, count)
	for i := 0; i < count; i++ {
		queryID := i % max(1, count/3)
		userID := i % 10_000
		events[i] = SearchEvent{
			Query:     fmt.Sprintf("query-%06d", queryID),
			UserID:    fmt.Sprintf("user-%05d", userID),
			SessionID: fmt.Sprintf("session-%05d", userID),
			IP:        fmt.Sprintf("10.0.%d.%d", (userID/255)%255, userID%255),
			Timestamp: now.Add(-time.Duration(i%300) * time.Second),
		}
	}
	return events
}

func benchmarkNow() time.Time {
	return time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
}
