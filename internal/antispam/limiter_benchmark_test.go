package antispam

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkAntiSpamAllow(b *testing.B) {
	limiter := NewLimiter(Config{
		Enabled:                          true,
		MaxEventsPerIdentityPerMinute:    120,
		MaxSameQueryPerIdentityPerWindow: 10,
	})
	events := make([]Event, 100_000)
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	for i := range events {
		userID := i % 10_000
		events[i] = Event{
			EventID:    fmt.Sprintf("event-%08d", i),
			Query:      fmt.Sprintf("query-%06d", i%20_000),
			UserIDHash: fmt.Sprintf("user-%05d", userID),
			SessionID:  fmt.Sprintf("session-%05d", userID),
			IPHash:     fmt.Sprintf("10.0.%d.%d", (userID/255)%255, userID%255),
			Timestamp:  now.Add(-time.Duration(i%300) * time.Second),
		}
	}

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := limiter.Allow(ctx, events[i%len(events)]); err != nil {
			b.Fatal(err)
		}
	}
}
