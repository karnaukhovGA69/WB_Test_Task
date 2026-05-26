package aggregator

import (
	"time"
)

type Config struct {
	Window             time.Duration
	BucketSize         time.Duration
	DefaultLimit       int
	MaxLimit           int
	TopRefreshInterval time.Duration
	MaxQueryRunes      int
	AllowedFutureSkew  time.Duration
	Metrics            Metrics
}

type Metrics interface {
	ObserveEvent(status string)
	SetUniqueQueries(count int)
	SetTopCacheSize(count int)
	ObserveTopRebuild(status string, duration time.Duration)
	SetWindowBuckets(count int)
	SetCurrentWindowSeconds(seconds int64)
}

type SearchEvent struct {
	Query     string
	UserID    string
	SessionID string
	IP        string
	Timestamp time.Time
}

type TopItem struct {
	Query string
	Count int64
}

type Snapshot struct {
	Items         []TopItem
	GeneratedAt   time.Time
	WindowSeconds int64
}
