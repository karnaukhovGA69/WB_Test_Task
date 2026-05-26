package aggregator

import (
	"container/heap"
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

const defaultBucketSize = time.Second

var (
	ErrInvalidTimestamp = errors.New("event timestamp is required")
	ErrEventFromFuture  = errors.New("event timestamp is too far in the future")
)

func IsValidationError(err error) bool {
	return errors.Is(err, ErrEmptyQuery) ||
		errors.Is(err, ErrQueryTooLong) ||
		errors.Is(err, ErrInvalidTimestamp) ||
		errors.Is(err, ErrEventFromFuture)
}

type Aggregator interface {
	Add(ctx context.Context, event SearchEvent) error
	GetTop(ctx context.Context, limit int) ([]TopItem, time.Time, error)
	RebuildTop(ctx context.Context) error
}

type Service interface {
	Aggregator
	Run(ctx context.Context) error
	Snapshot(ctx context.Context, limit int) (Snapshot, error)
	Close(ctx context.Context) error
}

type bucket struct {
	unixSecond int64
	counts     map[string]int64
}

type InMemoryAggregator struct {
	cfg         Config
	bucketCount int
	metrics     Metrics
	now         func() time.Time

	mu             sync.RWMutex
	buckets        []bucket
	counters       map[string]int64
	topCache       []TopItem
	topGeneratedAt time.Time
	dirty          bool
}

func New(cfg Config) (*InMemoryAggregator, error) {
	if cfg.Window <= 0 {
		return nil, errors.New("aggregator window must be positive")
	}
	if cfg.BucketSize <= 0 {
		cfg.BucketSize = defaultBucketSize
	}
	if cfg.Window%cfg.BucketSize != 0 {
		return nil, errors.New("aggregator window must be divisible by bucket size")
	}
	if cfg.DefaultLimit <= 0 {
		return nil, errors.New("aggregator default limit must be positive")
	}
	if cfg.MaxLimit < cfg.DefaultLimit {
		return nil, errors.New("aggregator max limit must be greater than or equal to default limit")
	}
	if cfg.TopRefreshInterval <= 0 {
		cfg.TopRefreshInterval = 500 * time.Millisecond
	}
	if cfg.MaxQueryRunes <= 0 {
		cfg.MaxQueryRunes = defaultMaxQueryRunes
	}
	if cfg.AllowedFutureSkew <= 0 {
		cfg.AllowedFutureSkew = 5 * time.Second
	}

	bucketCount := int(cfg.Window / cfg.BucketSize)
	if bucketCount <= 0 {
		return nil, errors.New("aggregator bucket count must be positive")
	}

	aggregator := &InMemoryAggregator{
		cfg:            cfg,
		bucketCount:    bucketCount,
		metrics:        cfg.Metrics,
		now:            func() time.Time { return time.Now().UTC() },
		buckets:        make([]bucket, bucketCount),
		counters:       make(map[string]int64),
		topCache:       make([]TopItem, 0, cfg.MaxLimit),
		topGeneratedAt: time.Now().UTC(),
	}
	aggregator.setWindowMetrics()

	return aggregator, nil
}

func (a *InMemoryAggregator) Add(ctx context.Context, event SearchEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	now := a.now()
	query, ignore, err := a.validateAndNormalize(event, now)
	if err != nil {
		a.observeEvent("invalid")
		return err
	}
	if ignore {
		a.observeEvent("expired")
		return nil
	}

	eventSecond := event.Timestamp.UTC().Truncate(a.cfg.BucketSize).Unix()

	a.mu.Lock()
	defer a.mu.Unlock()

	a.expireLocked(now)

	index := bucketIndex(eventSecond, a.bucketCount)
	currentBucket := &a.buckets[index]
	if currentBucket.counts == nil {
		currentBucket.counts = make(map[string]int64)
	}
	if currentBucket.unixSecond != eventSecond {
		a.clearBucketLocked(currentBucket)
		currentBucket.unixSecond = eventSecond
	}

	currentBucket.counts[query]++
	a.counters[query]++
	a.dirty = true
	a.setCounterMetricsLocked()
	a.observeEvent("accepted")

	return nil
}

func (a *InMemoryAggregator) GetTop(ctx context.Context, limit int) ([]TopItem, time.Time, error) {
	select {
	case <-ctx.Done():
		return nil, time.Time{}, ctx.Err()
	default:
	}

	limit = a.normalizeLimit(limit)

	a.mu.RLock()
	defer a.mu.RUnlock()

	items := a.topCache
	if limit < len(items) {
		items = items[:limit]
	}

	copied := make([]TopItem, len(items))
	copy(copied, items)

	return copied, a.topGeneratedAt, nil
}

func (a *InMemoryAggregator) RebuildTop(ctx context.Context) (err error) {
	startedAt := time.Now()
	status := "success"
	defer func() {
		if err != nil {
			status = "failed"
		}
		if a.metrics != nil {
			a.metrics.ObserveTopRebuild(status, time.Since(startedAt))
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	now := a.now()

	a.mu.Lock()
	defer a.mu.Unlock()

	expired := a.expireLocked(now)
	if !a.dirty && !expired {
		a.setCounterMetricsLocked()
		return nil
	}

	items := buildTop(a.counters, a.cfg.MaxLimit)
	a.topCache = items
	a.topGeneratedAt = now
	a.dirty = false
	a.setCounterMetricsLocked()

	return nil
}

func (a *InMemoryAggregator) Run(ctx context.Context) error {
	ticker := time.NewTicker(a.cfg.TopRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := a.RebuildTop(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}

				return err
			}
		}
	}
}

func (a *InMemoryAggregator) Snapshot(ctx context.Context, limit int) (Snapshot, error) {
	items, generatedAt, err := a.GetTop(ctx, limit)
	if err != nil {
		return Snapshot{}, err
	}

	return Snapshot{
		Items:         items,
		GeneratedAt:   generatedAt,
		WindowSeconds: int64(a.cfg.Window.Seconds()),
	}, nil
}

func (a *InMemoryAggregator) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (a *InMemoryAggregator) validateAndNormalize(event SearchEvent, now time.Time) (query string, ignore bool, err error) {
	if event.Timestamp.IsZero() {
		return "", false, ErrInvalidTimestamp
	}

	eventTime := event.Timestamp.UTC()
	if eventTime.After(now.Add(a.cfg.AllowedFutureSkew)) {
		return "", false, ErrEventFromFuture
	}
	if eventTime.Before(now.Add(-a.cfg.Window)) {
		return "", true, nil
	}

	query, err = NormalizeQuery(event.Query, a.cfg.MaxQueryRunes)
	if err != nil {
		return "", false, err
	}

	return query, false, nil
}

func (a *InMemoryAggregator) normalizeLimit(limit int) int {
	if limit <= 0 {
		return a.cfg.DefaultLimit
	}
	if limit > a.cfg.MaxLimit {
		return a.cfg.MaxLimit
	}

	return limit
}

func (a *InMemoryAggregator) expireLocked(now time.Time) bool {
	cutoffSecond := now.UTC().Truncate(a.cfg.BucketSize).Unix() - int64(a.bucketCount) + 1
	expired := false

	for index := range a.buckets {
		currentBucket := &a.buckets[index]
		if currentBucket.counts == nil || currentBucket.unixSecond == 0 {
			continue
		}
		if currentBucket.unixSecond < cutoffSecond {
			a.clearBucketLocked(currentBucket)
			expired = true
		}
	}

	return expired
}

func (a *InMemoryAggregator) clearBucketLocked(currentBucket *bucket) {
	for query, count := range currentBucket.counts {
		nextCount := a.counters[query] - count
		if nextCount <= 0 {
			delete(a.counters, query)
			continue
		}

		a.counters[query] = nextCount
	}

	clear(currentBucket.counts)
	currentBucket.unixSecond = 0
}

func (a *InMemoryAggregator) observeEvent(status string) {
	if a.metrics != nil {
		a.metrics.ObserveEvent(status)
	}
}

func (a *InMemoryAggregator) setCounterMetricsLocked() {
	if a.metrics == nil {
		return
	}

	a.metrics.SetUniqueQueries(len(a.counters))
	a.metrics.SetTopCacheSize(len(a.topCache))
}

func (a *InMemoryAggregator) setWindowMetrics() {
	if a.metrics == nil {
		return
	}

	a.metrics.SetWindowBuckets(a.bucketCount)
	a.metrics.SetCurrentWindowSeconds(int64(a.cfg.Window.Seconds()))
	a.metrics.SetTopCacheSize(0)
	a.metrics.SetUniqueQueries(0)
}

func buildTop(counters map[string]int64, limit int) []TopItem {
	if limit <= 0 || len(counters) == 0 {
		return nil
	}

	candidates := make(topItemHeap, 0, min(limit, len(counters)))

	for query, count := range counters {
		if count <= 0 {
			continue
		}

		item := TopItem{
			Query: query,
			Count: count,
		}
		if candidates.Len() < limit {
			heap.Push(&candidates, item)
			continue
		}

		if isBetter(item, candidates[0]) {
			candidates[0] = item
			heap.Fix(&candidates, 0)
		}
	}

	items := make([]TopItem, candidates.Len())
	copy(items, candidates)
	sortTop(items)

	return items
}

func sortTop(items []TopItem) {
	sort.Slice(items, func(i, j int) bool {
		return isBetter(items[i], items[j])
	})
}

func isBetter(left, right TopItem) bool {
	if left.Count != right.Count {
		return left.Count > right.Count
	}

	return left.Query < right.Query
}

func bucketIndex(unixSecond int64, bucketCount int) int {
	index := unixSecond % int64(bucketCount)
	if index < 0 {
		index += int64(bucketCount)
	}

	return int(index)
}

type topItemHeap []TopItem

func (h topItemHeap) Len() int {
	return len(h)
}

func (h topItemHeap) Less(i, j int) bool {
	if h[i].Count != h[j].Count {
		return h[i].Count < h[j].Count
	}

	return h[i].Query > h[j].Query
}

func (h topItemHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *topItemHeap) Push(value any) {
	*h = append(*h, value.(TopItem))
}

func (h *topItemHeap) Pop() any {
	old := *h
	index := len(old) - 1
	value := old[index]
	*h = old[:index]

	return value
}
