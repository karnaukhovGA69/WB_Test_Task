package antispam

import (
	"context"
	"time"
)

type Config struct {
	Enabled                          bool
	MaxEventsPerIdentityPerMinute    int
	MaxSameQueryPerIdentityPerWindow int
	Metrics                          Metrics
}

type Metrics interface {
	ObserveAntiSpamCheck(result string)
	SetAntiSpamActiveKeys(count int)
}

type Event struct {
	EventID    string
	Query      string
	UserIDHash string
	SessionID  string
	IPHash     string
	Timestamp  time.Time
}

type Decision struct {
	Allowed bool
	Reason  string
}

type Limiter interface {
	Allow(ctx context.Context, event Event) (Decision, error)
	Stats() Stats
}

type Stats struct {
	ActiveKeys int
}

type InMemoryLimiter struct {
	cfg     Config
	metrics Metrics
}

func NewLimiter(cfg Config) *InMemoryLimiter {
	limiter := &InMemoryLimiter{
		cfg: cfg,
	}
	limiter.metrics = cfg.Metrics
	limiter.setActiveKeys(0)

	return limiter
}

func (l *InMemoryLimiter) Allow(ctx context.Context, event Event) (Decision, error) {
	select {
	case <-ctx.Done():
		return Decision{}, ctx.Err()
	default:
	}

	if !l.cfg.Enabled {
		l.observeCheck("allowed")
		return Decision{Allowed: true}, nil
	}

	_ = event

	l.observeCheck("allowed")
	return Decision{Allowed: true}, nil
}

func (l *InMemoryLimiter) Stats() Stats {
	return Stats{ActiveKeys: 0}
}

func (l *InMemoryLimiter) observeCheck(result string) {
	if l.metrics != nil {
		l.metrics.ObserveAntiSpamCheck(result)
	}
}

func (l *InMemoryLimiter) setActiveKeys(count int) {
	if l.metrics != nil {
		l.metrics.SetAntiSpamActiveKeys(count)
	}
}
