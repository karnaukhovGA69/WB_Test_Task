package kafka

import (
	"time"

	"github.com/wb-test-task/search-trends/internal/aggregator"
)

type KafkaSearchEvent struct {
	EventID   string
	Query     string
	UserID    string
	SessionID string
	IP        string
	UserAgent string
	Timestamp time.Time
}

func (e KafkaSearchEvent) ToAggregatorEvent() aggregator.SearchEvent {
	return aggregator.SearchEvent{
		Query:     e.Query,
		UserID:    e.UserID,
		SessionID: e.SessionID,
		IP:        e.IP,
		Timestamp: e.Timestamp,
	}
}
