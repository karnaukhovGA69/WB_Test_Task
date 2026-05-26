package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/wb-test-task/search-trends/internal/aggregator"
)

func TestJSONDecoderValidEvent(t *testing.T) {
	t.Parallel()

	decoder := NewJSONDecoder()
	event, err := decoder.Decode([]byte(`{
		"event_id": "01HZY6Y4JQ9X8V9V7K2X3M4N5P",
		"query": "iPhone 15 Pro",
		"user_id": "user-123",
		"session_id": "session-456",
		"ip": "192.168.1.10",
		"user_agent": "Mozilla/5.0",
		"timestamp": "2026-05-23T12:00:00Z"
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	wantTimestamp := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	if event.EventID != "01HZY6Y4JQ9X8V9V7K2X3M4N5P" {
		t.Fatalf("EventID = %q", event.EventID)
	}
	if event.Query != "iPhone 15 Pro" {
		t.Fatalf("Query = %q", event.Query)
	}
	if event.UserID != "user-123" || event.SessionID != "session-456" || event.IP != "192.168.1.10" {
		t.Fatalf("actor fields = %#v", event)
	}
	if !event.Timestamp.Equal(wantTimestamp) {
		t.Fatalf("Timestamp = %v, want %v", event.Timestamp, wantTimestamp)
	}
}

func TestJSONDecoderErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{
			name:    "invalid json",
			payload: `{`,
			wantErr: ErrDecode,
		},
		{
			name:    "empty query",
			payload: `{"event_id":"evt-1","query":" ","timestamp":"2026-05-23T12:00:00Z"}`,
			wantErr: ErrValidation,
		},
		{
			name:    "empty event id",
			payload: `{"event_id":" ","query":"iphone","timestamp":"2026-05-23T12:00:00Z"}`,
			wantErr: ErrValidation,
		},
		{
			name:    "invalid timestamp",
			payload: `{"event_id":"evt-1","query":"iphone","timestamp":"not-a-date"}`,
			wantErr: ErrValidation,
		},
	}

	decoder := NewJSONDecoder()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := decoder.Decode([]byte(tt.payload))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Decode() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestKafkaSearchEventToAggregatorEvent(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	event := KafkaSearchEvent{
		EventID:   "evt-1",
		Query:     "iphone 15",
		UserID:    "user-1",
		SessionID: "session-1",
		IP:        "127.0.0.1",
		UserAgent: "test-agent",
		Timestamp: timestamp,
	}

	got := event.ToAggregatorEvent()
	want := aggregator.SearchEvent{
		Query:     "iphone 15",
		UserID:    "user-1",
		SessionID: "session-1",
		IP:        "127.0.0.1",
		Timestamp: timestamp,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ToAggregatorEvent() = %#v, want %#v", got, want)
	}
}

func TestConsumerProcessesValidMessageAndCommits(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{
		messages: []segmentio.Message{
			{
				Topic:         "search-events",
				Partition:     0,
				Offset:        42,
				HighWaterMark: 43,
				Value: []byte(`{
					"event_id":"evt-1",
					"query":"iphone",
					"user_id":"user-1",
					"timestamp":"2026-05-23T12:00:00Z"
				}`),
			},
		},
	}
	handler := &recordingHandler{}

	consumer := newTestConsumer(t, reader, handler)
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(handler.events) != 1 {
		t.Fatalf("handler events = %d, want 1", len(handler.events))
	}
	if handler.events[0].EventID != "evt-1" {
		t.Fatalf("event id = %q, want evt-1", handler.events[0].EventID)
	}
	if reader.commits != 1 {
		t.Fatalf("commits = %d, want 1", reader.commits)
	}
}

func TestConsumerCommitsInvalidMessageWithoutCallingHandler(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{
		messages: []segmentio.Message{
			{
				Topic:     "search-events",
				Partition: 0,
				Offset:    1,
				Value:     []byte(`{"event_id":"","query":"iphone","timestamp":"2026-05-23T12:00:00Z"}`),
			},
		},
	}
	handler := &recordingHandler{}

	consumer := newTestConsumer(t, reader, handler)
	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(handler.events) != 0 {
		t.Fatalf("handler events = %d, want 0", len(handler.events))
	}
	if reader.commits != 1 {
		t.Fatalf("commits = %d, want 1", reader.commits)
	}
}

func newTestConsumer(t *testing.T, reader Reader, handler Handler) *Consumer {
	t.Helper()

	consumer, err := NewConsumerWithReader(
		reader,
		NewJSONDecoder(),
		zap.NewNop(),
		nil,
		handler,
		1,
		time.Millisecond,
	)
	if err != nil {
		t.Fatalf("NewConsumerWithReader() error = %v", err)
	}

	return consumer
}

type fakeReader struct {
	messages []segmentio.Message
	index    int
	commits  int
	closed   bool
}

func (r *fakeReader) FetchMessage(context.Context) (segmentio.Message, error) {
	if r.index >= len(r.messages) {
		return segmentio.Message{}, context.Canceled
	}

	message := r.messages[r.index]
	r.index++
	return message, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, _ ...segmentio.Message) error {
	r.commits++
	return nil
}

func (r *fakeReader) Close() error {
	r.closed = true
	return nil
}

type recordingHandler struct {
	events []KafkaSearchEvent
}

func (h *recordingHandler) HandleSearchEvent(_ context.Context, event KafkaSearchEvent) error {
	h.events = append(h.events, event)
	return nil
}
