package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrDecode     = errors.New("decode kafka search event")
	ErrValidation = errors.New("validate kafka search event")
)

type Decoder interface {
	Decode(data []byte) (KafkaSearchEvent, error)
}

type JSONDecoder struct{}

func NewJSONDecoder() JSONDecoder {
	return JSONDecoder{}
}

func (d JSONDecoder) Decode(data []byte) (KafkaSearchEvent, error) {
	var raw rawSearchEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return KafkaSearchEvent{}, fmt.Errorf("%w: invalid json: %v", ErrDecode, err)
	}

	eventID := strings.TrimSpace(raw.EventID)
	if eventID == "" {
		return KafkaSearchEvent{}, fmt.Errorf("%w: event_id is required", ErrValidation)
	}

	query := strings.TrimSpace(raw.Query)
	if query == "" {
		return KafkaSearchEvent{}, fmt.Errorf("%w: query is required", ErrValidation)
	}

	timestampText := strings.TrimSpace(raw.Timestamp)
	if timestampText == "" {
		return KafkaSearchEvent{}, fmt.Errorf("%w: timestamp is required", ErrValidation)
	}

	timestamp, err := time.Parse(time.RFC3339Nano, timestampText)
	if err != nil {
		return KafkaSearchEvent{}, fmt.Errorf("%w: timestamp must be RFC3339: %v", ErrValidation, err)
	}

	return KafkaSearchEvent{
		EventID:   eventID,
		Query:     raw.Query,
		UserID:    strings.TrimSpace(raw.UserID),
		SessionID: strings.TrimSpace(raw.SessionID),
		IP:        strings.TrimSpace(raw.IP),
		UserAgent: strings.TrimSpace(raw.UserAgent),
		Timestamp: timestamp.UTC(),
	}, nil
}

type rawSearchEvent struct {
	EventID   string `json:"event_id"`
	Query     string `json:"query"`
	UserID    string `json:"user_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	IP        string `json:"ip,omitempty"`
	UserAgent string `json:"user_agent,omitempty"`
	Timestamp string `json:"timestamp"`
}
