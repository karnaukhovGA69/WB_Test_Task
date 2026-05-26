package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	mathrand "math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type searchEvent struct {
	EventID   string `json:"event_id"`
	Query     string `json:"query"`
	UserID    string `json:"user_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	IP        string `json:"ip,omitempty"`
	UserAgent string `json:"user_agent,omitempty"`
	Timestamp string `json:"timestamp"`
}

func main() {
	if err := run(); err != nil {
		log.Printf("producer failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		brokersText   = flag.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
		topic         = flag.String("topic", "search-events", "Kafka topic")
		query         = flag.String("query", "iphone 15", "search query")
		userID        = flag.String("user-id", "", "user id")
		sessionID     = flag.String("session-id", "", "session id")
		ip            = flag.String("ip", "", "ip address")
		userAgent     = flag.String("user-agent", "search-trends-producer/1.0", "user agent")
		count         = flag.Int("count", 1, "number of events to send")
		interval      = flag.Duration("interval", 0, "delay between events")
		queryPoolSize = flag.Int("query-pool-size", 0, "number of generated queries; 0 disables pool mode")
		users         = flag.Int("users", 1, "number of generated users in pool mode")
		botRatio      = flag.Float64("bot-ratio", 0.03, "share of events produced by one bot-like actor in pool mode")
		seed          = flag.Int64("seed", time.Now().UnixNano(), "math/rand seed for pool mode")
	)
	flag.Parse()

	brokers := splitBrokers(*brokersText)
	if len(brokers) == 0 {
		return fmt.Errorf("at least one broker is required")
	}
	if strings.TrimSpace(*topic) == "" {
		return fmt.Errorf("topic is required")
	}
	if strings.TrimSpace(*query) == "" {
		return fmt.Errorf("query is required")
	}
	if *count <= 0 {
		return fmt.Errorf("count must be positive")
	}
	if *queryPoolSize < 0 {
		return fmt.Errorf("query-pool-size must not be negative")
	}
	if *users <= 0 {
		return fmt.Errorf("users must be positive")
	}
	if *botRatio < 0 || *botRatio > 1 {
		return fmt.Errorf("bot-ratio must be between 0 and 1")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    *topic,
		Balancer: &kafka.Hash{},
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("close writer: %v", err)
		}
	}()

	rng := mathrand.New(mathrand.NewSource(*seed))

	sent := 0
	for i := 0; i < *count; i++ {
		eventID, err := newEventID()
		if err != nil {
			return err
		}

		generated := generateEventInput(eventInputConfig{
			query:         *query,
			userID:        *userID,
			sessionID:     *sessionID,
			ip:            *ip,
			userAgent:     *userAgent,
			queryPoolSize: *queryPoolSize,
			users:         *users,
			botRatio:      *botRatio,
			rng:           rng,
		})

		event := searchEvent{
			EventID:   eventID,
			Query:     generated.query,
			UserID:    generated.userID,
			SessionID: generated.sessionID,
			IP:        generated.ip,
			UserAgent: generated.userAgent,
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		}

		payload, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		if err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.UserID),
			Value: payload,
			Time:  time.Now().UTC(),
		}); err != nil {
			return fmt.Errorf("write event: %w", err)
		}
		sent++

		if *interval > 0 && i < *count-1 {
			if err := sleep(ctx, *interval); err != nil {
				break
			}
		}
	}

	log.Printf("sent %d events to topic %q via brokers %s", sent, *topic, strings.Join(brokers, ","))
	return nil
}

type eventInput struct {
	query     string
	userID    string
	sessionID string
	ip        string
	userAgent string
}

type eventInputConfig struct {
	query         string
	userID        string
	sessionID     string
	ip            string
	userAgent     string
	queryPoolSize int
	users         int
	botRatio      float64
	rng           *mathrand.Rand
}

func generateEventInput(cfg eventInputConfig) eventInput {
	if cfg.queryPoolSize <= 0 {
		return eventInput{
			query:     cfg.query,
			userID:    cfg.userID,
			sessionID: cfg.sessionID,
			ip:        cfg.ip,
			userAgent: cfg.userAgent,
		}
	}

	query := weightedQuery(cfg.rng, cfg.queryPoolSize)
	userID := cfg.userID
	sessionID := cfg.sessionID
	ip := cfg.ip

	if cfg.rng.Float64() < cfg.botRatio {
		userID = "bot-actor"
		sessionID = "bot-session"
		ip = "10.255.0.1"
		query = "popular-query-000"
	} else {
		userIndex := cfg.rng.Intn(cfg.users)
		if userID == "" {
			userID = fmt.Sprintf("user-%05d", userIndex)
		}
		if sessionID == "" {
			sessionID = fmt.Sprintf("session-%05d", userIndex)
		}
		if ip == "" {
			ip = fmt.Sprintf("10.0.%d.%d", (userIndex/255)%255, userIndex%255)
		}
	}

	return eventInput{
		query:     query,
		userID:    userID,
		sessionID: sessionID,
		ip:        ip,
		userAgent: cfg.userAgent,
	}
}

func weightedQuery(rng *mathrand.Rand, poolSize int) string {
	popularCount := max(1, poolSize/10)
	if rng.Float64() < 0.8 {
		return fmt.Sprintf("popular-query-%03d", rng.Intn(popularCount))
	}

	return fmt.Sprintf("rare-query-%03d", rng.Intn(poolSize))
}

func splitBrokers(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func newEventID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("generate event id: %w", err)
	}

	return "evt_" + hex.EncodeToString(data[:]), nil
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
