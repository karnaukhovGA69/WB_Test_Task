package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/wb-test-task/search-trends/internal/aggregator"
	appmetrics "github.com/wb-test-task/search-trends/internal/metrics"
)

type Config struct {
	Brokers        []string
	Topic          string
	GroupID        string
	ClientID       string
	MinBytes       int
	MaxBytes       int
	MaxWait        time.Duration
	CommitInterval time.Duration
	StartOffset    string
	CommitRetries  int
	RetryBackoff   time.Duration
}

type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Handler interface {
	HandleSearchEvent(ctx context.Context, event KafkaSearchEvent) error
}

type HandlerFunc func(ctx context.Context, event KafkaSearchEvent) error

func (f HandlerFunc) HandleSearchEvent(ctx context.Context, event KafkaSearchEvent) error {
	return f(ctx, event)
}

type Consumer struct {
	reader        Reader
	decoder       Decoder
	handler       Handler
	metrics       *appmetrics.Metrics
	logger        *zap.Logger
	commitRetries int
	retryBackoff  time.Duration
}

func NewConsumer(cfg Config, logger *zap.Logger, metrics *appmetrics.Metrics, handler Handler) (*Consumer, error) {
	reader, err := newKafkaReader(cfg)
	if err != nil {
		return nil, err
	}

	scopedLogger := logger.With(
		zap.String("topic", cfg.Topic),
		zap.String("group_id", cfg.GroupID),
		zap.String("client_id", cfg.ClientID),
	)

	return NewConsumerWithReader(reader, NewJSONDecoder(), scopedLogger, metrics, handler, cfg.CommitRetries, cfg.RetryBackoff)
}

func NewConsumerWithReader(
	reader Reader,
	decoder Decoder,
	logger *zap.Logger,
	metrics *appmetrics.Metrics,
	handler Handler,
	commitRetries int,
	retryBackoff time.Duration,
) (*Consumer, error) {
	if reader == nil {
		return nil, errors.New("kafka reader is required")
	}
	if decoder == nil {
		return nil, errors.New("kafka decoder is required")
	}
	if logger == nil {
		return nil, errors.New("logger is required")
	}
	if handler == nil {
		return nil, errors.New("kafka handler is required")
	}
	if commitRetries <= 0 {
		commitRetries = 3
	}
	if retryBackoff <= 0 {
		retryBackoff = 500 * time.Millisecond
	}

	return &Consumer{
		reader:        reader,
		decoder:       decoder,
		handler:       handler,
		metrics:       metrics,
		logger:        logger,
		commitRetries: commitRetries,
		retryBackoff:  retryBackoff,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("kafka consumer started")
	defer c.logger.Info("kafka consumer stopped")

	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if isContextError(err) {
				return nil
			}

			c.incKafkaReadError()
			c.observeKafkaMessage("failed")
			c.logger.Warn("fetch kafka message", zap.Error(err))
			if err := sleep(ctx, c.retryBackoff); err != nil {
				return nil
			}
			continue
		}

		c.observeKafkaMessage("received")

		shouldCommit, err := c.processMessage(ctx, message)
		if err != nil {
			if isContextError(err) {
				return nil
			}
			c.logger.Warn("process kafka message",
				zap.Error(err),
				zap.Int("partition", message.Partition),
				zap.Int64("offset", message.Offset),
			)
		}

		if !shouldCommit {
			continue
		}

		if err := c.commitWithRetry(ctx, message); err != nil {
			if isContextError(err) {
				return nil
			}
			return fmt.Errorf("commit kafka message: %w", err)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, message kafka.Message) (bool, error) {
	startedAt := time.Now()
	defer func() {
		if c.metrics != nil {
			c.metrics.ObserveKafkaProcessingDuration(time.Since(startedAt))
		}
	}()

	event, err := c.decoder.Decode(message.Value)
	if err != nil {
		if errors.Is(err, ErrDecode) {
			c.incKafkaDecodeError()
			c.observeKafkaMessage("invalid")
			c.logger.Warn("skip malformed kafka payload",
				zap.Error(err),
				zap.Int("partition", message.Partition),
				zap.Int64("offset", message.Offset),
			)
			return true, nil
		}
		if errors.Is(err, ErrValidation) {
			c.observeKafkaMessage("invalid")
			c.logger.Info("skip invalid kafka payload",
				zap.Error(err),
				zap.Int("partition", message.Partition),
				zap.Int64("offset", message.Offset),
			)
			return true, nil
		}

		c.observeKafkaMessage("failed")
		return false, err
	}

	if c.metrics != nil {
		c.metrics.SetKafkaLastMessageTimestamp(event.Timestamp)
	}

	if err := c.handler.HandleSearchEvent(ctx, event); err != nil {
		if aggregator.IsValidationError(err) {
			c.observeKafkaMessage("invalid")
			c.logger.Info("skip event rejected by aggregator validation",
				zap.Error(err),
				zap.String("event_id", event.EventID),
				zap.Int("partition", message.Partition),
				zap.Int64("offset", message.Offset),
			)
			return true, nil
		}

		c.observeKafkaMessage("failed")
		return false, err
	}

	c.observeKafkaMessage("processed")

	return true, nil
}

func (c *Consumer) commitWithRetry(ctx context.Context, message kafka.Message) error {
	var lastErr error

	for attempt := 1; attempt <= c.commitRetries; attempt++ {
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			lastErr = err
			if isContextError(err) {
				return err
			}

			c.incKafkaCommitError()
			c.observeKafkaMessage("failed")
			c.logger.Warn("commit kafka message failed",
				zap.Error(err),
				zap.Int("attempt", attempt),
				zap.Int("partition", message.Partition),
				zap.Int64("offset", message.Offset),
			)

			if err := sleep(ctx, c.retryBackoff); err != nil {
				return err
			}
			continue
		}

		return nil
	}

	return lastErr
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) observeKafkaMessage(status string) {
	if c.metrics == nil {
		return
	}

	c.metrics.ObserveKafkaMessage(status)
}

func (c *Consumer) incKafkaDecodeError() {
	if c.metrics != nil {
		c.metrics.IncKafkaDecodeError()
	}
}

func (c *Consumer) incKafkaCommitError() {
	if c.metrics != nil {
		c.metrics.IncKafkaCommitError()
	}
}

func (c *Consumer) incKafkaReadError() {
	if c.metrics != nil {
		c.metrics.IncKafkaReadError()
	}
}

func newKafkaReader(cfg Config) (Reader, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are required")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka topic is required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("kafka group id is required")
	}
	if cfg.MinBytes <= 0 {
		cfg.MinBytes = 1
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 10 * 1024 * 1024
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = time.Second
	}

	startOffset, err := parseStartOffset(cfg.StartOffset)
	if err != nil {
		return nil, err
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		GroupTopics:    []string{cfg.Topic},
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		MaxWait:        cfg.MaxWait,
		CommitInterval: cfg.CommitInterval,
		StartOffset:    startOffset,
	}

	return kafka.NewReader(readerConfig), nil
}

func parseStartOffset(value string) (int64, error) {
	switch value {
	case "", "latest":
		return kafka.LastOffset, nil
	case "earliest", "first":
		return kafka.FirstOffset, nil
	default:
		return 0, fmt.Errorf("unsupported kafka start offset %q", value)
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
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
