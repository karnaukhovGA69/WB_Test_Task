package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/wb-test-task/search-trends/internal/aggregator"
	"github.com/wb-test-task/search-trends/internal/antispam"
	"github.com/wb-test-task/search-trends/internal/config"
	appkafka "github.com/wb-test-task/search-trends/internal/kafka"
	appmetrics "github.com/wb-test-task/search-trends/internal/metrics"
	"github.com/wb-test-task/search-trends/internal/server"
	"github.com/wb-test-task/search-trends/internal/stoplist"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "search trends service failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger, err := newLogger(cfg.App.Env)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	registry := prometheus.NewRegistry()
	metrics, err := appmetrics.New(registry)
	if err != nil {
		return fmt.Errorf("create metrics: %w", err)
	}

	agg, err := aggregator.New(aggregator.Config{
		Window:             cfg.Aggregator.Window,
		BucketSize:         cfg.Aggregator.BucketSize,
		DefaultLimit:       cfg.Aggregator.DefaultLimit,
		MaxLimit:           cfg.Aggregator.MaxLimit,
		TopRefreshInterval: cfg.Aggregator.TopRefreshInterval,
		MaxQueryRunes:      cfg.Aggregator.MaxQueryRunes,
		AllowedFutureSkew:  cfg.Aggregator.AllowedFutureSkew,
		Metrics:            metrics,
	})
	if err != nil {
		return fmt.Errorf("create aggregator: %w", err)
	}

	stopWords := stoplist.NewManager(cfg.StopList.InitialWords)
	limiter := antispam.NewLimiter(antispam.Config{
		Enabled:                          cfg.AntiSpam.Enabled,
		MaxEventsPerIdentityPerMinute:    cfg.AntiSpam.MaxEventsPerIdentityPerMinute,
		MaxSameQueryPerIdentityPerWindow: cfg.AntiSpam.MaxSameQueryPerIdentityPerWindow,
		Metrics:                          metrics,
	})
	metrics.SetStopListSize(stopWords.Size())

	consumer, err := appkafka.NewConsumer(appkafka.Config{
		Brokers:        cfg.Kafka.Brokers,
		Topic:          cfg.Kafka.Topic,
		GroupID:        cfg.Kafka.GroupID,
		ClientID:       cfg.Kafka.ClientID,
		MinBytes:       cfg.Kafka.MinBytes,
		MaxBytes:       cfg.Kafka.MaxBytes,
		MaxWait:        time.Duration(cfg.Kafka.MaxWaitMS) * time.Millisecond,
		CommitInterval: time.Duration(cfg.Kafka.CommitIntervalMS) * time.Millisecond,
		StartOffset:    cfg.Kafka.StartOffset,
		CommitRetries:  cfg.Kafka.CommitRetries,
		RetryBackoff:   cfg.Kafka.RetryBackoff,
	}, logger, metrics, appkafka.HandlerFunc(func(ctx context.Context, event appkafka.KafkaSearchEvent) error {
		if stopWords.Contains(event.Query) {
			metrics.ObserveEvent("stopped")
			return nil
		}

		decision, err := limiter.Allow(ctx, antispam.Event{
			EventID:    event.EventID,
			Query:      event.Query,
			UserIDHash: event.UserID,
			SessionID:  event.SessionID,
			IPHash:     event.IP,
			Timestamp:  event.Timestamp,
		})
		if err != nil {
			return err
		}
		if !decision.Allowed {
			metrics.ObserveEvent("rate_limited")
			return nil
		}

		if err := agg.Add(ctx, event.ToAggregatorEvent()); err != nil {
			if aggregator.IsValidationError(err) {
				return nil
			}

			return err
		}

		return nil
	}))
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Warn("close kafka consumer", zap.Error(err))
		}
	}()

	grpcServer, err := server.NewGRPCServer(server.GRPCConfig{
		Address: cfg.Server.GRPCAddr,
	}, server.GRPCDependencies{
		TopProvider: agg,
		StopWords:   stopWords,
		Metrics:     metrics,
	}, logger)
	if err != nil {
		return fmt.Errorf("create gRPC server: %w", err)
	}

	metricsServer := server.NewMetricsServer(server.MetricsConfig{
		Host: cfg.Metrics.Host,
		Port: cfg.Metrics.Port,
		Path: cfg.Metrics.Path,
	}, registry, logger)

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return agg.Run(groupCtx)
	})
	group.Go(func() error {
		return consumer.Run(groupCtx)
	})
	group.Go(func() error {
		return grpcServer.Run(groupCtx)
	})
	group.Go(func() error {
		return metricsServer.Run(groupCtx)
	})

	logger.Info("search trends service started")

	if err := group.Wait(); err != nil {
		return fmt.Errorf("service stopped with error: %w", err)
	}

	logger.Info("search trends service stopped")
	return nil
}

func newLogger(env string) (*zap.Logger, error) {
	if env == "local" || env == "dev" {
		return zap.NewDevelopment()
	}

	return zap.NewProduction()
}
