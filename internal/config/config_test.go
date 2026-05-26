package config

import (
	"testing"
	"time"
)

func TestLoadExampleConfig(t *testing.T) {
	t.Parallel()

	cfg, err := Load("../../config.example.yaml")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Aggregator.Window != 5*time.Minute {
		t.Fatalf("Aggregator.Window = %s, want 5m", cfg.Aggregator.Window)
	}
	if cfg.Aggregator.BucketSize != time.Second {
		t.Fatalf("Aggregator.BucketSize = %s, want 1s", cfg.Aggregator.BucketSize)
	}
	if cfg.Aggregator.MaxQueryRunes != 256 {
		t.Fatalf("Aggregator.MaxQueryRunes = %d, want 256", cfg.Aggregator.MaxQueryRunes)
	}
	if cfg.Server.GRPCAddr != ":9090" {
		t.Fatalf("Server.GRPCAddr = %q, want :9090", cfg.Server.GRPCAddr)
	}
	if cfg.Metrics.Host != "0.0.0.0" || cfg.Metrics.Port != 2112 || cfg.Metrics.Path != "/metrics" {
		t.Fatalf("Metrics config = %#v, want host 0.0.0.0 port 2112 path /metrics", cfg.Metrics)
	}
	if cfg.Kafka.Topic != "search-events" {
		t.Fatalf("Kafka.Topic = %q, want search-events", cfg.Kafka.Topic)
	}
	if cfg.Kafka.CommitIntervalMS != 0 {
		t.Fatalf("Kafka.CommitIntervalMS = %d, want 0", cfg.Kafka.CommitIntervalMS)
	}
	if cfg.Kafka.StartOffset != "latest" {
		t.Fatalf("Kafka.StartOffset = %q, want latest", cfg.Kafka.StartOffset)
	}
}
