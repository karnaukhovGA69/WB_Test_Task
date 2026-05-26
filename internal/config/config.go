package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App        AppConfig        `yaml:"app"`
	Server     ServerConfig     `yaml:"server"`
	Metrics    MetricsConfig    `yaml:"metrics"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	Aggregator AggregatorConfig `yaml:"aggregator"`
	StopList   StopListConfig   `yaml:"stoplist"`
	AntiSpam   AntiSpamConfig   `yaml:"antispam"`
}

type AppConfig struct {
	Env             string        `yaml:"env"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type ServerConfig struct {
	GRPCAddr string `yaml:"grpc_addr"`
}

type MetricsConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
	Path string `yaml:"path"`
}

type KafkaConfig struct {
	Brokers          []string      `yaml:"brokers"`
	Topic            string        `yaml:"topic"`
	GroupID          string        `yaml:"group_id"`
	ClientID         string        `yaml:"client_id"`
	MinBytes         int           `yaml:"min_bytes"`
	MaxBytes         int           `yaml:"max_bytes"`
	MaxWaitMS        int           `yaml:"max_wait_ms"`
	CommitIntervalMS int           `yaml:"commit_interval_ms"`
	StartOffset      string        `yaml:"start_offset"`
	CommitRetries    int           `yaml:"commit_retries"`
	RetryBackoff     time.Duration `yaml:"retry_backoff"`
}

type AggregatorConfig struct {
	Window             time.Duration `yaml:"window"`
	BucketSize         time.Duration `yaml:"bucket_size"`
	DefaultLimit       int           `yaml:"default_limit"`
	MaxLimit           int           `yaml:"max_limit"`
	TopRefreshInterval time.Duration `yaml:"top_refresh_interval"`
	MaxQueryRunes      int           `yaml:"max_query_runes"`
	AllowedFutureSkew  time.Duration `yaml:"allowed_future_skew"`
}

type StopListConfig struct {
	InitialWords []string `yaml:"initial_words"`
}

type AntiSpamConfig struct {
	Enabled                          bool `yaml:"enabled"`
	MaxEventsPerIdentityPerMinute    int  `yaml:"max_events_per_identity_per_minute"`
	MaxSameQueryPerIdentityPerWindow int  `yaml:"max_same_query_per_identity_per_window"`
}

func Load(path string) (Config, error) {
	if path == "" {
		path = os.Getenv("CONFIG_PATH")
	}
	if path == "" {
		path = "config.example.yaml"
	}

	cfg := defaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Validate() error {
	if c.Server.GRPCAddr == "" {
		return errors.New("server.grpc_addr is required")
	}
	if c.Metrics.Host == "" {
		return errors.New("metrics.host is required")
	}
	if c.Metrics.Port <= 0 {
		return errors.New("metrics.port must be positive")
	}
	if c.Metrics.Path == "" {
		return errors.New("metrics.path is required")
	}
	if len(c.Kafka.Brokers) == 0 {
		return errors.New("kafka.brokers is required")
	}
	if c.Kafka.Topic == "" {
		return errors.New("kafka.topic is required")
	}
	if c.Kafka.GroupID == "" {
		return errors.New("kafka.group_id is required")
	}
	if c.Kafka.MinBytes <= 0 {
		return errors.New("kafka.min_bytes must be positive")
	}
	if c.Kafka.MaxBytes < c.Kafka.MinBytes {
		return errors.New("kafka.max_bytes must be greater than or equal to min_bytes")
	}
	if c.Kafka.MaxWaitMS <= 0 {
		return errors.New("kafka.max_wait_ms must be positive")
	}
	if c.Kafka.CommitIntervalMS < 0 {
		return errors.New("kafka.commit_interval_ms must not be negative")
	}
	if c.Kafka.StartOffset != "latest" && c.Kafka.StartOffset != "earliest" && c.Kafka.StartOffset != "first" {
		return errors.New("kafka.start_offset must be latest, earliest or first")
	}
	if c.Aggregator.Window <= 0 {
		return errors.New("aggregator.window must be positive")
	}
	if c.Aggregator.BucketSize <= 0 {
		return errors.New("aggregator.bucket_size must be positive")
	}
	if c.Aggregator.DefaultLimit <= 0 {
		return errors.New("aggregator.default_limit must be positive")
	}
	if c.Aggregator.MaxLimit < c.Aggregator.DefaultLimit {
		return errors.New("aggregator.max_limit must be greater than or equal to default_limit")
	}

	return nil
}

func defaultConfig() Config {
	return Config{
		App: AppConfig{
			Env:             "local",
			ShutdownTimeout: 10 * time.Second,
		},
		Server: ServerConfig{
			GRPCAddr: ":9090",
		},
		Metrics: MetricsConfig{
			Host: "0.0.0.0",
			Port: 2112,
			Path: "/metrics",
		},
		Kafka: KafkaConfig{
			Topic:            "search-events",
			GroupID:          "search-trends-service",
			ClientID:         "search-trends-service",
			MinBytes:         1,
			MaxBytes:         10 * 1024 * 1024,
			MaxWaitMS:        1000,
			CommitIntervalMS: 0,
			StartOffset:      "latest",
			CommitRetries:    3,
			RetryBackoff:     500 * time.Millisecond,
		},
		Aggregator: AggregatorConfig{
			Window:             5 * time.Minute,
			BucketSize:         time.Second,
			DefaultLimit:       10,
			MaxLimit:           100,
			TopRefreshInterval: 500 * time.Millisecond,
			MaxQueryRunes:      256,
			AllowedFutureSkew:  5 * time.Second,
		},
	}
}
