package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type MetricsConfig struct {
	Host string
	Port int
	Path string
}

type MetricsServer struct {
	address  string
	path     string
	registry *prometheus.Registry
	logger   *zap.Logger
}

func NewMetricsServer(cfg MetricsConfig, registry *prometheus.Registry, logger *zap.Logger) *MetricsServer {
	if cfg.Host == "" {
		cfg.Host = "0.0.0.0"
	}
	if cfg.Port == 0 {
		cfg.Port = 2112
	}
	if cfg.Path == "" {
		cfg.Path = "/metrics"
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	return &MetricsServer{
		address:  net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port)),
		path:     cfg.Path,
		registry: registry,
		logger:   logger,
	}
}

func (s *MetricsServer) Run(ctx context.Context) error {
	server := s.HTTPServer()

	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			s.logger.Warn("shutdown metrics server", zap.Error(err))
		}
	}()

	s.logger.Info("metrics server listening", zap.String("addr", s.address))

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func (s *MetricsServer) HTTPServer() *http.Server {
	return &http.Server{
		Addr:              s.address,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 3 * time.Second,
	}
}

func (s *MetricsServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(s.path, promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return mux
}
