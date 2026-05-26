package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	appmetrics "github.com/wb-test-task/search-trends/internal/metrics"
)

func TestMetricsServerHandlerServesMetricsAndHealth(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metrics, err := appmetrics.New(registry)
	if err != nil {
		t.Fatalf("metrics.New() error = %v", err)
	}
	metrics.ObserveEvent("accepted")

	server := NewMetricsServer(MetricsConfig{
		Host: "127.0.0.1",
		Port: 2112,
		Path: "/metrics",
	}, registry, nil)

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(metricsResp, metricsReq)

	if metricsResp.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want %d", metricsResp.Code, http.StatusOK)
	}
	if !strings.Contains(metricsResp.Body.String(), "search_trends_events_total") {
		t.Fatalf("/metrics body does not contain search_trends_events_total")
	}

	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthResp := httptest.NewRecorder()
	server.Handler().ServeHTTP(healthResp, healthReq)

	if healthResp.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d, want %d", healthResp.Code, http.StatusOK)
	}
}
