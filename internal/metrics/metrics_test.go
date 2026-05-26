package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewUsesProvidedRegistryAndAllowsRepeatedCreation(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	first, err := New(registry)
	if err != nil {
		t.Fatalf("New(first) error = %v", err)
	}

	second, err := New(registry)
	if err != nil {
		t.Fatalf("New(second) error = %v", err)
	}

	first.ObserveKafkaMessage("received")
	second.ObserveKafkaMessage("received")

	if got := testutil.ToFloat64(first.KafkaMessages.WithLabelValues("received")); got != 2 {
		t.Fatalf("kafka received = %v, want 2", got)
	}
}

func TestKafkaMetrics(t *testing.T) {
	t.Parallel()

	metrics := newTestMetrics(t)

	metrics.ObserveKafkaMessage("received")
	metrics.ObserveKafkaMessage("processed")
	metrics.IncKafkaDecodeError()
	metrics.IncKafkaCommitError()
	metrics.IncKafkaReadError()
	metrics.ObserveKafkaProcessingDuration(15 * time.Millisecond)
	metrics.SetKafkaLastMessageTimestamp(time.Unix(42, 0).UTC())

	if got := testutil.ToFloat64(metrics.KafkaMessages.WithLabelValues("received")); got != 1 {
		t.Fatalf("kafka received = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.KafkaMessages.WithLabelValues("processed")); got != 1 {
		t.Fatalf("kafka processed = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.KafkaDecodeErrors); got != 1 {
		t.Fatalf("decode errors = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.KafkaCommitErrors); got != 1 {
		t.Fatalf("commit errors = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.KafkaReadErrors); got != 1 {
		t.Fatalf("read errors = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.KafkaLastMessageTimestamp); got != 42 {
		t.Fatalf("last message timestamp = %v, want 42", got)
	}
}

func TestAggregatorMetrics(t *testing.T) {
	t.Parallel()

	metrics := newTestMetrics(t)

	metrics.ObserveEvent("accepted")
	metrics.ObserveEvent("invalid")
	metrics.SetUniqueQueries(17)
	metrics.SetTopCacheSize(10)
	metrics.SetWindowBuckets(300)
	metrics.SetCurrentWindowSeconds(300)
	metrics.ObserveTopRebuild("success", 3*time.Millisecond)

	if got := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues("accepted")); got != 1 {
		t.Fatalf("accepted events = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.EventsTotal.WithLabelValues("invalid")); got != 1 {
		t.Fatalf("invalid events = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.UniqueQueries); got != 17 {
		t.Fatalf("unique queries = %v, want 17", got)
	}
	if got := testutil.ToFloat64(metrics.TopCacheSize); got != 10 {
		t.Fatalf("top cache size = %v, want 10", got)
	}
	if got := testutil.ToFloat64(metrics.WindowBucketsTotal); got != 300 {
		t.Fatalf("window buckets = %v, want 300", got)
	}
	if got := testutil.ToFloat64(metrics.CurrentWindowSeconds); got != 300 {
		t.Fatalf("window seconds = %v, want 300", got)
	}
	if got := testutil.ToFloat64(metrics.TopRebuildTotal.WithLabelValues("success")); got != 1 {
		t.Fatalf("top rebuild success = %v, want 1", got)
	}
}

func newTestMetrics(t *testing.T) *Metrics {
	t.Helper()

	metrics, err := New(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return metrics
}
