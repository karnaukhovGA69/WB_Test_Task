package metrics

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "search_trends"

type Metrics struct {
	KafkaMessages             *prometheus.CounterVec
	KafkaProcessingDuration   prometheus.Histogram
	KafkaLastMessageTimestamp prometheus.Gauge
	KafkaDecodeErrors         prometheus.Counter
	KafkaCommitErrors         prometheus.Counter
	KafkaReadErrors           prometheus.Counter

	EventsTotal          *prometheus.CounterVec
	UniqueQueries        prometheus.Gauge
	TopCacheSize         prometheus.Gauge
	TopRebuildDuration   prometheus.Histogram
	TopRebuildTotal      *prometheus.CounterVec
	WindowBucketsTotal   prometheus.Gauge
	CurrentWindowSeconds prometheus.Gauge

	GRPCRequests        *prometheus.CounterVec
	GRPCRequestDuration *prometheus.HistogramVec

	StopListSize       prometheus.Gauge
	StopListOperations *prometheus.CounterVec

	AntiSpamChecks     *prometheus.CounterVec
	AntiSpamActiveKeys prometheus.Gauge
}

func New(registry *prometheus.Registry) (*Metrics, error) {
	if registry == nil {
		return nil, errors.New("prometheus registry is required")
	}

	var err error
	m := &Metrics{}

	m.KafkaMessages, err = registerCounterVec(registry, prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "kafka_messages_total",
		Help:      "Total number of Kafka messages by processing status.",
	}, []string{"status"})
	if err != nil {
		return nil, err
	}

	m.KafkaProcessingDuration, err = registerHistogram(registry, prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "kafka_processing_duration_seconds",
		Help:      "Kafka message processing duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	})
	if err != nil {
		return nil, err
	}

	m.KafkaLastMessageTimestamp, err = registerGauge(registry, prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "kafka_last_message_timestamp_seconds",
		Help:      "Unix timestamp of the last successfully decoded Kafka event.",
	})
	if err != nil {
		return nil, err
	}

	m.KafkaDecodeErrors, err = registerCounter(registry, prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "kafka_decode_errors_total",
		Help:      "Total number of Kafka payload decode errors.",
	})
	if err != nil {
		return nil, err
	}

	m.KafkaCommitErrors, err = registerCounter(registry, prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "kafka_commit_errors_total",
		Help:      "Total number of Kafka commit errors.",
	})
	if err != nil {
		return nil, err
	}

	m.KafkaReadErrors, err = registerCounter(registry, prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "kafka_read_errors_total",
		Help:      "Total number of Kafka read errors.",
	})
	if err != nil {
		return nil, err
	}

	m.EventsTotal, err = registerCounterVec(registry, prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "events_total",
		Help:      "Total number of search events by business processing status.",
	}, []string{"status"})
	if err != nil {
		return nil, err
	}

	m.UniqueQueries, err = registerGauge(registry, prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "unique_queries",
		Help:      "Current number of unique queries in the aggregation window.",
	})
	if err != nil {
		return nil, err
	}

	m.TopCacheSize, err = registerGauge(registry, prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "top_cache_size",
		Help:      "Current number of items in the cached Top-N snapshot.",
	})
	if err != nil {
		return nil, err
	}

	m.TopRebuildDuration, err = registerHistogram(registry, prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "top_rebuild_duration_seconds",
		Help:      "Top cache rebuild duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	})
	if err != nil {
		return nil, err
	}

	m.TopRebuildTotal, err = registerCounterVec(registry, prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "top_rebuild_total",
		Help:      "Total number of top cache rebuild attempts by status.",
	}, []string{"status"})
	if err != nil {
		return nil, err
	}

	m.WindowBucketsTotal, err = registerGauge(registry, prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "window_buckets_total",
		Help:      "Number of buckets in the aggregation window.",
	})
	if err != nil {
		return nil, err
	}

	m.CurrentWindowSeconds, err = registerGauge(registry, prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "current_window_seconds",
		Help:      "Current aggregation window size in seconds.",
	})
	if err != nil {
		return nil, err
	}

	m.GRPCRequests, err = registerCounterVec(registry, prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "grpc_requests_total",
		Help:      "Total number of gRPC unary requests by method and status code.",
	}, []string{"method", "code"})
	if err != nil {
		return nil, err
	}

	m.GRPCRequestDuration, err = registerHistogramVec(registry, prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "grpc_request_duration_seconds",
		Help:      "gRPC unary request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method"})
	if err != nil {
		return nil, err
	}

	m.StopListSize, err = registerGauge(registry, prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "stoplist_size",
		Help:      "Current number of stop-list entries.",
	})
	if err != nil {
		return nil, err
	}

	m.StopListOperations, err = registerCounterVec(registry, prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "stoplist_operations_total",
		Help:      "Total number of stop-list operations by operation and status.",
	}, []string{"operation", "status"})
	if err != nil {
		return nil, err
	}

	m.AntiSpamChecks, err = registerCounterVec(registry, prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "antispam_checks_total",
		Help:      "Total number of anti-spam checks by result.",
	}, []string{"result"})
	if err != nil {
		return nil, err
	}

	m.AntiSpamActiveKeys, err = registerGauge(registry, prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "antispam_active_keys",
		Help:      "Current number of active actor/query keys in anti-spam state.",
	})
	if err != nil {
		return nil, err
	}

	m.primeLabels()
	return m, nil
}

func (m *Metrics) ObserveKafkaMessage(status string) {
	m.KafkaMessages.WithLabelValues(status).Inc()
}

func (m *Metrics) ObserveKafkaProcessingDuration(duration time.Duration) {
	m.KafkaProcessingDuration.Observe(duration.Seconds())
}

func (m *Metrics) SetKafkaLastMessageTimestamp(timestamp time.Time) {
	m.KafkaLastMessageTimestamp.Set(float64(timestamp.Unix()))
}

func (m *Metrics) IncKafkaDecodeError() {
	m.KafkaDecodeErrors.Inc()
}

func (m *Metrics) IncKafkaCommitError() {
	m.KafkaCommitErrors.Inc()
}

func (m *Metrics) IncKafkaReadError() {
	m.KafkaReadErrors.Inc()
}

func (m *Metrics) ObserveEvent(status string) {
	m.EventsTotal.WithLabelValues(status).Inc()
}

func (m *Metrics) SetUniqueQueries(count int) {
	m.UniqueQueries.Set(float64(count))
}

func (m *Metrics) SetTopCacheSize(count int) {
	m.TopCacheSize.Set(float64(count))
}

func (m *Metrics) ObserveTopRebuild(status string, duration time.Duration) {
	m.TopRebuildTotal.WithLabelValues(status).Inc()
	m.TopRebuildDuration.Observe(duration.Seconds())
}

func (m *Metrics) SetWindowBuckets(count int) {
	m.WindowBucketsTotal.Set(float64(count))
}

func (m *Metrics) SetCurrentWindowSeconds(seconds int64) {
	m.CurrentWindowSeconds.Set(float64(seconds))
}

func (m *Metrics) ObserveGRPCRequest(method string, code string, duration time.Duration) {
	m.GRPCRequests.WithLabelValues(method, code).Inc()
	m.GRPCRequestDuration.WithLabelValues(method).Observe(duration.Seconds())
}

func (m *Metrics) SetStopListSize(count int) {
	m.StopListSize.Set(float64(count))
}

func (m *Metrics) ObserveStopListOperation(operation string, status string) {
	m.StopListOperations.WithLabelValues(operation, status).Inc()
}

func (m *Metrics) ObserveAntiSpamCheck(result string) {
	m.AntiSpamChecks.WithLabelValues(result).Inc()
}

func (m *Metrics) SetAntiSpamActiveKeys(count int) {
	m.AntiSpamActiveKeys.Set(float64(count))
}

func (m *Metrics) primeLabels() {
	for _, status := range []string{"received", "processed", "invalid", "failed"} {
		m.KafkaMessages.WithLabelValues(status)
	}
	for _, status := range []string{"accepted", "rejected", "stopped", "rate_limited", "expired", "invalid"} {
		m.EventsTotal.WithLabelValues(status)
	}
	for _, status := range []string{"success", "failed"} {
		m.TopRebuildTotal.WithLabelValues(status)
	}
	for _, operation := range []string{"add", "remove", "list"} {
		for _, status := range []string{"success", "failed"} {
			m.StopListOperations.WithLabelValues(operation, status)
		}
	}
	for _, result := range []string{"allowed", "blocked"} {
		m.AntiSpamChecks.WithLabelValues(result)
	}
}

func registerCounter(registry *prometheus.Registry, opts prometheus.CounterOpts) (prometheus.Counter, error) {
	collector := prometheus.NewCounter(opts)
	if err := registry.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			existing, ok := alreadyRegistered.ExistingCollector.(prometheus.Counter)
			if !ok {
				return nil, fmt.Errorf("collector %s already registered with incompatible type", opts.Name)
			}
			return existing, nil
		}
		return nil, err
	}
	return collector, nil
}

func registerCounterVec(registry *prometheus.Registry, opts prometheus.CounterOpts, labels []string) (*prometheus.CounterVec, error) {
	collector := prometheus.NewCounterVec(opts, labels)
	if err := registry.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			existing, ok := alreadyRegistered.ExistingCollector.(*prometheus.CounterVec)
			if !ok {
				return nil, fmt.Errorf("collector %s already registered with incompatible type", opts.Name)
			}
			return existing, nil
		}
		return nil, err
	}
	return collector, nil
}

func registerGauge(registry *prometheus.Registry, opts prometheus.GaugeOpts) (prometheus.Gauge, error) {
	collector := prometheus.NewGauge(opts)
	if err := registry.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			existing, ok := alreadyRegistered.ExistingCollector.(prometheus.Gauge)
			if !ok {
				return nil, fmt.Errorf("collector %s already registered with incompatible type", opts.Name)
			}
			return existing, nil
		}
		return nil, err
	}
	return collector, nil
}

func registerHistogram(registry *prometheus.Registry, opts prometheus.HistogramOpts) (prometheus.Histogram, error) {
	collector := prometheus.NewHistogram(opts)
	if err := registry.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			existing, ok := alreadyRegistered.ExistingCollector.(prometheus.Histogram)
			if !ok {
				return nil, fmt.Errorf("collector %s already registered with incompatible type", opts.Name)
			}
			return existing, nil
		}
		return nil, err
	}
	return collector, nil
}

func registerHistogramVec(registry *prometheus.Registry, opts prometheus.HistogramOpts, labels []string) (*prometheus.HistogramVec, error) {
	collector := prometheus.NewHistogramVec(opts, labels)
	if err := registry.Register(collector); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			existing, ok := alreadyRegistered.ExistingCollector.(*prometheus.HistogramVec)
			if !ok {
				return nil, fmt.Errorf("collector %s already registered with incompatible type", opts.Name)
			}
			return existing, nil
		}
		return nil, err
	}
	return collector, nil
}
