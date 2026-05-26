package server

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	appmetrics "github.com/wb-test-task/search-trends/internal/metrics"
)

func TestUnaryMetricsInterceptorCountsStatusCode(t *testing.T) {
	t.Parallel()

	metrics, err := appmetrics.New(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("metrics.New() error = %v", err)
	}

	interceptor := unaryMetricsInterceptor(metrics)
	_, gotErr := interceptor(
		context.Background(),
		"request",
		&grpc.UnaryServerInfo{FullMethod: "/search_trends.v1.SearchTrendsService/GetTop"},
		func(context.Context, any) (any, error) {
			return nil, status.Error(codes.NotFound, "missing")
		},
	)
	if status.Code(gotErr) != codes.NotFound {
		t.Fatalf("code = %s, want %s", status.Code(gotErr), codes.NotFound)
	}

	got := testutil.ToFloat64(metrics.GRPCRequests.WithLabelValues(
		"/search_trends.v1.SearchTrendsService/GetTop",
		codes.NotFound.String(),
	))
	if got != 1 {
		t.Fatalf("grpc requests = %v, want 1", got)
	}
}
