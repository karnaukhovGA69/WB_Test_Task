package server

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	appmetrics "github.com/wb-test-task/search-trends/internal/metrics"
)

func unaryMetricsInterceptor(metrics *appmetrics.Metrics) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		startedAt := time.Now()

		resp, err := handler(ctx, req)

		method := info.FullMethod
		code := status.Code(err).String()

		metrics.ObserveGRPCRequest(method, code, time.Since(startedAt))

		return resp, err
	}
}
