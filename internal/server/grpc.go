package server

import (
	"context"
	"errors"
	"fmt"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	appmetrics "github.com/wb-test-task/search-trends/internal/metrics"
	searchtrendspb "github.com/wb-test-task/search-trends/proto/search_trends/v1"
)

type GRPCConfig struct {
	Address string
}

type GRPCDependencies struct {
	TopProvider TopProvider
	StopWords   StopWords
	Metrics     *appmetrics.Metrics
}

type GRPCServer struct {
	address string
	server  *grpc.Server
	logger  *zap.Logger
}

func NewGRPCServer(cfg GRPCConfig, deps GRPCDependencies, logger *zap.Logger) (*GRPCServer, error) {
	if cfg.Address == "" {
		return nil, errors.New("grpc address is required")
	}
	if deps.TopProvider == nil {
		return nil, errors.New("top provider is required")
	}
	if deps.StopWords == nil {
		return nil, errors.New("stop words manager is required")
	}
	if logger == nil {
		return nil, errors.New("logger is required")
	}

	options := make([]grpc.ServerOption, 0, 1)
	if deps.Metrics != nil {
		options = append(options, grpc.ChainUnaryInterceptor(unaryMetricsInterceptor(deps.Metrics)))
	}

	grpcServer := grpc.NewServer(options...)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	trendsHandler, err := NewSearchTrendsHandler(deps.TopProvider, deps.StopWords, deps.Metrics, logger)
	if err != nil {
		return nil, fmt.Errorf("create search trends handler: %w", err)
	}
	searchtrendspb.RegisterSearchTrendsServiceServer(grpcServer, trendsHandler)

	reflection.Register(grpcServer)

	return &GRPCServer{
		address: cfg.Address,
		server:  grpcServer,
		logger:  logger,
	}, nil
}

func (s *GRPCServer) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		s.server.GracefulStop()
	}()

	s.logger.Info("gRPC server listening", zap.String("addr", s.address))

	if err := s.server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}

	return nil
}

func (s *GRPCServer) Server() *grpc.Server {
	return s.server
}
