package app

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	authpb "BHLA/proto/auth_service"
	"BHLA/shared/config"
	"BHLA/shared/grpc/interceptors/err_map"
	"BHLA/shared/grpc/interceptors/panic_recover"
	"BHLA/shared/grpc/interceptors/validation"
	"BHLA/shared/logging"
	"BHLA/shared/logging/zap_adapter"
	"BHLA/shared/metrics"
	"BHLA/shared/policy"
	"BHLA/shared/postgres"
	"BHLA/shared/quota"
	"BHLA/shared/rate_limiter"
	"BHLA/shared/redis_client"
	"BHLA/shared/session_validation"

	"BHLA/services/auth_service/internal/adapters/grpc_adapter"
	"BHLA/services/auth_service/internal/adapters/postgres_adapter"
	"BHLA/services/auth_service/internal/usecase"
)

type App struct {
	cfg        *config.Config
	logger     logging.Logger
	pool       *pgxpool.Pool
	redis      *redis_client.Client
	grpcServer *grpc.Server
	metricsRec *metrics.PrometheusRecord
}

func New(ctx context.Context) (*App, error) {
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	logger, err := zap_adapter.New()
	if err != nil {
		return nil, fmt.Errorf("logger: %w", err)
	}

	pool, err := postgres.NewPool(ctx, postgres.Config{
		DatabaseURL:    cfg.DatabaseURL,
		MaxConnections: int32(cfg.DBMaxConn),
		MinConnections: int32(cfg.DBMinConn),
		MaxConnTTL:     time.Duration(cfg.DBMaxConnTTL) * time.Minute,
		MaxConnIdleTTL: time.Duration(cfg.DBMaxConnIdTTL) * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}

	rdb, err := redis_client.New(ctx, redis_client.Config{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           cfg.RedisDB,
		PoolSize:     cfg.RedisPoolSize,
		MinIdleConns: cfg.RedisMinIdleConns,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("redis: %w", err)
	}

	provider, err := policy.NewStaticProvider()
	if err != nil {
		pool.Close()
		_ = rdb.Close()
		return nil, fmt.Errorf("policy: %w", err)
	}

	repo := postgres_adapter.NewCredentialRepo(pool)
	sessionStore := session_validation.NewStore(rdb.Client)
	sessionReader := session_validation.NewRedisValidator(rdb.Client)
	limiter := rate_limiter.NewRateLimiter(rdb.Client, cfg.RateLimitPerMin, time.Minute) // окно берётся из quota на AllowKey
	enforcer := quota.NewEnforced(provider, limiter)
	sessionTTL := time.Duration(cfg.SessionTTLSeconds) * time.Second

	uc := usecase.New(repo, sessionStore, sessionReader, enforcer, sessionTTL, logger)
	handler := grpc_adapter.NewHandler(uc, logger)

	rec := metrics.NewPrometheusRecord()
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			panic_recover.UnaryServerInterceptor(logger),
			metrics.UnaryServerInterceptor(rec),
			err_map.UnaryServerInterceptor(logger),
			validation.UnaryServerInterceptor(),
		),
	)
	authpb.RegisterAuthServiceServer(grpcServer, handler)

	return &App{cfg: cfg, logger: logger, pool: pool, redis: rdb, grpcServer: grpcServer, metricsRec: rec}, nil
}

func (a *App) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", a.cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	go func() {
		if err := metrics.StartMetricsServer(ctx, a.cfg.MetricsPort, a.metricsRec.Registry()); err != nil {
			a.logger.LogError("metrics server stopped", logging.Err(err))
		}
	}()

	serveErr := make(chan error, 1)
	go func() {
		a.logger.LogInfo("auth-service serving", logging.Field{Key: "grpc_port", Value: a.cfg.GRPCPort})
		serveErr <- a.grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		a.logger.LogInfo("auth-service shutting down")
		a.grpcServer.GracefulStop()
		a.shutdown()
		return nil
	case err := <-serveErr:
		a.shutdown()
		return err
	}
}

func (a *App) shutdown() {
	a.pool.Close()
	_ = a.redis.Close()
	_ = a.logger.Sync()
}
