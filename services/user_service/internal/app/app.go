package app

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	userpb "BHLA/proto/user_service"
	"BHLA/services/user_service/internal/adapters/event_log"
	"BHLA/shared/config"
	"BHLA/shared/grpc/interceptors/err_map"
	"BHLA/shared/grpc/interceptors/panic_recover"
	"BHLA/shared/grpc/interceptors/session_auth"
	"BHLA/shared/grpc/interceptors/validation"
	"BHLA/shared/logging"
	"BHLA/shared/logging/zap_adapter"
	"BHLA/shared/metrics"
	"BHLA/shared/postgres"
	"BHLA/shared/redis_client"
	"BHLA/shared/session_validation"
	"BHLA/shared/tx_manager"

	"BHLA/services/user_service/internal/adapters/grpc_adapters"
	"BHLA/services/user_service/internal/adapters/postges_adapter"
	"BHLA/services/user_service/internal/usecase"
)

const publicUserRegistration = "/user_service.UserService/UserRegistration"

type App struct {
	cfg        *config.Config
	logger     logging.Logger
	pool       *pgxpool.Pool
	redis      *redis_client.Client
	grpcServer *grpc.Server
	metricsRec *metrics.PrometheusRecorder
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

	redis, err := redis_client.New(ctx, redis_client.Config{
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

	repo := postges_adapter.NewUserRepo(pool)
	emitter := event_log.New(logger)
	txm := tx_manager.NewTxManager(pool)
	uc := usecase.New(repo, emitter, txm, logger)
	handler := grpc_adapters.NewHandler(uc, grpc_adapters.NewStubPrerequisite(), logger)

	rec := metrics.NewPrometheusRecorder()
	validator := session_validation.NewRedisValidator(redis.Client)
	authn := session_auth.New(validator, logger, publicUserRegistration)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			panic_recover.UnaryServerInterceptor(logger),
			metrics.UnaryServerInterceptor(rec),
			err_map.UnaryServerInterceptor(logger),
			validation.UnaryServerInterceptor(),
			authn.UnaryServerInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			panic_recover.StreamServerInterceptor(logger),
			metrics.StreamServerInterceptor(rec),
			err_map.StreamServerInterceptor(logger),
			authn.StreamServerInterceptor(),
		),
	)
	userpb.RegisterUserServiceServer(grpcServer, handler)

	return &App{cfg: cfg, logger: logger, pool: pool, redis: redis, grpcServer: grpcServer, metricsRec: rec}, nil
}

func (a *App) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", a.cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	go func() {
		if err := metrics.StartMetricsServer(ctx, a.cfg.MetricsPort, a.metricsRec.Handler()); err != nil {
			a.logger.LogError("metrics server stopped", logging.Err(err))
		}
	}()

	serveErr := make(chan error, 1)
	go func() {
		a.logger.LogInfo("user-service serving", logging.Field{Key: "grpc_port", Value: a.cfg.GRPCPort})
		serveErr <- a.grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		a.logger.LogInfo("user-service shutting down")
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
