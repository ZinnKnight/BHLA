package app

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	userpb "BHLA/proto/user_service"
	"BHLA/services/user-service/internal/adapters/eventlog"
	"BHLA/shared/config"
	"BHLA/shared/grpc/interceptors/errmap"
	"BHLA/shared/grpc/interceptors/panicrecover"
	"BHLA/shared/grpc/interceptors/sessionauth"
	"BHLA/shared/grpc/interceptors/validation"
	"BHLA/shared/logging"
	"BHLA/shared/logging/zapadapter"
	"BHLA/shared/metrics"
	"BHLA/shared/postgres"
	"BHLA/shared/redisclient"
	"BHLA/shared/sessionvalidation"
	"BHLA/shared/txmanager"

	"BHLA/services/user-service/internal/adapters/grpcadapter"
	"BHLA/services/user-service/internal/adapters/postgresadapter"
	"BHLA/services/user-service/internal/usecase"
)

const publicUserRegistration = "/user_service.UserService/UserRegistration"

type App struct {
	cfg        *config.Config
	logger     logging.Logger
	pool       *pgxpool.Pool
	redis      *redisclient.Client
	grpcServer *grpc.Server
	metricsRec *metrics.PrometheusRecord
}

func New(ctx context.Context) (*App, error) {
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	logger, err := zapadapter.New()
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

	redis, err := redisclient.New(ctx, redisclient.Config{
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

	repo := postgresadapter.NewUserRepo(pool)
	emitter := eventlog.New(logger)
	txm := txmanager.NewTxManager(pool)
	uc := usecase.New(repo, emitter, txm, logger)
	handler := grpcadapter.NewHandler(uc, grpcadapter.NewStubPrerequisite(), logger)

	rec := metrics.NewPrometheusRecord()
	validator := sessionvalidation.NewRedisValidator(redis.Client)
	authn := sessionauth.New(validator, logger, publicUserRegistration)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			panicrecover.UnaryServerInterceptor(logger),
			metrics.UnaryServerInterceptor(rec),
			errmap.UnaryServerInterceptor(logger),
			validation.UnaryServerInterceptor(),
			authn.UnaryServerInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			panicrecover.StreamServerInterceptor(logger),
			metrics.StreamServerInterceptor(rec),
			errmap.StreamServerInterceptor(logger),
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
		if err := metrics.StartMetricsServer(ctx, a.cfg.MetricsPort, a.metricsRec.Registry()); err != nil {
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
