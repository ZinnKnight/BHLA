package app

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	marketpb "BHLA/proto/market_service"
	"BHLA/shared/config"
	"BHLA/shared/events"
	"BHLA/shared/grpc/interceptors/errmap"
	"BHLA/shared/grpc/interceptors/panicrecover"
	"BHLA/shared/grpc/interceptors/sessionauth"
	"BHLA/shared/grpc/interceptors/validation"
	"BHLA/shared/idempotency"
	"BHLA/shared/kafka"
	"BHLA/shared/logging"
	"BHLA/shared/logging/zapadapter"
	"BHLA/shared/metrics"
	"BHLA/shared/outbox"
	"BHLA/shared/postgres"
	"BHLA/shared/redisclient"
	"BHLA/shared/sagatopics"
	"BHLA/shared/sessionvalidation"
	"BHLA/shared/txmanager"

	"BHLA/services/market-service/internal/adapters/grpcadapter"
	"BHLA/services/market-service/internal/adapters/postgresadapter"
	"BHLA/services/market-service/internal/saga"
	"BHLA/services/market-service/internal/usecase"
)

type App struct {
	cfg         *config.Config
	logger      logging.Logger
	pool        *pgxpool.Pool
	redis       *redisclient.Client
	producer    *kafka.Producer
	relay       *outbox.Relay
	cmdConsumer *kafka.Consumer
	participant *saga.Participant
	grpcServer  *grpc.Server
	listener    net.Listener
	metricsRec  *metrics.PrometheusRecord
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
		Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB,
		PoolSize: cfg.RedisPoolSize, MinIdleConns: cfg.RedisMinIdleConns,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("redis: %w", err)
	}

	producer, err := kafka.NewProducer(ctx, kafka.Config{Brokers: cfg.KafkaBrokers, ClientID: cfg.ServiceName})
	if err != nil {
		_ = redis.Close()
		pool.Close()
		return nil, fmt.Errorf("kafka producer: %w", err)
	}

	txm := txmanager.NewTxManager(pool)

	topicResolver := func(e events.Event) string {
		switch e.EventType {
		case sagatopics.EventStockReserved, sagatopics.EventStockRejected:
			return sagatopics.TopicSagaReplies
		default:
			return e.AggregationType + ".events"
		}
	}
	writer := outbox.NewWriter(pool, topicResolver)
	relay := outbox.NewRelay(pool, producer, logger, 100, time.Second)

	marketRepo := postgresadapter.NewMarketRepo(pool)
	participant := saga.NewParticipant(marketRepo, txm, writer, idempotency.NewGuard(pool, "market-reserve"), logger)

	cmdConsumer, err := kafka.NewConsumer(ctx, kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers,
		Group:   "market-reserve",
		Topics:  []string{sagatopics.TopicSagaCommands},
	}, logger)
	if err != nil {
		producer.Close()
		_ = redis.Close()
		pool.Close()
		return nil, fmt.Errorf("kafka command consumer: %w", err)
	}

	uc := usecase.New(marketRepo, logger)
	handler := grpcadapter.NewHandler(uc, logger)

	rec := metrics.NewPrometheusRecord()
	validator := sessionvalidation.NewRedisValidator(redis.Client)
	authn := sessionauth.New(validator, logger)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			panicrecover.UnaryServerInterceptor(logger),
			metrics.UnaryServerInterceptor(rec),
			errmap.UnaryServerInterceptor(logger),
			validation.UnaryServerInterceptor(),
			authn.UnaryServerInterceptor(),
		),
	)
	marketpb.RegisterSpotInstrumentServiceServer(grpcServer, handler)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		cmdConsumer.Close()
		producer.Close()
		_ = redis.Close()
		pool.Close()
		return nil, fmt.Errorf("listen: %w", err)
	}

	return &App{
		cfg: cfg, logger: logger, pool: pool, redis: redis, producer: producer, relay: relay,
		cmdConsumer: cmdConsumer, participant: participant, grpcServer: grpcServer, listener: lis, metricsRec: rec,
	}, nil
}

func (a *App) runWorker(ctx context.Context, name string, fn func(context.Context)) {
	a.logger.LogInfo("worker launched", logging.Field{Key: "worker", Value: name})
	defer func() {
		if r := recover(); r != nil {
			a.logger.LogError("worker PANIC recovered",
				logging.Field{Key: "worker", Value: name},
				logging.Field{Key: "panic", Value: r},
				logging.Field{Key: "stack", Value: string(debug.Stack())})
		}
		if ctx.Err() != nil {
			a.logger.LogInfo("worker stopped (shutdown)", logging.Field{Key: "worker", Value: name})
		} else {
			a.logger.LogError("worker stopped UNEXPECTEDLY", logging.Field{Key: "worker", Value: name})
		}
	}()
	fn(ctx)
}

func (a *App) Run(ctx context.Context) error {
	workerCtx, workerCancel := context.WithCancel(ctx)

	go func() {
		if err := metrics.StartMetricsServer(workerCtx, a.cfg.MetricsPort, a.metricsRec.Registry()); err != nil {
			a.logger.LogError("metrics server stopped", logging.Err(err))
		}
	}()
	go a.runWorker(workerCtx, "outbox-relay", a.relay.Run)
	go a.runWorker(workerCtx, "market-reserve", func(ctx context.Context) { a.cmdConsumer.Run(ctx, a.participant.HandleReserveStock) })

	serveErr := make(chan error, 1)
	go func() {
		a.logger.LogInfo("market-service serving",
			logging.Field{Key: "grpc_port", Value: a.cfg.GRPCPort},
			logging.Field{Key: "metrics_port", Value: a.cfg.MetricsPort})
		serveErr <- a.grpcServer.Serve(a.listener)
	}()

	var runErr error
	select {
	case <-ctx.Done():
		a.logger.LogInfo("market-service shutting down")
	case err := <-serveErr:
		runErr = err
	}

	workerCancel()
	a.grpcServer.GracefulStop()
	a.shutdown()
	return runErr
}

func (a *App) shutdown() {
	a.cmdConsumer.Close()
	a.producer.Close()
	_ = a.redis.Close()
	a.pool.Close()
	_ = a.logger.Sync()
}
