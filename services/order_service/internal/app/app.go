package app

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	orderpb "BHLA/proto/order_service"
	sagapb "BHLA/proto/saga_events"
	"BHLA/shared/config"
	"BHLA/shared/events"
	"BHLA/shared/grpc/interceptors/err_map"
	"BHLA/shared/grpc/interceptors/panic_recover"
	"BHLA/shared/grpc/interceptors/session_auth"
	"BHLA/shared/grpc/interceptors/validation"
	"BHLA/shared/idempotency"
	"BHLA/shared/kafka"
	"BHLA/shared/logging"
	"BHLA/shared/logging/zap_adapter"
	"BHLA/shared/metrics"
	"BHLA/shared/outbox"
	"BHLA/shared/policy"
	"BHLA/shared/postgres"
	"BHLA/shared/quota"
	"BHLA/shared/rate_limiter"
	"BHLA/shared/redis_client"
	"BHLA/shared/saga_topics"
	"BHLA/shared/session_validation"
	"BHLA/shared/tx_manager"

	"BHLA/services/order_service/internal/adapters/grpc_adapter"
	"BHLA/services/order_service/internal/adapters/postgres_adapter"
	"BHLA/services/order_service/internal/saga"
	"BHLA/services/order_service/internal/streaming"
	"BHLA/services/order_service/internal/usecase"
)

type App struct {
	cfg            *config.Config
	logger         logging.Logger
	pool           *pgxpool.Pool
	redis          *redis_client.Client
	producer       *kafka.Producer
	relay          *outbox.Relay
	orderConsumer  *kafka.Consumer
	orchestrator   *saga.Orchestrator
	statusConsumer *kafka.Consumer
	hub            *streaming.Hub
	grpcServer     *grpc.Server
	listener       net.Listener
	metricsRec     *metrics.PrometheusRecorder
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
		AfterConn: func(ctx context.Context, conn *pgx.Conn) error {
			pgxdecimal.Register(conn.TypeMap())
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}

	redis, err := redis_client.New(ctx, redis_client.Config{
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

	txm := tx_manager.NewTxManager(pool)

	topicResolver := func(e events.Event) string {
		switch e.EventType {
		case saga_topics.EventOrderCreated:
			return saga_topics.TopicOrderEvents
		case saga_topics.CommandReserveStock:
			return saga_topics.TopicSagaCommands
		case saga_topics.EventOrderStatusChanged:
			return saga_topics.TopicOrderStatus
		default:
			return e.AggregationType + ".events"
		}
	}
	writer := outbox.NewWriter(pool, topicResolver)
	relay := outbox.NewRelay(pool, producer, logger, 100, time.Second)

	orderRepo := postgres_adapter.NewOrderRepo(pool)
	orchestrator := saga.NewOrchestrator(orderRepo, txm, writer, idempotency.NewGuard(pool, "order-orchestrator"), logger)

	orderConsumer, err := kafka.NewConsumer(ctx, kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers,
		Group:   "order-orchestrator",
		Topics:  []string{saga_topics.TopicOrderEvents, saga_topics.TopicSagaReplies},
	}, logger)
	if err != nil {
		producer.Close()
		_ = redis.Close()
		pool.Close()
		return nil, fmt.Errorf("kafka orchestrator consumer: %w", err)
	}

	hub := streaming.NewHub()

	statusConsumer, err := kafka.NewConsumer(ctx, kafka.ConsumerConfig{
		Brokers:    cfg.KafkaBrokers,
		Group:      "order-status-stream-" + uuid.NewString(),
		Topics:     []string{saga_topics.TopicOrderStatus},
		StartAtEnd: true,
	}, logger)
	if err != nil {
		orderConsumer.Close()
		producer.Close()
		_ = redis.Close()
		pool.Close()
		return nil, fmt.Errorf("kafka status consumer: %w", err)
	}

	provider, err := policy.NewStaticProvider()
	if err != nil {
		statusConsumer.Close()
		orderConsumer.Close()
		producer.Close()
		_ = redis.Close()
		pool.Close()
		return nil, fmt.Errorf("policy: %w", err)
	}
	limiter := rate_limiter.NewRateLimiter(redis.Client, cfg.RateLimitPerMin, time.Minute)
	enforcer := quota.NewEnforced(provider, limiter)

	uc := usecase.New(orderRepo, writer, txm, enforcer, logger)
	handler := grpc_adapter.NewHandler(uc, hub, logger)

	rec := metrics.NewPrometheusRecorder()
	validator := session_validation.NewRedisValidator(redis.Client)
	authn := session_auth.New(validator, logger)

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
	orderpb.RegisterOrderServiceServer(grpcServer, handler)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		statusConsumer.Close()
		orderConsumer.Close()
		producer.Close()
		_ = redis.Close()
		pool.Close()
		return nil, fmt.Errorf("listen: %w", err)
	}

	return &App{
		cfg: cfg, logger: logger, pool: pool, redis: redis, producer: producer, relay: relay,
		orderConsumer: orderConsumer, orchestrator: orchestrator, statusConsumer: statusConsumer,
		hub: hub, grpcServer: grpcServer, listener: lis, metricsRec: rec,
	}, nil
}

func (a *App) publishStatusUpdate(_ context.Context, msg kafka.Message) error {
	if msg.Header["event_type"] != saga_topics.EventOrderStatusChanged {
		return nil
	}
	var p sagapb.OrderStatusChanged
	if err := proto.Unmarshal(msg.Value, &p); err != nil {
		a.logger.LogError("orderApp: bad OrderStatusChanged payload", logging.Err(err))
		return nil
	}
	a.hub.Publish(streaming.Update{OrderID: p.OrderId, Status: p.Status})
	return nil
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
		if err := metrics.StartMetricsServer(workerCtx, a.cfg.MetricsPort, a.metricsRec.Handler()); err != nil {
			a.logger.LogError("metrics server stopped", logging.Err(err))
		}
	}()
	go a.runWorker(workerCtx, "outbox-relay", a.relay.Run)
	go a.runWorker(workerCtx, "saga-orchestrator", func(ctx context.Context) { a.orderConsumer.Run(ctx, a.orchestrator.Handle) })
	go a.runWorker(workerCtx, "status-stream", func(ctx context.Context) { a.statusConsumer.Run(ctx, a.publishStatusUpdate) })

	serveErr := make(chan error, 1)
	go func() {
		a.logger.LogInfo("order-service serving",
			logging.Field{Key: "grpc_port", Value: a.cfg.GRPCPort},
			logging.Field{Key: "metrics_port", Value: a.cfg.MetricsPort})
		serveErr <- a.grpcServer.Serve(a.listener)
	}()

	var runErr error
	select {
	case <-ctx.Done():
		a.logger.LogInfo("order-service shutting down")
	case err := <-serveErr:
		runErr = err
	}

	workerCancel()
	a.grpcServer.GracefulStop()
	a.shutdown()
	return runErr
}

func (a *App) shutdown() {
	a.statusConsumer.Close()
	a.orderConsumer.Close()
	a.producer.Close()
	_ = a.redis.Close()
	a.pool.Close()
	_ = a.logger.Sync()
}
