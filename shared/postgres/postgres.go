package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DatabaseURL       string
	MaxConnections    int32
	MinConnections    int32
	MaxConnTTL        time.Duration
	MaxConnIdleTTL    time.Duration
	HealthCheckPeriod time.Duration
	AfterConn         func(ctx context.Context, conn *pgx.Conn) error
}

func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse url: %w", err)
	}
	if cfg.MaxConnections > 0 {
		poolCfg.MaxConns = cfg.MaxConnections
	}
	if cfg.MinConnections > 0 {
		poolCfg.MinConns = cfg.MinConnections
	}
	if cfg.MaxConnTTL > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnTTL
	}
	if cfg.MaxConnIdleTTL > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTTL
	}
	if cfg.AfterConn != nil {
		poolCfg.AfterConnect = cfg.AfterConn
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return pool, nil
}
