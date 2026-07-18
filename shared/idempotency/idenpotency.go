package idempotency

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"BHLA/shared/txmanager"
)

type Guard struct {
	pool     *pgxpool.Pool
	consumer string
}

func NewGuard(pool *pgxpool.Pool, consumer string) *Guard {
	return &Guard{pool: pool, consumer: consumer}
}

type dbExec interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (g *Guard) connection(ctx context.Context) dbExec {
	if tx, ok := txmanager.ExtractManager(ctx); ok {
		return tx
	}
	return g.pool
}

func (g *Guard) Acquire(ctx context.Context, idempotencyKey string) (bool, error) {
	const query = `
	INSERT INTO processed_events (consumer, idempotency_key)
	VALUES ($1, $2)
	ON CONFLICT (consumer, idempotency_key) DO NOTHING`

	tag, err := g.connection(ctx).Exec(ctx, query, g.consumer, idempotencyKey)
	if err != nil {
		return false, fmt.Errorf("idempotency: acquire %q: %w", idempotencyKey, err)
	}
	return tag.RowsAffected() == 1, nil
}
