package outbox

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"BHLA/shared/events"
	"BHLA/shared/txmanager"
)

type TopicResolver func(e events.Event) string

var _ events.Emitter = (*Writer)(nil)

type Writer struct {
	pool  *pgxpool.Pool
	solve TopicResolver
}

func NewWriter(pool *pgxpool.Pool, solve TopicResolver) *Writer {
	return &Writer{pool: pool, solve: solve}
}

type dbExec interface {
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
}

func (w *Writer) connection(ctx context.Context) dbExec {
	if tx, ok := txmanager.ExtractManager(ctx); ok {
		return tx
	}
	return w.pool
}

func (w *Writer) Emit(ctx context.Context, e events.Event) error {
	const query = `
	INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, topic, payload, idempotency_key)
	VALUES ($1, $2, $3, $4, $5, $6, $7)`

	// payload как []byte -> pgx запишет в BYTEA без текстового преобразования.
	_, err := w.connection(ctx).Exec(ctx, query,
		uuid.NewString(),
		e.AggregationType,
		e.AggregateID,
		e.EventType,
		w.solve(e),
		e.PayLoad,
		e.IdempotencyKey,
	)
	if err != nil {
		return fmt.Errorf("outbox: write event %q: %w", e.EventType, err)
	}
	return nil
}
