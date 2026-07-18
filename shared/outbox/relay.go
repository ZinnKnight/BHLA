package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"BHLA/shared/kafka"
	"BHLA/shared/logging"
)

type Relay struct {
	pool      *pgxpool.Pool
	producer  *kafka.Producer
	logger    logging.Logger
	batchSize int
	interval  time.Duration
}

func NewRelay(pool *pgxpool.Pool, producer *kafka.Producer, logger logging.Logger, batchSize int, interval time.Duration) *Relay {
	if batchSize <= 0 {
		batchSize = 100
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &Relay{pool: pool, producer: producer, logger: logger, batchSize: batchSize, interval: interval}
}

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.logger.LogInfo("outbox relay starting",
		logging.Field{Key: "interval", Value: r.interval.String()},
		logging.Field{Key: "batch_size", Value: r.batchSize},
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				n, err := r.BatchRun(ctx)
				if err != nil {
					r.logger.LogError("outbox relay batch failed", logging.Err(err))
					break
				}
				if n < r.batchSize {
					break // дренаж окончен
				}
			}
		}
	}
}

type outboxRow struct {
	id             string
	topic          string
	key            string
	payLoad        []byte
	eventType      string
	aggType        string
	idempotencyKey string
}

func (r *Relay) BatchRun(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox relay: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op после Commit; Postgres откатит при обрыве

	const query = `
	SELECT id, topic, aggregate_id, payload, event_type, aggregate_type, idempotency_key
	FROM outbox
	WHERE published_at IS NULL
	ORDER BY created_at
	LIMIT $1
	FOR UPDATE SKIP LOCKED`

	rows, err := tx.Query(ctx, query, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("outbox relay: select: %w", err)
	}

	var batch []outboxRow
	for rows.Next() {
		var row outboxRow
		if err := rows.Scan(&row.id, &row.topic, &row.key, &row.payLoad, &row.eventType, &row.aggType, &row.idempotencyKey); err != nil {
			rows.Close()
			return 0, fmt.Errorf("outbox relay: scan: %w", err)
		}
		batch = append(batch, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("outbox relay: rows: %w", err)
	}
	if len(batch) == 0 {
		return 0, nil
	}

	records := make([]kafka.Record, len(batch))
	for i, row := range batch {
		records[i] = kafka.Record{
			Topic: row.topic,
			Key:   []byte(row.key),
			Value: row.payLoad,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(row.eventType)},
				{Key: "aggregate_type", Value: []byte(row.aggType)},
				{Key: "idempotency_key", Value: []byte(row.idempotencyKey)},
			},
		}
	}

	errs := r.producer.PublishBatch(ctx, records)

	publishedIDs := make([]string, 0, len(batch))
	for i, row := range batch {
		if errs[i] != nil {
			r.logger.LogError("outbox relay publish row",
				logging.Field{Key: "topic", Value: row.topic},
				logging.Field{Key: "outbox_id", Value: row.id},
				logging.Err(errs[i]),
			)
			continue
		}
		publishedIDs = append(publishedIDs, row.id)
	}
	if len(publishedIDs) == 0 {
		return 0, nil
	}

	const markSQL = `UPDATE outbox SET published_at = NOW() WHERE id = ANY($1::uuid[])`
	if _, err := tx.Exec(ctx, markSQL, publishedIDs); err != nil {
		return 0, fmt.Errorf("outbox relay: mark: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("outbox relay: commit: %w", err)
	}
	return len(publishedIDs), nil
}
