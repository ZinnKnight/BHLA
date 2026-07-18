
CREATE TABLE IF NOT EXISTS outbox (
                                      id              UUID PRIMARY KEY,
                                      aggregate_type  TEXT        NOT NULL,
                                      aggregate_id    TEXT        NOT NULL,
                                      event_type      TEXT        NOT NULL,
                                      topic           TEXT        NOT NULL,
                                      payload         BYTEA       NOT NULL,
                                      idempotency_key TEXT        NOT NULL,
                                      created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
    );

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox (created_at)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS processed_events (
                                                consumer        TEXT        NOT NULL,
                                                idempotency_key TEXT        NOT NULL,
                                                processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, idempotency_key)
    );

