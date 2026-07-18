CREATE TABLE IF NOT EXISTS reservations (
                                            order_id   UUID PRIMARY KEY,
                                            market_id  UUID NOT NULL,
                                            status     TEXT NOT NULL,
                                            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
