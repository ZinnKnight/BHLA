CREATE TABLE IF NOT EXISTS orders (
                                      order_id     UUID PRIMARY KEY,
                                      user_id      UUID        NOT NULL,
                                      market_id    UUID        NOT NULL,
                                      price        NUMERIC     NOT NULL,
                                      amount       NUMERIC     NOT NULL,
                                      order_status TEXT        NOT NULL,
                                      created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );

CREATE INDEX IF NOT EXISTS idx_orders_user_id_order_id ON orders (user_id, order_id);
