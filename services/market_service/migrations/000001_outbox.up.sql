CREATE TABLE IF NOT EXISTS markets (
                                       market_id     UUID PRIMARY KEY,
                                       market_name   TEXT NOT NULL UNIQUE,
                                       goods_id      UUID NOT NULL,
                                       accessibility BOOLEAN NOT NULL DEFAULT TRUE,
                                       ttl           TIMESTAMPTZ
);
