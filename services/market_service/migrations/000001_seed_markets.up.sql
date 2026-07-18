INSERT INTO markets (market_id, market_name, goods_id, accessibility) VALUES
                                                                          ('11111111-1111-1111-1111-111111111111', 'BTC-USD', '22222222-2222-2222-2222-222222222222', TRUE),
                                                                          ('33333333-3333-3333-3333-333333333333', 'ETH-USD', '44444444-4444-4444-4444-444444444444', TRUE),
                                                                          ('55555555-5555-5555-5555-555555555555', 'CLOSED-MKT', '66666666-6666-6666-6666-666666666666', FALSE)
    ON CONFLICT (market_id) DO NOTHING;

