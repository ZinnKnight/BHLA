package postgres_adapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"BHLA/shared/txmanager"

	"BHLA/services/market_service/internal/domain"
	"BHLA/services/market_service/internal/ports"
)

var _ ports.MarketRepo = (*MarketRepo)(nil)

type MarketRepo struct {
	pool *pgxpool.Pool
}

func NewMarketRepo(pool *pgxpool.Pool) *MarketRepo { return &MarketRepo{pool: pool} }

type dbConn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (r *MarketRepo) connection(ctx context.Context) dbConn {
	if tx, ok := txmanager.ExtractManager(ctx); ok {
		return tx
	}
	return r.pool
}

func (r *MarketRepo) FindByID(ctx context.Context, marketID string) (*domain.Market, error) {
	const query = `SELECT market_id, market_name, goods_id, accessibility, ttl FROM markets WHERE market_id = $1`

	var m domain.Market
	err := r.connection(ctx).QueryRow(ctx, query, marketID).
		Scan(&m.MarketID, &m.MarketName, &m.GoodsID, &m.Accessibility, &m.TTL)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrMarketNotFound
		}
		return nil, fmt.Errorf("postgres: find market by id: %w", err)
	}
	return &m, nil
}

func (r *MarketRepo) FindAll(ctx context.Context, limit int, cursor string) ([]*domain.Market, error) {
	const query = `SELECT market_id, market_name, goods_id, accessibility, ttl
		FROM markets WHERE market_id > $1
		ORDER BY market_id ASC LIMIT $2`

	rows, err := r.connection(ctx).Query(ctx, query, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: find all markets: %w", err)
	}
	defer rows.Close()

	var markets []*domain.Market
	for rows.Next() {
		var m domain.Market
		if err := rows.Scan(&m.MarketID, &m.MarketName, &m.GoodsID, &m.Accessibility, &m.TTL); err != nil {
			return nil, fmt.Errorf("postgres: scan market: %w", err)
		}
		markets = append(markets, &m)
	}
	return markets, rows.Err()
}

func (r *MarketRepo) SaveReservation(ctx context.Context, orderID, marketID, status string) error {
	const query = `
		INSERT INTO reservations (order_id, market_id, status)
		VALUES ($1, $2, $3)
		ON CONFLICT (order_id) DO NOTHING`

	if _, err := r.connection(ctx).Exec(ctx, query, orderID, marketID, status); err != nil {
		return fmt.Errorf("postgres: save reservation: %w", err)
	}
	return nil
}
