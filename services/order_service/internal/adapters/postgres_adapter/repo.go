package postgres_adapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"BHLA/shared/txmanager"

	"BHLA/services/order-service/internal/domain"
	"BHLA/services/order-service/internal/ports"
)

var _ ports.OrderRepo = (*OrderRepo)(nil)

type OrderRepo struct {
	pool *pgxpool.Pool
}

func NewOrderRepo(pool *pgxpool.Pool) *OrderRepo { return &OrderRepo{pool: pool} }

type dbConn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (r *OrderRepo) connection(ctx context.Context) dbConn {
	if tx, ok := txmanager.ExtractManager(ctx); ok {
		return tx
	}
	return r.pool
}

func (r *OrderRepo) SaveOrder(ctx context.Context, order *domain.Order) error {
	const query = `INSERT INTO orders (order_id, user_id, market_id, price, amount, order_status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err := r.connection(ctx).Exec(ctx, query,
		order.OrderID, order.UserID, order.MarketID, order.Price, order.Amount,
		string(order.OrderStatus), order.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return domain.ErrOrderAlreadyExists
		}
		return fmt.Errorf("postgres: save order: %w", err)
	}
	return nil
}

func (r *OrderRepo) UpdateStatus(ctx context.Context, orderID, status string) error {
	const query = `UPDATE orders SET order_status = $1 WHERE order_id = $2`
	if _, err := r.connection(ctx).Exec(ctx, query, status, orderID); err != nil {
		return fmt.Errorf("postgres: update status: %w", err)
	}
	return nil
}

func (r *OrderRepo) FindByID(ctx context.Context, orderID, userID string) (*domain.Order, error) {
	const query = `SELECT order_id, user_id, market_id, price, amount, order_status, created_at
		FROM orders WHERE order_id = $1 AND user_id = $2`

	var o domain.Order
	err := r.connection(ctx).QueryRow(ctx, query, orderID, userID).
		Scan(&o.OrderID, &o.UserID, &o.MarketID, &o.Price, &o.Amount, &o.OrderStatus, &o.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrOrderNotFound
		}
		return nil, fmt.Errorf("postgres: find order by id: %w", err)
	}
	return &o, nil
}

func (r *OrderRepo) FindAll(ctx context.Context, userID, pageToken string, pageSize int) ([]*domain.Order, error) {
	const query = `SELECT order_id, user_id, market_id, price, amount, order_status, created_at
		FROM orders WHERE user_id = $1 AND order_id > $2
		ORDER BY order_id ASC LIMIT $3`

	rows, err := r.connection(ctx).Query(ctx, query, userID, pageToken, pageSize)
	if err != nil {
		return nil, fmt.Errorf("postgres: find all orders: %w", err)
	}
	defer rows.Close()

	var orders []*domain.Order
	for rows.Next() {
		var o domain.Order
		if err := rows.Scan(&o.OrderID, &o.UserID, &o.MarketID, &o.Price, &o.Amount, &o.OrderStatus, &o.CreatedAt); err != nil {
			return nil, fmt.Errorf("postgres: scan order: %w", err)
		}
		orders = append(orders, &o)
	}
	return orders, rows.Err()
}
