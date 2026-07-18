package postges_adapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"BHLA/shared/authroles"
	"BHLA/shared/txmanager"

	"BHLA/services/user-service/internal/domain"
	"BHLA/services/user-service/internal/ports"
)

var _ ports.UserRepo = (*UserRepo)(nil)

type UserRepo struct {
	pool *pgxpool.Pool
}

func NewUserRepo(pool *pgxpool.Pool) *UserRepo { return &UserRepo{pool: pool} }

type dbConn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (r *UserRepo) connection(ctx context.Context) dbConn {
	if tx, ok := txmanager.ExtractManager(ctx); ok {
		return tx
	}
	return r.pool
}

func (r *UserRepo) SaveUser(ctx context.Context, user *domain.User) error {
	const query = `
		INSERT INTO users (user_id, user_name, user_password, user_role)
		VALUES ($1, $2, $3, $4)`

	_, err := r.connection(ctx).Exec(ctx, query, user.UserID, user.UserName, user.UserPassword, user.Role.String())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			return domain.ErrUserAlreadyExists
		}
		return fmt.Errorf("postgres: save user: %w", err)
	}
	return nil
}

func (r *UserRepo) GetByID(ctx context.Context, userID string) (*domain.User, error) {
	const query = `
		SELECT user_id, user_name, user_password, user_role
		FROM users WHERE user_id = $1`

	var u domain.User
	var role string
	err := r.connection(ctx).QueryRow(ctx, query, userID).Scan(&u.UserID, &u.UserName, &u.UserPassword, &role)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrUserNotFound
		}
		return nil, fmt.Errorf("postgres: get user by id: %w", err)
	}
	u.Role = authroles.Plan(role)
	return &u, nil
}

func (r *UserRepo) UpdatePlan(ctx context.Context, userID string, plan authroles.Plan) error {
	const query = `UPDATE users SET user_role = $1 WHERE user_id = $2`
	if _, err := r.connection(ctx).Exec(ctx, query, plan.String(), userID); err != nil {
		return fmt.Errorf("postgres: update plan: %w", err)
	}
	return nil
}
