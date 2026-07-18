package postgres_adapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"BHLA/shared/authroles"

	"BHLA/services/auth-service/internal/domain"
	"BHLA/services/auth-service/internal/ports"
)

var _ ports.CredentialRepo = (*CredentialRepo)(nil)

type CredentialRepo struct {
	pool *pgxpool.Pool
}

func NewCredentialRepo(pool *pgxpool.Pool) *CredentialRepo { return &CredentialRepo{pool: pool} }

func (r *CredentialRepo) GetByName(ctx context.Context, userName string) (*domain.Credentials, error) {
	const query = `
		SELECT user_id, user_name, user_password, user_role
		FROM users WHERE user_name = $1`

	var c domain.Credentials
	var role string
	err := r.pool.QueryRow(ctx, query, userName).Scan(&c.UserID, &c.UserName, &c.PasswordHash, &role)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrUserNotFound
		}
		return nil, fmt.Errorf("postgres: get credentials: %w", err)
	}
	c.Role = authroles.Plan(role)
	return &c, nil
}
