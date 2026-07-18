package ports

import (
	"context"

	"BHLA/services/user_service/internal/domain"
	"BHLA/shared/auth_roles"
)

type UserInbound interface {
	Register(ctx context.Context, userName, userPassword string) (*domain.User, error)
	GetUser(ctx context.Context, userID string) (*domain.User, error)
	PlanChange(ctx context.Context, userID string, newPlan auth_roles.Plan) (*domain.User, error)
}

type UserRepo interface {
	SaveUser(ctx context.Context, user *domain.User) error
	GetByID(ctx context.Context, userID string) (*domain.User, error)
	UpdatePlan(ctx context.Context, userID string, plan auth_roles.Plan) error
}
