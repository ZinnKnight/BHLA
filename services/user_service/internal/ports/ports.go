package ports

import (
	"context"

	"BHLA/services/user-service/internal/domain"
	"BHLA/shared/authroles"
)

type UserInbound interface {
	Register(ctx context.Context, userName, userPassword string) (*domain.User, error)
	GetUser(ctx context.Context, userID string) (*domain.User, error)
	PlanChange(ctx context.Context, userID string, newPlan authroles.Plan) (*domain.User, error)
}

type UserRepo interface {
	SaveUser(ctx context.Context, user *domain.User) error
	GetByID(ctx context.Context, userID string) (*domain.User, error)
	UpdatePlan(ctx context.Context, userID string, plan authroles.Plan) error
}
