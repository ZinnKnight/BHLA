package ports

import (
	"context"

	"BHLA/services/order_service/internal/domain"
)

type OrderInbound interface {
	CreateOrder(ctx context.Context, cmd domain.CreateOrderCmd) (*domain.Order, error)
	GetOrderStatusByID(ctx context.Context, orderID, userID string) (*domain.Order, error)
	GetOrderStatusAll(ctx context.Context, userID, pageToken string, pageSize int) ([]*domain.Order, string, error)
}

type OrderRepo interface {
	SaveOrder(ctx context.Context, order *domain.Order) error
	FindByID(ctx context.Context, orderID, userID string) (*domain.Order, error)
	FindAll(ctx context.Context, userID, pageToken string, pageSize int) ([]*domain.Order, error)
	UpdateStatus(ctx context.Context, orderID, status string) error
}
