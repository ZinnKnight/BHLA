package ports

import (
	"context"

	"BHLA/services/market_service/internal/domain"
)

type MarketInbound interface {
	ViewMarketByID(ctx context.Context, marketID string) (*domain.Market, error)
	ViewAllMarkets(ctx context.Context, pageSize int, cursor string) ([]*domain.Market, string, error)
}

type MarketRepo interface {
	FindByID(ctx context.Context, marketID string) (*domain.Market, error)
	FindAll(ctx context.Context, limit int, cursor string) ([]*domain.Market, error)
	SaveReservation(ctx context.Context, orderID, marketID, status string) error
}
