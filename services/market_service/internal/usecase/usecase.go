package usecase

import (
	"context"
	"fmt"

	"BHLA/shared/logging"

	"BHLA/services/market-service/internal/domain"
	"BHLA/services/market-service/internal/ports"
)

const (
	defaultPageSize = 10
	maxPageSize     = 50
)

var _ ports.MarketInbound = (*UseCase)(nil)

type UseCase struct {
	repo   ports.MarketRepo
	logger logging.Logger
}

func New(repo ports.MarketRepo, logger logging.Logger) *UseCase {
	return &UseCase{repo: repo, logger: logger}
}

func (uc *UseCase) ViewMarketByID(ctx context.Context, marketID string) (*domain.Market, error) {
	market, err := uc.repo.FindByID(ctx, marketID)
	if err != nil {
		return nil, fmt.Errorf("usecase view market: %w", err)
	}
	return market, nil
}

func (uc *UseCase) ViewAllMarkets(ctx context.Context, pageSize int, cursor string) ([]*domain.Market, string, error) {
	if pageSize <= 0 || pageSize > maxPageSize {
		pageSize = defaultPageSize
	}

	markets, err := uc.repo.FindAll(ctx, pageSize+1, cursor)
	if err != nil {
		return nil, "", fmt.Errorf("usecase view all markets: %w", err)
	}

	var next string
	if len(markets) > pageSize {
		next = markets[pageSize-1].MarketID
		markets = markets[:pageSize]
	}
	return markets, next, nil
}
