package grpc_adapter

import (
	"BHLA/shared/grpc/interceptors/errmap"

	"BHLA/services/market-service/internal/domain"
)

func init() {
	errmap.RegisterError(domain.ErrMarketNotFound, errmap.NotFound, "рынок не найден")
}
