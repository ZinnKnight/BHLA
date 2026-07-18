package grpc_adapter

import (
	"BHLA/shared/grpc/interceptors/err_map"

	"BHLA/services/market_service/internal/domain"
)

func init() {
	err_map.RegisterError(domain.ErrMarketNotFound, err_map.NotFound, "рынок не найден")
}
