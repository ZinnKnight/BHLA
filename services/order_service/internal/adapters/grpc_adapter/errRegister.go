package grpc_adapter

import (
	"BHLA/shared/grpc/interceptors/err_map"

	"BHLA/services/order_service/internal/domain"
)

func init() {
	err_map.RegisterError(domain.ErrOrderNotFound, err_map.NotFound, "заказ не найден")
	err_map.RegisterError(domain.ErrOrderQuotaExceeded, err_map.RateLimited, "достигнут лимит заказов")
	err_map.RegisterError(domain.ErrInvalidUserID, err_map.Invalid, "некорректный идентификатор пользователя")
	err_map.RegisterError(domain.ErrInvalidMarketID, err_map.Invalid, "некорректный идентификатор рынка")
	err_map.RegisterError(domain.ErrInvalidPrice, err_map.Invalid, "некорректная цена заказа")
	err_map.RegisterError(domain.ErrInvalidAmount, err_map.Invalid, "некорректное количество в заказе")
	err_map.RegisterError(domain.ErrOrderAlreadyExists, err_map.AlreadyExists, "заказ уже существует")
}
