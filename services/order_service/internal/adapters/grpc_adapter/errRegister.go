package grpc_adapter

import (
	"BHLA/shared/grpc/interceptors/errmap"

	"BHLA/services/order-service/internal/domain"
)

func init() {
	errmap.RegisterError(domain.ErrOrderNotFound, errmap.NotFound, "заказ не найден")
	errmap.RegisterError(domain.ErrOrderQuotaExceeded, errmap.RateLimited, "достигнут лимит заказов")
	errmap.RegisterError(domain.ErrInvalidUserID, errmap.Invalid, "некорректный идентификатор пользователя")
	errmap.RegisterError(domain.ErrInvalidMarketID, errmap.Invalid, "некорректный идентификатор рынка")
	errmap.RegisterError(domain.ErrInvalidPrice, errmap.Invalid, "некорректная цена заказа")
	errmap.RegisterError(domain.ErrInvalidAmount, errmap.Invalid, "некорректное количество в заказе")
	errmap.RegisterError(domain.ErrOrderAlreadyExists, errmap.AlreadyExists, "заказ уже существует")
}
