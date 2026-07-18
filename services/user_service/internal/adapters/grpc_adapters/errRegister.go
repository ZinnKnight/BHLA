package grpc_adapters

import (
	"BHLA/shared/grpc/interceptors/errmap"

	"BHLA/services/user-service/internal/domain"
)

func init() {
	errmap.RegisterError(domain.ErrUserNotFound, errmap.NotFound, "пользователь не найден")
	errmap.RegisterError(domain.ErrUserAlreadyExists, errmap.AlreadyExists, "пользователь уже существует")
	errmap.RegisterError(domain.ErrEmptyName, errmap.Invalid, "имя пользователя не может быть пустым")
	errmap.RegisterError(domain.ErrEmptyPassword, errmap.Invalid, "пароль не может быть пустым")
	errmap.RegisterError(domain.ErrInvalidPlan, errmap.Invalid, "некорректный тариф")
}
