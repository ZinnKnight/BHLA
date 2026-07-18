package grpc_adapters

import (
	"BHLA/shared/grpc/interceptors/err_map"

	"BHLA/services/user_service/internal/domain"
)

func init() {
	err_map.RegisterError(domain.ErrUserNotFound, err_map.NotFound, "пользователь не найден")
	err_map.RegisterError(domain.ErrUserAlreadyExists, err_map.AlreadyExists, "пользователь уже существует")
	err_map.RegisterError(domain.ErrEmptyName, err_map.Invalid, "имя пользователя не может быть пустым")
	err_map.RegisterError(domain.ErrEmptyPassword, err_map.Invalid, "пароль не может быть пустым")
	err_map.RegisterError(domain.ErrInvalidPlan, err_map.Invalid, "некорректный тариф")
}
