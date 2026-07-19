package grpc_adapter

import (
	"BHLA/shared/grpc/interceptors/err_map"
	"BHLA/shared/session_validation"

	"BHLA/services/auth_service/internal/domain"
)

func init() {
	err_map.RegisterError(domain.ErrInvalidCredentials, err_map.Unauthenticated, "неверный логин или пароль")
	err_map.RegisterError(domain.ErrTooManyLoginAttempts, err_map.RateLimited, "исчерпан лимит попыток входа")
	err_map.RegisterError(session_validation.ErrSessionNotFound, err_map.Unauthenticated, "сессия не найдена")
	err_map.RegisterError(session_validation.ErrSessionInvalid, err_map.Unauthenticated, "сессия недействительна")
}
