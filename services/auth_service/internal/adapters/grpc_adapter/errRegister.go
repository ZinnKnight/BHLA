package grpc_adapter

import (
	"BHLA/shared/grpc/interceptors/errmap"
	"BHLA/shared/sessionvalidation"

	"BHLA/services/auth-service/internal/domain"
)

func init() {
	errmap.RegisterError(domain.ErrInvalidCredentials, errmap.Unauthenticated, "неверный логин или пароль")
	errmap.RegisterError(domain.ErrTooManyLoginAttempts, errmap.RateLimited, "исчерпан лимит попыток входа")
	errmap.RegisterError(sessionvalidation.ErrSessionNotFound, errmap.Unauthenticated, "сессия не найдена")
	errmap.RegisterError(sessionvalidation.ErrSessionInvalid, errmap.Unauthenticated, "сессия недействительна")
}
