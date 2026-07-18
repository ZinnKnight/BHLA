package domain

import (
	"errors"

	"BHLA/shared/auth_roles"
)

type Credentials struct {
	UserID       string
	UserName     string
	PasswordHash string
	Role         auth_roles.Plan
}

var (
	ErrUserNotFound         = errors.New("user not found")
	ErrInvalidCredentials   = errors.New("invalid credentials")
	ErrTooManyLoginAttempts = errors.New("too many login attempts")
)
