package domain

import (
	"errors"

	"BHLA/shared/authroles"
)

type Credentials struct {
	UserID       string
	UserName     string
	PasswordHash string
	Role         authroles.Plan
}

var (
	ErrUserNotFound         = errors.New("user not found")
	ErrInvalidCredentials   = errors.New("invalid credentials")
	ErrTooManyLoginAttempts = errors.New("too many login attempts")
)
