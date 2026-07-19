package domain

import (
	"errors"

	"BHLA/shared/auth_roles"
)

type User struct {
	UserID       string
	UserName     string
	UserPassword string
	Role         auth_roles.Plan
}

func (u *User) ValidateUser() error {
	if u.UserName == "" {
		return ErrEmptyName
	}
	if u.UserPassword == "" {
		return ErrEmptyPassword
	}
	return nil
}

func CanSelfPlanChange(p auth_roles.Plan) bool {
	return p == auth_roles.Free || p == auth_roles.Pro
}

var (
	ErrUserNotFound      = errors.New("user not found")
	ErrUserAlreadyExists = errors.New("user already exists")
	ErrEmptyName         = errors.New("user name is empty")
	ErrEmptyPassword     = errors.New("user password is empty")
	ErrInvalidPlan       = errors.New("invalid plan")
)
