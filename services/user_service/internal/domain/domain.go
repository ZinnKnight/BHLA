package domain

import (
	"errors"

	"BHLA/shared/authroles"
)

type User struct {
	UserID       string
	UserName     string
	UserPassword string
	Role         authroles.Plan
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

func CanSelfPlanChange(p authroles.Plan) bool {
	return p == authroles.Free || p == authroles.Pro
}

var (
	ErrUserNotFound      = errors.New("user not found")
	ErrUserAlreadyExists = errors.New("user already exists")
	ErrEmptyName         = errors.New("user name is empty")
	ErrEmptyPassword     = errors.New("user password is empty")
	ErrInvalidPlan       = errors.New("invalid plan")
)
