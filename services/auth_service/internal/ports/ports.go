package ports

import (
	"context"
	"time"

	"BHLA/shared/policy"
	"BHLA/shared/quota"
	"BHLA/shared/sessionvalidation"

	"BHLA/services/auth-service/internal/domain"
)

type LoginResult struct {
	UserID    string
	SessionID string
}

type AuthInbound interface {
	Login(ctx context.Context, userName, userPassword string) (LoginResult, error)
	ValidateSession(ctx context.Context, sessionID string) (sessionvalidation.Session, error)
}

type CredentialRepo interface {
	GetByName(ctx context.Context, userName string) (*domain.Credentials, error)
}

type SessionWriter interface {
	Save(ctx context.Context, sess sessionvalidation.Session, ttl time.Duration) error
}

type SessionReader interface {
	Validate(ctx context.Context, sessionID string) (sessionvalidation.Session, error)
}

type QuotaChecker interface {
	Check(ctx context.Context, plan string, action policy.Action, subject string) (quota.Decision, error)
}
