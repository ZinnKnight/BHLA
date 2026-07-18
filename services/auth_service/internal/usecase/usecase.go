package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"BHLA/shared/logging"
	"BHLA/shared/passwordhash"
	"BHLA/shared/policy"
	"BHLA/shared/sessionvalidation"

	"BHLA/services/auth-service/internal/domain"
	"BHLA/services/auth-service/internal/ports"
)

var _ ports.AuthInbound = (*AuthUseCase)(nil)

type AuthUseCase struct {
	repo       ports.CredentialRepo
	sessions   ports.SessionWriter
	reader     ports.SessionReader
	quota      ports.QuotaChecker
	sessionTTL time.Duration
	logger     logging.Logger
}

func New(repo ports.CredentialRepo, sessions ports.SessionWriter, reader ports.SessionReader,
	quota ports.QuotaChecker, sessionTTL time.Duration, logger logging.Logger) *AuthUseCase {
	return &AuthUseCase{repo: repo, sessions: sessions, reader: reader, quota: quota, sessionTTL: sessionTTL, logger: logger}
}

func (uc *AuthUseCase) Login(ctx context.Context, userName, userPassword string) (ports.LoginResult, error) {
	cred, err := uc.repo.GetByName(ctx, userName)
	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			return ports.LoginResult{}, domain.ErrInvalidCredentials // без энумерации
		}
		return ports.LoginResult{}, fmt.Errorf("auth login: %w", err)
	}

	if uc.quota != nil {
		dec, qerr := uc.quota.Check(ctx, cred.Role.String(), policy.ActionLogin, cred.UserName)
		if qerr != nil {
			uc.logger.LogError("login quota check", logging.Field{Key: "user", Value: cred.UserName}, logging.Err(qerr))
		} else if !dec.Allowed {
			return ports.LoginResult{}, domain.ErrTooManyLoginAttempts
		}
	}

	if err := passwordhash.Verify(cred.PasswordHash, userPassword); err != nil {
		return ports.LoginResult{}, domain.ErrInvalidCredentials
	}

	sess := sessionvalidation.Session{
		SessionID:  uuid.NewString(),
		UserID:     cred.UserID,
		Role:       cred.Role.String(),
		State:      sessionvalidation.StateActive,
		ValidUntil: time.Now().Add(uc.sessionTTL),
	}
	if err := uc.sessions.Save(ctx, sess, uc.sessionTTL); err != nil {
		return ports.LoginResult{}, fmt.Errorf("auth login: save session: %w", err)
	}
	return ports.LoginResult{UserID: cred.UserID, SessionID: sess.SessionID}, nil
}

func (uc *AuthUseCase) ValidateSession(ctx context.Context, sessionID string) (sessionvalidation.Session, error) {
	return uc.reader.Validate(ctx, sessionID)
}
