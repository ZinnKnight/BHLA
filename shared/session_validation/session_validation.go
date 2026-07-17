package session_validation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type State int

const (
	StateUndefined State = iota
	StateActive
	StateInactive
	StateSuspended
)

type Session struct {
	SessionID  string    `json:"session_id"`
	UserID     string    `json:"user_id"`
	Role       string    `json:"role"`
	State      State     `json:"state"`
	ValidUntil time.Time `json:"valid_until"`
}

func (s Session) IsValid(now time.Time) bool {
	return s.State == StateActive && now.Before(s.ValidUntil)
}

var (
	ErrSessionNotFound = errors.New("sessionvalidation: session not found")
	ErrSessionInvalid  = errors.New("sessionvalidation: session inactive or expired")
)

type Validator interface {
	Validate(ctx context.Context, sessionID string) (Session, error)
}

func RedisKey(sessionID string) string { return "session:" + sessionID }

type RedisValidator struct {
	rdb   redis.Cmdable
	clock func() time.Time
}

func NewRedisValidator(rdb redis.Cmdable) *RedisValidator {
	return &RedisValidator{rdb: rdb, clock: time.Now}
}

func (v *RedisValidator) Validate(ctx context.Context, sessionID string) (Session, error) {
	raw, err := v.rdb.Get(ctx, RedisKey(sessionID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return Session{}, ErrSessionNotFound
		}
		return Session{}, fmt.Errorf("sessionvalidation: redis get: %w", err)
	}

	var s Session
	if err := json.Unmarshal(raw, &s); err != nil {
		return Session{}, fmt.Errorf("sessionvalidation: decode: %w", err)
	}

	if !s.IsValid(v.clock()) {
		return Session{}, ErrSessionInvalid
	}
	return s, nil
}
