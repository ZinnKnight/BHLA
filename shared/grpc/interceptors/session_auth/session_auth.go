package session_auth

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"BHLA/shared/auth_context"
	"BHLA/shared/session_validation"
)

const authHeader = "authorization"

type Logger interface {
	LogError(msg string, err error)
}

type Authenticator struct {
	validator session_validation.Validator
	log       Logger
	public    map[string]struct{}
}

func New(v session_validation.Validator, log Logger, publicMethods ...string) *Authenticator {
	p := make(map[string]struct{}, len(publicMethods))
	for _, m := range publicMethods {
		p[m] = struct{}{}
	}
	return &Authenticator{validator: v, log: log, public: p}
}

func (a *Authenticator) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := a.public[info.FullMethod]; ok {
			return handler(ctx, req)
		}
		authCtx, err := a.authenticate(ctx)
		if err != nil {
			return nil, err
		}
		return handler(authCtx, req)
	}
}

func (a *Authenticator) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if _, ok := a.public[info.FullMethod]; ok {
			return handler(srv, ss)
		}
		authCtx, err := a.authenticate(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, &authStream{ServerStream: ss, ctx: authCtx})
	}
}

type authStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authStream) Context() context.Context { return s.ctx }

func (a *Authenticator) authenticate(ctx context.Context) (context.Context, error) {
	sessionID, err := sessionIDFromMetadata(ctx)
	if err != nil {
		return nil, err
	}

	sess, err := a.validator.Validate(ctx, sessionID)
	if err != nil {
		switch {
		case errors.Is(err, sessionvalidation.ErrSessionNotFound),
			errors.Is(err, sessionvalidation.ErrSessionInvalid):
			// Сессии нет / она невалидна -> клиенту НЕ раскрываем детали.
			return nil, status.Error(codes.Unauthenticated, "invalid or expired session")
		default:
			// ИНФРА-ошибка (Redis недоступен): fail-CLOSED (для auth иначе нельзя), но код
			// Unavailable — сигнал, что это временно и запрос можно повторить.
			a.log.LogError("session validation failed (infra)", err)
			return nil, status.Error(codes.Unavailable, "auth backend unavailable")
		}
	}

	ctx = authcontext.With(ctx, authcontext.Identity{
		UserID:    sess.UserID,
		Role:      sess.Role,
		SessionID: sess.SessionID,
	})
	_ = grpc.SetHeader(ctx, metadata.Pairs("x-user-id", sess.UserID))
	return ctx, nil
}

func sessionIDFromMetadata(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get(authHeader)
	if len(vals) == 0 || strings.TrimSpace(vals[0]) == "" {
		return "", status.Error(codes.Unauthenticated, "missing authorization")
	}
	token := strings.TrimSpace(vals[0])
	// Срезаем необязательный префикс схемы.
	if i := strings.IndexByte(token, ' '); i >= 0 {
		if scheme := strings.ToLower(token[:i]); scheme == "bearer" {
			token = strings.TrimSpace(token[i+1:])
		}
	}
	if token == "" {
		return "", status.Error(codes.Unauthenticated, "empty session id")
	}
	return token, nil
}
