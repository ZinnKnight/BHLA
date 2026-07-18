package auth_context

import "context"

type Identity struct {
	UserID    string
	Role      string
	SessionID string
}

type ctxKey struct{}

func With(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

func From(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

func UserID(ctx context.Context) string {
	id, _ := From(ctx)
	return id.UserID
}
