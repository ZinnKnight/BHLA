package quota

import (
	"context"
	"fmt"
	"time"

	"BHLA/shared/policy"
	"BHLA/shared/rate_limiter"
)

type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
}

type Enforced struct {
	provider policy.Provider
	limiter  *rate_limiter.Limiter
}

func NewEnforced(provider policy.Provider, limiter *rate_limiter.Limiter) *Enforced {
	return &Enforced{provider: provider, limiter: limiter}
}

func (e *Enforced) Check(ctx context.Context, plan string, action policy.Action, subject string) (Decision, error) {
	rule := e.provider.RuleFor(plan, action)
	if rule.Limit <= 0 {
		return Decision{Allowed: true}, nil // Unlimited — Redis не трогаем
	}

	key := fmt.Sprintf("%s_%s", subject, plan)
	res, err := e.limiter.AllowKey(ctx, key, rule.Limit, rule.Window)
	if err != nil {
		return Decision{}, fmt.Errorf("quota: check %s for %s: %w", action, subject, err)
	}
	return Decision{Allowed: res.Allowed, RetryAfter: res.RetryAfter}, nil
}
