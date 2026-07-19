package rate_limiter

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	rdb           *redis.Client
	limit         int
	slidingWindow time.Duration
}

type RateLimiterRes struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration
}

func NewRateLimiter(rdb *redis.Client, limit int, window time.Duration) *Limiter {
	return &Limiter{rdb: rdb, limit: limit, slidingWindow: window}
}

func (rl *Limiter) Allow(ctx context.Context, userID string) (*RateLimiterRes, error) {
	return rl.AllowKey(ctx, fmt.Sprintf("rate-limiting:%s", userID), rl.limit, rl.slidingWindow)
}

func (rl *Limiter) AllowKey(ctx context.Context, key string, limit int, window time.Duration) (*RateLimiterRes, error) {
	nowMs := time.Now().UnixMilli()
	windowMs := window.Milliseconds()
	member := fmt.Sprintf("%d-%s", nowMs, uuid.NewString())

	pipe := rl.rdb.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "0", strconv.FormatInt(nowMs-windowMs, 10))
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(nowMs), Member: member})
	countCmd := pipe.ZCard(ctx, key)
	pipe.PExpire(ctx, key, window)

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("ratelimiter: pipeline exec: %w", err)
	}

	count := countCmd.Val()
	if count <= int64(limit) {
		return &RateLimiterRes{Allowed: true, Remaining: limit - int(count)}, nil
	}

	if err := rl.rdb.ZRem(ctx, key, member).Err(); err != nil {
		_ = err
	}
	return &RateLimiterRes{
		Allowed:    false,
		RetryAfter: rl.retryAfter(ctx, key, nowMs, windowMs),
	}, nil
}

func (rl *Limiter) retryAfter(ctx context.Context, key string, nowMs, windowMs int64) time.Duration {
	oldest, err := rl.rdb.ZRangeWithScores(ctx, key, 0, 0).Result()
	if err != nil || len(oldest) == 0 {
		return time.Duration(windowMs) * time.Millisecond
	}
	freeAt := int64(oldest[0].Score) + windowMs
	if d := freeAt - nowMs; d > 0 {
		return time.Duration(d) * time.Millisecond
	}
	return 0
}
