package events

import "context"

type Event struct {
	AggregationType string
	AggregateID     string
	EventType       string
	PayLoad         []byte
	IdempotencyKey  string
}

type Emitter interface {
	Emit(ctx context.Context, event Event) error
}
