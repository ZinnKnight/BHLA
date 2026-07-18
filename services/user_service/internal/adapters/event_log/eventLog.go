package event_log

import (
	"context"

	"BHLA/shared/events"
	"BHLA/shared/logging"
)

var _ events.Emitter = (*LoggingEmitter)(nil)

type LoggingEmitter struct {
	log logging.Logger
}

func New(log logging.Logger) *LoggingEmitter { return &LoggingEmitter{log: log} }

func (e *LoggingEmitter) Emit(_ context.Context, ev events.Event) error {
	e.log.LogInfo("event emitted",
		logging.Field{Key: "event_type", Value: ev.EventType},
		logging.Field{Key: "aggregate_id", Value: ev.AggregateID},
	)
	return nil
}
