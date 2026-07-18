package zap_adapter

import (
	"go.uber.org/zap"

	"BHLA/shared/logging"
)

var _ logging.Logger = (*ZapLogger)(nil)

type ZapLogger struct {
	log *zap.Logger
}

func New() (*ZapLogger, error) {
	l, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	return &ZapLogger{log: l}, nil
}

func toZap(fields []logging.Field) []zap.Field {
	res := make([]zap.Field, 0, len(fields))
	for _, f := range fields {
		res = append(res, zap.Any(f.Key, f.Value))
	}
	return res
}

func (l *ZapLogger) LogError(msg string, fields ...logging.Field) { l.log.Error(msg, toZap(fields)...) }
func (l *ZapLogger) LogInfo(msg string, fields ...logging.Field)  { l.log.Info(msg, toZap(fields)...) }
func (l *ZapLogger) Sync() error                                  { return l.log.Sync() }
