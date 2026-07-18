package logging

type Field struct {
	Key   string
	Value any
}

type Logger interface {
	LogError(msg string, fields ...Field)
	LogInfo(msg string, fields ...Field)
	Sync() error
}

func Err(err error) Field { return Field{Key: "error", Value: err} }
