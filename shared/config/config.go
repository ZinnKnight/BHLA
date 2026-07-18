package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DatabaseURL       string
	RedisAddr         string
	RedisPassword     string
	RedisDB           int
	RedisPoolSize     int
	RedisMinIdleConns int
	GRPCPort          int
	MetricsPort       int
	RateLimitPerMin   int

	SessionTTLSeconds int

	DBMaxConn      int
	DBMinConn      int
	DBMaxConnTTL   int
	DBMaxConnIdTTL int

	ServiceName           string
	ServiceVersion        string
	Environment           string
	OpenTelemetryEndpoint string
	TracingEnabled        bool
	TracingSampleRatio    float64

	KafkaBrokers []string
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		DatabaseURL:           getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/BHLA"),
		RedisAddr:             getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:         getEnv("REDIS_PASSWORD", ""),
		RedisDB:               getEnvInt("REDIS_DB", 0),
		RedisPoolSize:         getEnvInt("REDIS_POOL_SIZE", 100),
		RedisMinIdleConns:     getEnvInt("REDIS_MIN_IDLE_CONNS", 0),
		GRPCPort:              getEnvInt("GRPC_PORT", 50051),
		MetricsPort:           getEnvInt("METRICS_PORT", 2112),
		RateLimitPerMin:       getEnvInt("RATE_LIMIT_PER_MIN", 100),
		SessionTTLSeconds:     getEnvInt("SESSION_TTL_SECONDS", 3600),
		DBMaxConn:             getEnvInt("DB_MAX_CONN", 50),
		DBMinConn:             getEnvInt("DB_MIN_CONN", 10),
		DBMaxConnTTL:          getEnvInt("DB_MAX_CONN_TTL", 30),
		DBMaxConnIdTTL:        getEnvInt("DB_MAX_CONN_IDLE_TTL", 5),
		ServiceName:           getEnv("SERVICE_NAME", "unknown-service"),
		ServiceVersion:        getEnv("SERVICE_VERSION", "dev"),
		Environment:           getEnv("ENVIRONMENT", "local"),
		OpenTelemetryEndpoint: getEnv("OPEN_TELEMETRY_ENDPOINT", "jaeger:4317"),
		TracingEnabled:        getEnvBool("TRACING_ENABLED", true),
		TracingSampleRatio:    getEnvFloat("TRACING_SAMPLE_RATIO", 1.0),
		KafkaBrokers:          getEnvSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func getEnvSlice(key string, fallback []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}
