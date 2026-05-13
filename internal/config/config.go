package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	App      AppConfig
	DB       DBConfig
	Redis    RedisConfig
	Provider ProviderConfig
	Worker   WorkerConfig
	Tracing  TracingConfig
}

type AppConfig struct {
	Port string
	Env  string
}

type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
}

type RedisConfig struct {
	Host     string
	Port     string
	Password string
	DB       int
}

type ProviderConfig struct {
	WebhookURL string
}

type WorkerConfig struct {
	// max goroutines per channel
	Concurrency int
	// max msgs/sec per channel (rate limiter)
	RateLimit int
}

type TracingConfig struct {
	Enabled      bool
	ServiceName  string
	OTLPEndpoint string
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))
	concurrency, _ := strconv.Atoi(getEnv("WORKER_CONCURRENCY", "10"))
	rateLimit, _ := strconv.Atoi(getEnv("WORKER_RATE_LIMIT", "100"))
	tracingEnabled, _ := strconv.ParseBool(getEnv("TRACING_ENABLED", "false"))

	cfg := &Config{
		App: AppConfig{
			Port: getEnv("APP_PORT", "8080"),
			Env:  getEnv("APP_ENV", "development"),
		},
		DB: DBConfig{
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     getEnv("DB_PORT", "5432"),
			User:     getEnv("DB_USER", "postgres"),
			Password: getEnv("DB_PASSWORD", "postgres"),
			Name:     getEnv("DB_NAME", "notifications"),
			SSLMode:  getEnv("DB_SSLMODE", "disable"),
		},
		Redis: RedisConfig{
			Host:     getEnv("REDIS_HOST", "localhost"),
			Port:     getEnv("REDIS_PORT", "6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       redisDB,
		},
		Provider: ProviderConfig{
			WebhookURL: getEnv("PROVIDER_WEBHOOK_URL", ""),
		},
		Worker: WorkerConfig{
			Concurrency: concurrency,
			RateLimit:   rateLimit,
		},
		Tracing: TracingConfig{
			Enabled:      tracingEnabled,
			ServiceName:  getEnv("TRACING_SERVICE_NAME", "notification-system"),
			OTLPEndpoint: getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		},
	}

	if cfg.Provider.WebhookURL == "" {
		return nil, fmt.Errorf("PROVIDER_WEBHOOK_URL is required")
	}
	if _, err := url.ParseRequestURI(cfg.Provider.WebhookURL); err != nil {
		return nil, fmt.Errorf("PROVIDER_WEBHOOK_URL must be a valid URL")
	}
	if cfg.Worker.Concurrency < 1 {
		return nil, fmt.Errorf("WORKER_CONCURRENCY must be greater than 0")
	}
	if cfg.Worker.RateLimit < 1 || cfg.Worker.RateLimit > 100 {
		return nil, fmt.Errorf("WORKER_RATE_LIMIT must be between 1 and 100")
	}

	return cfg, nil
}

func (d *DBConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode,
	)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
