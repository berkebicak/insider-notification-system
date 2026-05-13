package queue

import (
	"context"
	"fmt"

	"github.com/bicak/notification-system/internal/config"
	"github.com/redis/go-redis/v9"
)

func NewRedisClient(cfg *config.RedisConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return rdb, nil
}

// QueueKey builds the Redis list key for a channel/priority pair.
func QueueKey(channel, priority string) string {
	return fmt.Sprintf("notif:queue:%s:%s", channel, priority)
}

// ScheduledKey stores delayed notifications and retries.
const ScheduledKey = "notif:scheduled"

// IdempotencyKey stores a notification ID for a client-provided key.
func IdempotencyKey(key string) string {
	return fmt.Sprintf("notif:idem:%s", key)
}
