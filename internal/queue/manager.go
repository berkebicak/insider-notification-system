package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bicak/notification-system/internal/models"
	"github.com/redis/go-redis/v9"
)

type Manager struct {
	rdb *redis.Client
}

func NewManager(rdb *redis.Client) *Manager {
	return &Manager{rdb: rdb}
}

// Enqueue stores a notification in the channel/priority queue.
func (m *Manager) Enqueue(ctx context.Context, n *models.Notification) error {
	data, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}

	key := QueueKey(string(n.Channel), string(n.Priority))

	// LPUSH + RPOP gives FIFO behavior per queue.
	if err := m.rdb.LPush(ctx, key, data).Err(); err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}

	return nil
}

// EnqueueScheduled stores a notification in a sorted set. The score is the
// Unix timestamp at which the scheduler should move it back to the live queue.
func (m *Manager) EnqueueScheduled(ctx context.Context, n *models.Notification) error {
	data, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}

	score := float64(n.ScheduledAt.Unix())
	if err := m.rdb.ZAdd(ctx, ScheduledKey, redis.Z{Score: score, Member: data}).Err(); err != nil {
		return fmt.Errorf("enqueue scheduled: %w", err)
	}

	return nil
}

// Dequeue reads a notification from a single channel/priority queue.
func (m *Manager) Dequeue(ctx context.Context, channel, priority string, timeout time.Duration) (*models.Notification, error) {
	key := QueueKey(channel, priority)

	result, err := m.rdb.BRPop(ctx, timeout, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // timeout doldu, mesaj yok
		}
		return nil, fmt.Errorf("dequeue: %w", err)
	}

	return decodeNotification(result[1])
}

// DequeueNext reads the next notification for a channel, preferring high
// priority over normal and low whenever work is immediately available.
func (m *Manager) DequeueNext(ctx context.Context, channel string, timeout time.Duration) (*models.Notification, error) {
	priorities := []string{"high", "normal", "low"}
	keys := make([]string, 0, len(priorities))

	for _, priority := range priorities {
		key := QueueKey(channel, priority)
		keys = append(keys, key)

		value, err := m.rdb.RPop(ctx, key).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("dequeue %s: %w", key, err)
		}
		return decodeNotification(value)
	}

	result, err := m.rdb.BRPop(ctx, timeout, keys...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("dequeue: %w", err)
	}

	return decodeNotification(result[1])
}

// QueueDepth returns current Redis queue sizes grouped by channel/priority.
func (m *Manager) QueueDepth(ctx context.Context) (map[string]int64, error) {
	channels := []string{"sms", "email", "push"}
	priorities := []string{"high", "normal", "low"}

	depths := make(map[string]int64)

	for _, ch := range channels {
		for _, pr := range priorities {
			key := QueueKey(ch, pr)
			count, err := m.rdb.LLen(ctx, key).Result()
			if err != nil {
				continue
			}
			if count > 0 {
				depths[fmt.Sprintf("%s:%s", ch, pr)] = count
			}
		}
	}

	// scheduled set
	scheduled, _ := m.rdb.ZCard(ctx, ScheduledKey).Result()
	if scheduled > 0 {
		depths["scheduled"] = scheduled
	}

	return depths, nil
}

// PopDueScheduled returns notifications whose scheduled time has passed.
func (m *Manager) PopDueScheduled(ctx context.Context) ([]*models.Notification, error) {
	now := float64(time.Now().Unix())

	results, err := m.rdb.ZRangeByScore(ctx, ScheduledKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", now),
	}).Result()
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, nil
	}

	var notifications []*models.Notification
	for _, r := range results {
		removed, err := m.rdb.ZRem(ctx, ScheduledKey, r).Result()
		if err != nil || removed == 0 {
			continue
		}

		var n models.Notification
		if err := json.Unmarshal([]byte(r), &n); err != nil {
			continue
		}
		notifications = append(notifications, &n)
	}

	return notifications, nil
}

// CheckIdempotency returns the existing notification ID for a key. When notifID
// is not empty, it stores that ID only if the key has not been seen before.
func (m *Manager) CheckIdempotency(ctx context.Context, key, notifID string) (string, error) {
	redisKey := IdempotencyKey(key)

	existing, err := m.rdb.Get(ctx, redisKey).Result()
	if err == redis.Nil {
		if notifID == "" {
			return "", nil
		}

		ok, setErr := m.rdb.SetNX(ctx, redisKey, notifID, 24*time.Hour).Result()
		if setErr != nil {
			return "", fmt.Errorf("set idempotency key: %w", setErr)
		}
		if !ok {
			value, getErr := m.rdb.Get(ctx, redisKey).Result()
			if getErr != nil {
				return "", fmt.Errorf("get idempotency key: %w", getErr)
			}
			return value, nil
		}
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("check idempotency: %w", err)
	}

	return existing, nil
}

func decodeNotification(value string) (*models.Notification, error) {
	var n models.Notification
	if err := json.Unmarshal([]byte(value), &n); err != nil {
		return nil, fmt.Errorf("unmarshal notification: %w", err)
	}
	return &n, nil
}
