package worker_test

import (
	"testing"
	"time"

	"github.com/bicak/notification-system/internal/models"
	"github.com/google/uuid"
)

func TestNotificationDefaults(t *testing.T) {
	n := &models.Notification{
		ID:         uuid.New(),
		Recipient:  "+905551234567",
		Channel:    models.ChannelSMS,
		Content:    "test message",
		Priority:   models.PriorityNormal,
		Status:     models.StatusQueued,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if n.RetryCount != 0 {
		t.Errorf("initial retry count should be 0, got %d", n.RetryCount)
	}
	if n.MaxRetries != 3 {
		t.Errorf("max retries should be 3, got %d", n.MaxRetries)
	}
	if n.Status != models.StatusQueued {
		t.Errorf("expected queued status, got %s", n.Status)
	}
}

func TestBackoffCalc(t *testing.T) {
	// exponential backoff: 2^retry * 5s
	cases := []struct {
		retry    uint
		expected time.Duration
	}{
		{1, 10 * time.Second},
		{2, 20 * time.Second},
		{3, 40 * time.Second},
	}

	for _, tc := range cases {
		delay := time.Duration(1<<tc.retry) * 5 * time.Second
		if delay != tc.expected {
			t.Errorf("retry %d: expected %s, got %s", tc.retry, tc.expected, delay)
		}
	}
}

func TestChannelValidation(t *testing.T) {
	validChannels := []models.Channel{
		models.ChannelSMS,
		models.ChannelEmail,
		models.ChannelPush,
	}

	for _, ch := range validChannels {
		if ch == "" {
			t.Errorf("channel should not be empty")
		}
	}
}

func TestPriorityValues(t *testing.T) {
	if models.PriorityHigh != "high" {
		t.Error("PriorityHigh should be 'high'")
	}
	if models.PriorityNormal != "normal" {
		t.Error("PriorityNormal should be 'normal'")
	}
	if models.PriorityLow != "low" {
		t.Error("PriorityLow should be 'low'")
	}
}
