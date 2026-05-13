package handlers

import (
	"strings"
	"testing"
	"time"

	"github.com/bicak/notification-system/internal/models"
)

func TestValidateCreateRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     models.CreateNotificationRequest
		wantErr string
	}{
		{
			name: "missing recipient",
			req: models.CreateNotificationRequest{
				Channel: models.ChannelSMS,
				Content: "hello",
			},
			wantErr: "recipient is required",
		},
		{
			name: "invalid channel",
			req: models.CreateNotificationRequest{
				Recipient: "+905551234567",
				Channel:   "fax",
				Content:   "hello",
			},
			wantErr: "channel must be one of",
		},
		{
			name: "sms recipient must be international",
			req: models.CreateNotificationRequest{
				Recipient: "05551234567",
				Channel:   models.ChannelSMS,
				Content:   "hello",
			},
			wantErr: "international format",
		},
		{
			name: "invalid email recipient",
			req: models.CreateNotificationRequest{
				Recipient: "not-an-email",
				Channel:   models.ChannelEmail,
				Content:   "hello",
			},
			wantErr: "valid email",
		},
		{
			name: "push content too long",
			req: models.CreateNotificationRequest{
				Recipient: "device-token",
				Channel:   models.ChannelPush,
				Content:   strings.Repeat("x", 513),
			},
			wantErr: "push content exceeds",
		},
		{
			name: "scheduled in the past",
			req: models.CreateNotificationRequest{
				Recipient:   "+905551234567",
				Channel:     models.ChannelSMS,
				Content:     "hello",
				ScheduledAt: ptrTime(time.Now().Add(-2 * time.Minute)),
			},
			wantErr: "scheduled_at cannot be in the past",
		},
		{
			name: "valid sms",
			req: models.CreateNotificationRequest{
				Recipient: "+905551234567",
				Channel:   models.ChannelSMS,
				Content:   "hello",
				Priority:  models.PriorityHigh,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCreateRequest(&tt.req)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}
