package delivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bicak/notification-system/internal/models"
	"github.com/google/uuid"
)

func TestProviderSendAccepted(t *testing.T) {
	var got sendPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("X-Notification-ID") == "" {
			t.Fatal("missing X-Notification-ID header")
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"provider-1","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`))
	}))
	defer server.Close()

	provider := NewProvider(server.URL)
	resp, err := provider.Send(context.Background(), &models.Notification{
		ID:        uuid.New(),
		Recipient: "+905551234567",
		Channel:   models.ChannelSMS,
		Content:   "hello",
	})
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	if resp.MessageID != "provider-1" {
		t.Fatalf("unexpected message id: %s", resp.MessageID)
	}
	if got.To != "+905551234567" || got.Channel != "sms" || got.Content != "hello" {
		t.Fatalf("unexpected payload: %+v", got)
	}
}

func TestProviderSendNonAcceptedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "provider down", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	provider := NewProvider(server.URL)
	_, err := provider.Send(context.Background(), &models.Notification{
		ID:        uuid.New(),
		Recipient: "+905551234567",
		Channel:   models.ChannelSMS,
		Content:   "hello",
	})
	if err == nil {
		t.Fatal("expected provider error")
	}
}
