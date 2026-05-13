package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bicak/notification-system/internal/models"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type Provider struct {
	webhookURL string
	client     *http.Client
}

func NewProvider(webhookURL string) *Provider {
	return &Provider{
		webhookURL: webhookURL,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

type sendPayload struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// Send posts the provider payload and returns the provider message ID.
func (p *Provider) Send(ctx context.Context, n *models.Notification) (*models.ProviderResponse, error) {
	ctx, span := otel.Tracer("notification-system/provider").Start(ctx, "provider.send")
	defer span.End()
	span.SetAttributes(
		attribute.String("notification.id", n.ID.String()),
		attribute.String("notification.channel", string(n.Channel)),
	)

	payload := sendPayload{
		To:      n.Recipient,
		Channel: string(n.Channel),
		Content: n.Content,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.webhookURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Notification-ID", n.ID.String())

	resp, err := p.client.Do(req)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		span.SetStatus(codes.Error, fmt.Sprintf("provider returned status %d", resp.StatusCode))
		return nil, fmt.Errorf("provider returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var providerResp models.ProviderResponse
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &providerResp); err != nil {
			providerResp.MessageID = fmt.Sprintf("unknown-%d", time.Now().UnixNano())
			providerResp.Status = "accepted"
		}
	} else {
		providerResp.MessageID = fmt.Sprintf("wh-%d", time.Now().UnixNano())
		providerResp.Status = "accepted"
		providerResp.Timestamp = time.Now().Format(time.RFC3339)
	}

	span.SetAttributes(attribute.String("provider.message_id", providerResp.MessageID))
	return &providerResp, nil
}
