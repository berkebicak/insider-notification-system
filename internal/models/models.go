package models

import (
	"time"

	"github.com/google/uuid"
)

type Channel string
type Status string
type Priority string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"

	StatusPending    Status = "pending"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusDelivered  Status = "delivered"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
	StatusScheduled  Status = "scheduled"

	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

type Notification struct {
	ID             uuid.UUID  `json:"id" db:"id"`
	BatchID        *uuid.UUID `json:"batch_id,omitempty" db:"batch_id"`
	Recipient      string     `json:"recipient" db:"recipient"`
	Channel        Channel    `json:"channel" db:"channel"`
	Content        string     `json:"content" db:"content"`
	Priority       Priority   `json:"priority" db:"priority"`
	Status         Status     `json:"status" db:"status"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty" db:"idempotency_key"`
	ProviderMsgID  *string    `json:"provider_message_id,omitempty" db:"provider_message_id"`
	RetryCount     int        `json:"retry_count" db:"retry_count"`
	MaxRetries     int        `json:"max_retries" db:"max_retries"`
	ScheduledAt    *time.Time `json:"scheduled_at,omitempty" db:"scheduled_at"`
	TemplateID     *uuid.UUID `json:"template_id,omitempty" db:"template_id"`
	TemplateVars   *string    `json:"template_vars,omitempty" db:"template_vars"`
	ErrorMessage   *string    `json:"error_message,omitempty" db:"error_message"`
	CreatedAt      time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at" db:"updated_at"`
	SentAt         *time.Time `json:"sent_at,omitempty" db:"sent_at"`
}

type Batch struct {
	ID        uuid.UUID `json:"id" db:"id"`
	Total     int       `json:"total" db:"total"`
	Pending   int       `json:"pending" db:"pending"`
	Delivered int       `json:"delivered" db:"delivered"`
	Failed    int       `json:"failed" db:"failed"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

type Template struct {
	ID        uuid.UUID `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Channel   Channel   `json:"channel" db:"channel"`
	Content   string    `json:"content" db:"content"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// -- Request/Response types --

type CreateNotificationRequest struct {
	Recipient      string            `json:"recipient" validate:"required"`
	Channel        Channel           `json:"channel" validate:"required,oneof=sms email push"`
	Content        string            `json:"content" validate:"required"`
	Priority       Priority          `json:"priority" validate:"omitempty,oneof=high normal low"`
	IdempotencyKey *string           `json:"idempotency_key,omitempty"`
	ScheduledAt    *time.Time        `json:"scheduled_at,omitempty"`
	TemplateID     *uuid.UUID        `json:"template_id,omitempty"`
	TemplateVars   map[string]string `json:"template_vars,omitempty"`
}

type CreateBatchRequest struct {
	Notifications []CreateNotificationRequest `json:"notifications" validate:"required,min=1,max=1000"`
}

type ListNotificationsQuery struct {
	Status    *Status   `query:"status"`
	Channel   *Channel  `query:"channel"`
	BatchID   *string   `query:"batch_id"`
	DateFrom  *string   `query:"date_from"`
	DateTo    *string   `query:"date_to"`
	Page      int       `query:"page"`
	PageSize  int       `query:"page_size"`
}

type PaginatedResponse struct {
	Data     interface{} `json:"data"`
	Total    int64       `json:"total"`
	Page     int         `json:"page"`
	PageSize int         `json:"page_size"`
	Pages    int64       `json:"pages"`
}

type ProviderResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}
