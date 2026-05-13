package handlers

import (
	"context"
	"fmt"
	"log"
	"net/mail"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bicak/notification-system/internal/models"
	"github.com/bicak/notification-system/internal/queue"
	"github.com/bicak/notification-system/internal/realtime"
	"github.com/bicak/notification-system/internal/templates"
	"github.com/bicak/notification-system/internal/worker"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type NotificationHandler struct {
	db          *pgxpool.Pool
	queueMgr    *queue.Manager
	templateSvc *templates.Service
}

func NewNotificationHandler(db *pgxpool.Pool, qm *queue.Manager, ts *templates.Service) *NotificationHandler {
	return &NotificationHandler{db: db, queueMgr: qm, templateSvc: ts}
}

// Create godoc
// @Summary Create a notification
// @Tags notifications
// @Accept json
// @Produce json
// @Param body body models.CreateNotificationRequest true "Notification"
// @Success 201 {object} models.Notification
// @Router /api/v1/notifications [post]
func (h *NotificationHandler) Create(c *fiber.Ctx) error {
	var req models.CreateNotificationRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	if err := h.prepareCreateRequest(c.Context(), &req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	n, err := worker.EnqueueNotification(c.Context(), h.db, h.queueMgr, &req, nil)
	if err != nil {
		log.Printf("[handler] create notification error: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to create notification"})
	}

	return c.Status(fiber.StatusCreated).JSON(n)
}

// CreateBatch godoc
// @Summary Create batch notifications (up to 1000)
// @Tags notifications
// @Accept json
// @Produce json
// @Param body body models.CreateBatchRequest true "Batch"
// @Success 201 {object} models.Batch
// @Router /api/v1/notifications/batch [post]
func (h *NotificationHandler) CreateBatch(c *fiber.Ctx) error {
	var req models.CreateBatchRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	if len(req.Notifications) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "notifications array is required"})
	}
	if len(req.Notifications) > 1000 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "max 1000 notifications per batch"})
	}

	batchID := uuid.New()
	total := len(req.Notifications)

	_, err := h.db.Exec(c.Context(),
		`INSERT INTO batches (id, total, pending) VALUES ($1, $2, $3)`,
		batchID, total, total,
	)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to create batch"})
	}

	var failedCount int
	var createdCount int
	for _, notifReq := range req.Notifications {
		nr := notifReq
		if err := h.prepareCreateRequest(c.Context(), &nr); err != nil {
			failedCount++
			continue
		}
		if _, err := worker.EnqueueNotification(c.Context(), h.db, h.queueMgr, &nr, &batchID); err != nil {
			log.Printf("[handler] batch enqueue error: %v", err)
			failedCount++
			continue
		}
		createdCount++
	}

	if _, err := h.db.Exec(c.Context(),
		`UPDATE batches SET pending = $1, failed = $2 WHERE id = $3`,
		createdCount, failedCount, batchID,
	); err != nil {
		log.Printf("[handler] batch counter update error: %v", err)
	}

	batch := models.Batch{
		ID:        batchID,
		Total:     total,
		Pending:   createdCount,
		Failed:    failedCount,
		CreatedAt: time.Now(),
	}

	return c.Status(fiber.StatusCreated).JSON(batch)
}

// Get godoc
// @Summary Get notification by ID
// @Tags notifications
// @Produce json
// @Param id path string true "Notification ID"
// @Success 200 {object} models.Notification
// @Router /api/v1/notifications/{id} [get]
func (h *NotificationHandler) Get(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid notification id"})
	}

	n, err := h.fetchByID(c.Context(), id)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "notification not found"})
	}

	return c.JSON(n)
}

// GetBatch godoc
// @Summary Get batch status by ID
// @Tags notifications
// @Produce json
// @Param id path string true "Batch ID"
// @Success 200 {object} models.Batch
// @Router /api/v1/notifications/batch/{id} [get]
func (h *NotificationHandler) GetBatch(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid batch id"})
	}

	var batch models.Batch
	err = h.db.QueryRow(c.Context(),
		`SELECT id, total, pending, delivered, failed, created_at FROM batches WHERE id = $1`, id,
	).Scan(&batch.ID, &batch.Total, &batch.Pending, &batch.Delivered, &batch.Failed, &batch.CreatedAt)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "batch not found"})
	}

	return c.JSON(batch)
}

// List godoc
// @Summary List notifications with filtering and pagination
// @Tags notifications
// @Produce json
// @Param status query string false "Filter by status"
// @Param channel query string false "Filter by channel"
// @Param batch_id query string false "Filter by batch ID"
// @Param date_from query string false "Created after (RFC3339)"
// @Param date_to query string false "Created before (RFC3339)"
// @Param page query int false "Page number"
// @Param page_size query int false "Items per page (max 100)"
// @Success 200 {object} models.PaginatedResponse
// @Router /api/v1/notifications [get]
func (h *NotificationHandler) List(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	conditions := []string{"1=1"}
	args := []interface{}{}
	argIdx := 1

	if v := c.Query("status"); v != "" {
		conditions = append(conditions, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, v)
		argIdx++
	}
	if v := c.Query("channel"); v != "" {
		conditions = append(conditions, fmt.Sprintf("channel = $%d", argIdx))
		args = append(args, v)
		argIdx++
	}
	if v := c.Query("batch_id"); v != "" {
		conditions = append(conditions, fmt.Sprintf("batch_id = $%d", argIdx))
		args = append(args, v)
		argIdx++
	}
	if v := c.Query("date_from"); v != "" {
		conditions = append(conditions, fmt.Sprintf("created_at >= $%d", argIdx))
		args = append(args, v)
		argIdx++
	}
	if v := c.Query("date_to"); v != "" {
		conditions = append(conditions, fmt.Sprintf("created_at <= $%d", argIdx))
		args = append(args, v)
		argIdx++
	}

	where := strings.Join(conditions, " AND ")

	var total int64
	if err := h.db.QueryRow(c.Context(),
		fmt.Sprintf("SELECT COUNT(*) FROM notifications WHERE %s", where), args...,
	).Scan(&total); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "count query failed"})
	}

	offset := (page - 1) * pageSize
	dataArgs := append(args, pageSize, offset)
	rows, err := h.db.Query(c.Context(), fmt.Sprintf(`
		SELECT id, batch_id, recipient, channel, content, priority, status,
		       idempotency_key, provider_msg_id, retry_count, max_retries,
		       scheduled_at, template_id, template_vars::text, error_message,
		       created_at, updated_at, sent_at
		FROM notifications
		WHERE %s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, where, argIdx, argIdx+1), dataArgs...)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "list query failed"})
	}
	defer rows.Close()

	var list []*models.Notification
	for rows.Next() {
		n := &models.Notification{}
		if err := rows.Scan(
			&n.ID, &n.BatchID, &n.Recipient, &n.Channel, &n.Content,
			&n.Priority, &n.Status, &n.IdempotencyKey, &n.ProviderMsgID,
			&n.RetryCount, &n.MaxRetries, &n.ScheduledAt, &n.TemplateID,
			&n.TemplateVars, &n.ErrorMessage, &n.CreatedAt, &n.UpdatedAt,
			&n.SentAt,
		); err != nil {
			continue
		}
		list = append(list, n)
	}

	if list == nil {
		list = []*models.Notification{}
	}

	pages := (total + int64(pageSize) - 1) / int64(pageSize)
	return c.JSON(models.PaginatedResponse{
		Data:     list,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
		Pages:    pages,
	})
}

// Cancel godoc
// @Summary Cancel a pending/queued notification
// @Tags notifications
// @Produce json
// @Param id path string true "Notification ID"
// @Success 200 {object} map[string]string
// @Router /api/v1/notifications/{id}/cancel [patch]
func (h *NotificationHandler) Cancel(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid id"})
	}

	tag, err := h.db.Exec(c.Context(),
		`UPDATE notifications SET status = 'cancelled', updated_at = NOW()
		 WHERE id = $1 AND status IN ('pending', 'queued', 'scheduled')`,
		id,
	)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "update failed"})
	}
	if tag.RowsAffected() == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "notification cannot be cancelled (already processing, delivered, or not found)",
		})
	}

	realtime.BroadcastStatus(id.String(), string(models.StatusCancelled))
	h.refreshBatchCounters(c.Context(), id)

	return c.JSON(fiber.Map{"message": "notification cancelled", "id": id})
}

func (h *NotificationHandler) fetchByID(ctx context.Context, id uuid.UUID) (*models.Notification, error) {
	n := &models.Notification{}
	err := h.db.QueryRow(ctx, `
		SELECT id, batch_id, recipient, channel, content, priority, status,
		       idempotency_key, provider_msg_id, retry_count, max_retries,
		       scheduled_at, template_id, template_vars::text, error_message,
		       created_at, updated_at, sent_at
		FROM notifications WHERE id = $1
	`, id).Scan(
		&n.ID, &n.BatchID, &n.Recipient, &n.Channel, &n.Content,
		&n.Priority, &n.Status, &n.IdempotencyKey, &n.ProviderMsgID,
		&n.RetryCount, &n.MaxRetries, &n.ScheduledAt, &n.TemplateID,
		&n.TemplateVars, &n.ErrorMessage, &n.CreatedAt, &n.UpdatedAt,
		&n.SentAt,
	)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (h *NotificationHandler) prepareCreateRequest(ctx context.Context, req *models.CreateNotificationRequest) error {
	if err := validateCreateRequest(req); err != nil {
		return err
	}

	if req.TemplateID == nil {
		return nil
	}

	tmpl, err := h.templateSvc.Get(ctx, *req.TemplateID)
	if err != nil {
		return fmt.Errorf("template not found")
	}
	if tmpl.Channel != req.Channel {
		return fmt.Errorf("template channel %q does not match notification channel %q", tmpl.Channel, req.Channel)
	}

	rendered, err := h.templateSvc.Render(ctx, *req.TemplateID, req.TemplateVars)
	if err != nil {
		return fmt.Errorf("template render: %v", err)
	}
	req.Content = rendered

	return validateCreateRequest(req)
}

func (h *NotificationHandler) refreshBatchCounters(ctx context.Context, notifID uuid.UUID) {
	var batchID *uuid.UUID
	err := h.db.QueryRow(ctx,
		`SELECT batch_id FROM notifications WHERE id = $1`, notifID,
	).Scan(&batchID)
	if err != nil || batchID == nil {
		return
	}

	_, _ = h.db.Exec(ctx, `
		UPDATE batches SET
			delivered = (SELECT COUNT(*) FROM notifications WHERE batch_id = $1 AND status = 'delivered'),
			failed    = GREATEST(0, total - (SELECT COUNT(*) FROM notifications WHERE batch_id = $1))
			            + (SELECT COUNT(*) FROM notifications WHERE batch_id = $1 AND status = 'failed'),
			pending   = (SELECT COUNT(*) FROM notifications WHERE batch_id = $1 AND status NOT IN ('delivered','failed','cancelled'))
		WHERE id = $1
	`, batchID)
}

func validateCreateRequest(req *models.CreateNotificationRequest) error {
	if req.Recipient == "" {
		return fmt.Errorf("recipient is required")
	}
	if req.Channel == "" {
		return fmt.Errorf("channel is required")
	}
	if req.Channel != models.ChannelSMS && req.Channel != models.ChannelEmail && req.Channel != models.ChannelPush {
		return fmt.Errorf("channel must be one of: sms, email, push")
	}
	if req.Content == "" && req.TemplateID == nil {
		return fmt.Errorf("content or template_id is required")
	}
	if req.ScheduledAt != nil && req.ScheduledAt.Before(time.Now().Add(-1*time.Minute)) {
		return fmt.Errorf("scheduled_at cannot be in the past")
	}
	if req.Priority != "" &&
		req.Priority != models.PriorityHigh &&
		req.Priority != models.PriorityNormal &&
		req.Priority != models.PriorityLow {
		return fmt.Errorf("priority must be one of: high, normal, low")
	}
	switch req.Channel {
	case models.ChannelSMS:
		if !strings.HasPrefix(req.Recipient, "+") {
			return fmt.Errorf("sms recipient must be in international format")
		}
		if contentLength(req.Content) > 1600 {
			return fmt.Errorf("sms content exceeds 1600 characters")
		}
	case models.ChannelEmail:
		if _, err := mail.ParseAddress(req.Recipient); err != nil {
			return fmt.Errorf("email recipient must be a valid email address")
		}
		if contentLength(req.Content) > 10000 {
			return fmt.Errorf("email content exceeds 10000 characters")
		}
	case models.ChannelPush:
		if contentLength(req.Content) > 512 {
			return fmt.Errorf("push content exceeds 512 characters")
		}
	}
	return nil
}

func contentLength(value string) int {
	return utf8.RuneCountInString(value)
}
