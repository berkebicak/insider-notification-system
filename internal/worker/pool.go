package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bicak/notification-system/internal/delivery"
	"github.com/bicak/notification-system/internal/metrics"
	"github.com/bicak/notification-system/internal/models"
	"github.com/bicak/notification-system/internal/queue"
	"github.com/bicak/notification-system/internal/realtime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/time/rate"
)

// Pool owns the queue workers and delivery retry flow.
type Pool struct {
	db          *pgxpool.Pool
	queueMgr    *queue.Manager
	provider    *delivery.Provider
	concurrency int
	rateLimit   int
}

func NewPool(db *pgxpool.Pool, qm *queue.Manager, p *delivery.Provider, concurrency, rateLimit int) *Pool {
	return &Pool{
		db:          db,
		queueMgr:    qm,
		provider:    p,
		concurrency: concurrency,
		rateLimit:   rateLimit,
	}
}

// Start runs one worker pool per channel. Each worker reads priorities in the
// order high, normal, low so urgent messages are drained first.
func (p *Pool) Start(ctx context.Context) {
	channels := []string{"sms", "email", "push"}

	for _, ch := range channels {
		limiter := rate.NewLimiter(rate.Limit(p.rateLimit), p.rateLimit)

		for i := 0; i < p.concurrency; i++ {
			go p.runWorker(ctx, ch, limiter, i)
		}
	}

	log.Printf("[worker] started pools for channels: %v, concurrency: %d, rate: %d/s",
		channels, p.concurrency, p.rateLimit)
}

// StartScheduler moves due scheduled and retry jobs back to the live queues.
func (p *Pool) StartScheduler(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.processScheduled(ctx)
			}
		}
	}()

	log.Println("[scheduler] started")
}

func (p *Pool) processScheduled(ctx context.Context) {
	due, err := p.queueMgr.PopDueScheduled(ctx)
	if err != nil {
		log.Printf("[scheduler] pop due error: %v", err)
		return
	}

	for _, n := range due {
		status, err := p.currentStatus(ctx, n.ID)
		if err != nil {
			log.Printf("[scheduler] status check error for %s: %v", n.ID, err)
			continue
		}
		if status == models.StatusCancelled || status == models.StatusDelivered || status == models.StatusFailed {
			continue
		}

		if status == models.StatusScheduled {
			if err := p.updateStatus(ctx, n.ID, models.StatusQueued, ""); err != nil {
				log.Printf("[scheduler] update status error for %s: %v", n.ID, err)
				continue
			}
		}

		n.Status = models.StatusQueued
		if err := p.queueMgr.Enqueue(ctx, n); err != nil {
			log.Printf("[scheduler] enqueue error for %s: %v", n.ID, err)
		}
	}
}

func (p *Pool) runWorker(ctx context.Context, channel string, limiter *rate.Limiter, workerID int) {
	log.Printf("[worker] %s #%d started", channel, workerID)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[worker] %s #%d stopped", channel, workerID)
			return
		default:
		}

		n, err := p.queueMgr.DequeueNext(ctx, channel, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[worker] %s dequeue error: %v", channel, err)
			time.Sleep(1 * time.Second)
			continue
		}

		if n == nil {
			continue
		}

		// rate limiter'a uy
		if err := limiter.Wait(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}

		p.process(ctx, n)
	}
}

func (p *Pool) process(ctx context.Context, n *models.Notification) {
	ctx, span := otel.Tracer("notification-system/worker").Start(ctx, "notification.process")
	defer span.End()
	span.SetAttributes(
		attribute.String("notification.id", n.ID.String()),
		attribute.String("notification.channel", string(n.Channel)),
		attribute.String("notification.priority", string(n.Priority)),
	)

	start := time.Now()

	status, retryCount, maxRetries, err := p.currentDeliveryState(ctx, n.ID)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.Printf("[worker] load state failed for %s: %v", n.ID, err)
		return
	}
	if status != models.StatusQueued && status != models.StatusPending {
		log.Printf("[worker] skipping %s with status %s", n.ID, status)
		return
	}
	n.RetryCount = retryCount
	n.MaxRetries = maxRetries

	if err := p.markProcessing(ctx, n.ID); err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.Printf("[worker] update to processing failed for %s: %v", n.ID, err)
		return
	}

	provResp, err := p.provider.Send(ctx, n)
	elapsed := time.Since(start)
	metrics.Global.RecordLatency(elapsed)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.Printf("[worker] send failed for %s (attempt %d): %v", n.ID, n.RetryCount+1, err)
		metrics.Global.IncFailed()
		p.handleFailure(ctx, n, err.Error())
		return
	}

	metrics.Global.IncSent()
	now := time.Now()
	if err := p.markDelivered(ctx, n.ID, provResp.MessageID, now); err != nil {
		span.SetStatus(codes.Error, err.Error())
		log.Printf("[worker] mark delivered failed for %s: %v", n.ID, err)
	}

	log.Printf("[worker] delivered %s via %s in %dms", n.ID, n.Channel, elapsed.Milliseconds())
}

// handleFailure schedules retry attempts with exponential backoff.
func (p *Pool) handleFailure(ctx context.Context, n *models.Notification, errMsg string) {
	n.RetryCount++

	if n.RetryCount > n.MaxRetries {
		if err := p.updateStatus(ctx, n.ID, models.StatusFailed, errMsg); err != nil {
			log.Printf("[worker] mark failed error for %s: %v", n.ID, err)
		}
		p.updateBatchCounters(ctx, n.ID)
		log.Printf("[worker] %s permanently failed after %d attempts", n.ID, n.RetryCount)
		return
	}

	metrics.Global.IncRetried()

	delay := time.Duration(1<<uint(n.RetryCount)) * 5 * time.Second
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	nextAttempt := time.Now().Add(delay)

	log.Printf("[worker] %s retry %d/%d in %s", n.ID, n.RetryCount, n.MaxRetries, delay)

	if _, err := p.db.Exec(ctx,
		`UPDATE notifications
		 SET status = $1, retry_count = $2, error_message = $3, updated_at = NOW()
		 WHERE id = $4 AND status = $5`,
		models.StatusQueued, n.RetryCount, errMsg, n.ID, models.StatusProcessing,
	); err != nil {
		log.Printf("[worker] update retry state error: %v", err)
	}
	realtime.BroadcastStatus(n.ID.String(), string(models.StatusQueued))

	n.Status = models.StatusQueued
	n.ScheduledAt = &nextAttempt
	n.ErrorMessage = &errMsg
	if err := p.queueMgr.EnqueueScheduled(ctx, n); err != nil {
		log.Printf("[worker] schedule retry error for %s: %v", n.ID, err)
	}
}

func (p *Pool) updateStatus(ctx context.Context, id uuid.UUID, status models.Status, errMsg string) error {
	var (
		tag interface{ RowsAffected() int64 }
		err error
	)
	if errMsg != "" {
		tag, err = p.db.Exec(ctx,
			`UPDATE notifications SET status = $1, error_message = $2, updated_at = NOW() WHERE id = $3`,
			status, errMsg, id,
		)
	} else {
		tag, err = p.db.Exec(ctx,
			`UPDATE notifications SET status = $1, updated_at = NOW() WHERE id = $2`,
			status, id,
		)
	}
	if err == nil && tag.RowsAffected() > 0 {
		realtime.BroadcastStatus(id.String(), string(status))
	}
	return err
}

func (p *Pool) markProcessing(ctx context.Context, id uuid.UUID) error {
	tag, err := p.db.Exec(ctx,
		`UPDATE notifications
		 SET status = $1, updated_at = NOW()
		 WHERE id = $2 AND status IN ($3, $4)`,
		models.StatusProcessing, id, models.StatusQueued, models.StatusPending,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("notification is no longer queued")
	}
	realtime.BroadcastStatus(id.String(), string(models.StatusProcessing))
	return nil
}

func (p *Pool) markDelivered(ctx context.Context, id uuid.UUID, providerMsgID string, sentAt time.Time) error {
	tag, err := p.db.Exec(ctx,
		`UPDATE notifications
		 SET status = $1, provider_msg_id = $2, sent_at = $3, updated_at = NOW()
		 WHERE id = $4 AND status = $5`,
		models.StatusDelivered, providerMsgID, sentAt, id,
		models.StatusProcessing,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("notification is no longer processing")
	}

	realtime.BroadcastStatus(id.String(), string(models.StatusDelivered))
	p.updateBatchCounters(ctx, id)
	return nil
}

func (p *Pool) currentStatus(ctx context.Context, id uuid.UUID) (models.Status, error) {
	var status models.Status
	err := p.db.QueryRow(ctx, `SELECT status FROM notifications WHERE id = $1`, id).Scan(&status)
	return status, err
}

func (p *Pool) currentDeliveryState(ctx context.Context, id uuid.UUID) (models.Status, int, int, error) {
	var status models.Status
	var retryCount, maxRetries int
	err := p.db.QueryRow(ctx,
		`SELECT status, retry_count, max_retries FROM notifications WHERE id = $1`, id,
	).Scan(&status, &retryCount, &maxRetries)
	return status, retryCount, maxRetries, err
}

func (p *Pool) updateBatchCounters(ctx context.Context, notifID uuid.UUID) {
	var batchID *uuid.UUID
	err := p.db.QueryRow(ctx,
		`SELECT batch_id FROM notifications WHERE id = $1`, notifID,
	).Scan(&batchID)
	if err != nil || batchID == nil {
		return
	}

	_, _ = p.db.Exec(ctx, `
		UPDATE batches SET
			delivered = (SELECT COUNT(*) FROM notifications WHERE batch_id = $1 AND status = 'delivered'),
			failed    = GREATEST(0, total - (SELECT COUNT(*) FROM notifications WHERE batch_id = $1))
			            + (SELECT COUNT(*) FROM notifications WHERE batch_id = $1 AND status = 'failed'),
			pending   = (SELECT COUNT(*) FROM notifications WHERE batch_id = $1 AND status NOT IN ('delivered','failed','cancelled'))
		WHERE id = $1
	`, batchID)
}

// EnqueueNotification persists a notification and publishes it to the right queue.
func EnqueueNotification(ctx context.Context, db *pgxpool.Pool, qm *queue.Manager, req *models.CreateNotificationRequest, batchID *uuid.UUID) (*models.Notification, error) {
	if req.IdempotencyKey != nil {
		existing, err := qm.CheckIdempotency(ctx, *req.IdempotencyKey, "")
		if err == nil && existing != "" {
			id, err := uuid.Parse(existing)
			if err == nil {
				n, dbErr := fetchNotificationByID(ctx, db, id)
				if dbErr == nil {
					return n, nil
				}
			}
		}
	}

	if req.Priority == "" {
		req.Priority = models.PriorityNormal
	}

	n := &models.Notification{
		ID:             uuid.New(),
		BatchID:        batchID,
		Recipient:      req.Recipient,
		Channel:        req.Channel,
		Content:        req.Content,
		Priority:       req.Priority,
		IdempotencyKey: req.IdempotencyKey,
		MaxRetries:     3,
		ScheduledAt:    req.ScheduledAt,
	}

	if req.TemplateID != nil {
		n.TemplateID = req.TemplateID
		varsJSON, _ := json.Marshal(req.TemplateVars)
		s := string(varsJSON)
		n.TemplateVars = &s
	}

	status := models.StatusQueued
	if req.ScheduledAt != nil && req.ScheduledAt.After(time.Now()) {
		status = models.StatusScheduled
	}
	n.Status = status

	_, err := db.Exec(ctx, `
		INSERT INTO notifications
			(id, batch_id, recipient, channel, content, priority, status, idempotency_key, max_retries, scheduled_at, template_id, template_vars)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`,
		n.ID, n.BatchID, n.Recipient, n.Channel, n.Content,
		n.Priority, n.Status, n.IdempotencyKey, n.MaxRetries,
		n.ScheduledAt, n.TemplateID, n.TemplateVars,
	)
	if err != nil {
		if req.IdempotencyKey != nil {
			existing, fetchErr := fetchNotificationByIdempotencyKey(ctx, db, *req.IdempotencyKey)
			if fetchErr == nil {
				_, _ = qm.CheckIdempotency(ctx, *req.IdempotencyKey, existing.ID.String())
				return existing, nil
			}
		}
		return nil, fmt.Errorf("insert notification: %w", err)
	}

	if req.IdempotencyKey != nil {
		_, _ = qm.CheckIdempotency(ctx, *req.IdempotencyKey, n.ID.String())
	}

	if n.Status == models.StatusScheduled {
		if err := qm.EnqueueScheduled(ctx, n); err != nil {
			log.Printf("[enqueue] scheduled enqueue error for %s: %v", n.ID, err)
		}
	} else {
		if err := qm.Enqueue(ctx, n); err != nil {
			log.Printf("[enqueue] enqueue error for %s: %v", n.ID, err)
		}
	}

	return n, nil
}

func fetchNotificationByID(ctx context.Context, db *pgxpool.Pool, id uuid.UUID) (*models.Notification, error) {
	return fetchNotification(ctx, db, `id = $1`, id)
}

func fetchNotificationByIdempotencyKey(ctx context.Context, db *pgxpool.Pool, key string) (*models.Notification, error) {
	return fetchNotification(ctx, db, `idempotency_key = $1`, key)
}

func fetchNotification(ctx context.Context, db *pgxpool.Pool, where string, arg interface{}) (*models.Notification, error) {
	n := &models.Notification{}
	err := db.QueryRow(ctx, fmt.Sprintf(`
		SELECT id, batch_id, recipient, channel, content, priority, status,
		       idempotency_key, provider_msg_id, retry_count, max_retries,
		       scheduled_at, template_id, template_vars::text, error_message,
		       created_at, updated_at, sent_at
		FROM notifications
		WHERE %s
	`, where), arg).Scan(
		&n.ID, &n.BatchID, &n.Recipient, &n.Channel, &n.Content,
		&n.Priority, &n.Status, &n.IdempotencyKey, &n.ProviderMsgID,
		&n.RetryCount, &n.MaxRetries, &n.ScheduledAt, &n.TemplateID,
		&n.TemplateVars, &n.ErrorMessage, &n.CreatedAt, &n.UpdatedAt, &n.SentAt,
	)
	if err != nil {
		return nil, err
	}
	return n, nil
}
