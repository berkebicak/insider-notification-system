package handlers

import (
	"context"
	"time"

	"github.com/bicak/notification-system/internal/metrics"
	"github.com/bicak/notification-system/internal/queue"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type MetricsHandler struct {
	queueMgr *queue.Manager
	db       *pgxpool.Pool
	rdb      *redis.Client
}

func NewMetricsHandler(qm *queue.Manager, db *pgxpool.Pool, rdb *redis.Client) *MetricsHandler {
	return &MetricsHandler{queueMgr: qm, db: db, rdb: rdb}
}

// Metrics godoc
// @Summary Get real-time system metrics
// @Tags system
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /api/v1/metrics [get]
func (h *MetricsHandler) Metrics(c *fiber.Ctx) error {
	ctx := c.Context()

	snap := metrics.Global.Snapshot()
	queueDepths, _ := h.queueMgr.QueueDepth(ctx)

	var dbStats struct {
		Total     int64
		Pending   int64
		Delivered int64
		Failed    int64
	}
	_ = h.db.QueryRow(ctx, `SELECT
		COUNT(*),
		COUNT(*) FILTER (WHERE status IN ('pending','queued','processing','scheduled')),
		COUNT(*) FILTER (WHERE status = 'delivered'),
		COUNT(*) FILTER (WHERE status = 'failed')
	FROM notifications`).Scan(&dbStats.Total, &dbStats.Pending, &dbStats.Delivered, &dbStats.Failed)

	return c.JSON(fiber.Map{
		"timestamp":    time.Now().UTC(),
		"queue_depths": queueDepths,
		"counters": fiber.Map{
			"total_notifications": dbStats.Total,
			"pending":             dbStats.Pending,
			"delivered":           dbStats.Delivered,
			"failed":              dbStats.Failed,
		},
		"performance": fiber.Map{
			"total_sent":     snap.TotalSent,
			"total_failed":   snap.TotalFailed,
			"total_retried":  snap.TotalRetried,
			"success_rate":   snap.SuccessRate,
			"avg_latency_ms": snap.AvgLatencyMs,
			"p95_latency_ms": snap.P95LatencyMs,
		},
	})
}

// Health godoc
// @Summary Health check endpoint
// @Tags system
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /health [get]
func (h *MetricsHandler) Health(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Second)
	defer cancel()

	dbOK := true
	if err := h.db.Ping(ctx); err != nil {
		dbOK = false
	}

	redisOK := true
	if err := h.rdb.Ping(ctx).Err(); err != nil {
		redisOK = false
	}

	status := "healthy"
	code := fiber.StatusOK
	if !dbOK || !redisOK {
		status = "degraded"
		code = fiber.StatusServiceUnavailable
	}

	return c.Status(code).JSON(fiber.Map{
		"status": status,
		"checks": fiber.Map{
			"database": map[bool]string{true: "ok", false: "error"}[dbOK],
			"redis":    map[bool]string{true: "ok", false: "error"}[redisOK],
		},
		"timestamp": time.Now().UTC(),
	})
}
