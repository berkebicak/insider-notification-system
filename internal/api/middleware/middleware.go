package middleware

import (
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const CorrelationIDHeader = "X-Correlation-ID"

// CorrelationID attaches a stable request ID to logs and responses.
func CorrelationID() fiber.Handler {
	return func(c *fiber.Ctx) error {
		id := c.Get(CorrelationIDHeader)
		if id == "" {
			id = uuid.New().String()
		}
		c.Set(CorrelationIDHeader, id)
		c.Locals("correlation_id", id)
		return c.Next()
	}
}

// Logger emits structured request logs.
func Logger() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		correlationID := c.Locals("correlation_id")

		err := c.Next()

		slog.Info(
			"http_request",
			"correlation_id", correlationID,
			"method", c.Method(),
			"path", c.Path(),
			"status", c.Response().StatusCode(),
			"latency_ms", time.Since(start).Milliseconds(),
		)

		return err
	}
}

func Tracing() fiber.Handler {
	tracer := otel.Tracer("notification-system/http")

	return func(c *fiber.Ctx) error {
		ctx, span := tracer.Start(c.UserContext(), c.Method()+" "+c.Path())
		c.SetUserContext(ctx)
		defer span.End()

		correlationID, _ := c.Locals("correlation_id").(string)
		span.SetAttributes(
			attribute.String("http.method", c.Method()),
			attribute.String("http.path", c.Path()),
			attribute.String("correlation_id", correlationID),
		)

		err := c.Next()
		status := c.Response().StatusCode()
		span.SetAttributes(attribute.Int("http.status_code", status))
		if err != nil || status >= 500 {
			span.SetStatus(codes.Error, "request failed")
		}

		return err
	}
}

// Recover converts panics into a structured 500 response.
func Recover() fiber.Handler {
	return func(c *fiber.Ctx) error {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic recovered", "error", r, "correlation_id", c.Locals("correlation_id"))
				_ = c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "internal server error",
				})
			}
		}()
		return c.Next()
	}
}
