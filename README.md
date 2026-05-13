# Event-Driven Notification System

Notification service for SMS, email, and push delivery. The API accepts single or batch notification requests, persists state in PostgreSQL, publishes work to Redis queues, and processes delivery asynchronously through rate-limited workers.

## Architecture

```
Client
  |
  v
Fiber REST API + WebSocket
  |
  +--> PostgreSQL: notification, batch, template state
  |
  +--> Redis: channel/priority queues, scheduled jobs, idempotency keys
          |
          v
     Worker pools
          |
          v
     webhook.site provider
```

Key points:

- Separate Redis queues per channel and priority: `sms/email/push` x `high/normal/low`
- Per-channel worker rate limit, capped at 100 messages per second
- Priority draining order: high, then normal, then low
- Exponential retry backoff persisted through Redis scheduled jobs
- Idempotency keys stored for 24 hours
- Status updates available through REST, metrics, and WebSocket
- Versioned SQL migrations embedded under `internal/db/migrations`

## Quick Start

Create a webhook.site URL first, then put it in `.env`.

```bash
cp .env.example .env
# edit PROVIDER_WEBHOOK_URL=https://webhook.site/<your-uuid>

docker-compose up --build
```

Health check:

```bash
curl http://localhost:8080/health
```

Swagger UI:

```text
http://localhost:8080/swagger/index.html
```

## API Examples

Create one notification:

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: demo-1" \
  -d '{
    "recipient": "+905551234567",
    "channel": "sms",
    "content": "Your order is on the way.",
    "priority": "high",
    "idempotency_key": "order-123-shipped"
  }'
```

Create a batch:

```bash
curl -X POST http://localhost:8080/api/v1/notifications/batch \
  -H "Content-Type: application/json" \
  -d '{
    "notifications": [
      {"recipient": "+905551234567", "channel": "sms", "content": "Flash sale started", "priority": "high"},
      {"recipient": "user@example.com", "channel": "email", "content": "Your weekly summary is ready"}
    ]
  }'
```

Query state:

```bash
curl http://localhost:8080/api/v1/notifications/{notification_id}
curl http://localhost:8080/api/v1/notifications/batch/{batch_id}
curl "http://localhost:8080/api/v1/notifications?status=failed&channel=sms&page=1&page_size=20"
```

Cancel a queued or scheduled notification:

```bash
curl -X PATCH http://localhost:8080/api/v1/notifications/{notification_id}/cancel
```

Schedule for later:

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "recipient": "+905551234567",
    "channel": "sms",
    "content": "Meeting starts in 15 minutes.",
    "scheduled_at": "2026-12-01T12:00:00Z"
  }'
```

Templates:

```bash
curl -X POST http://localhost:8080/api/v1/templates \
  -H "Content-Type: application/json" \
  -d '{
    "name": "otp",
    "channel": "sms",
    "content": "Hello {{.Name}}, your code is {{.Code}}"
  }'

curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "recipient": "+905551234567",
    "channel": "sms",
    "template_id": "<template-id>",
    "template_vars": {"Name": "Ahmet", "Code": "123456"}
  }'
```

Metrics and WebSocket:

```bash
curl http://localhost:8080/api/v1/metrics
```

```js
const ws = new WebSocket("ws://localhost:8080/ws/notifications");
ws.onmessage = (event) => console.log(JSON.parse(event.data));
```

Use `ws://localhost:8080/ws/notifications?id=<notification-id>` to watch one notification.

## Provider Contract

The worker sends this payload to `PROVIDER_WEBHOOK_URL`:

```json
{
  "to": "+905551234567",
  "channel": "sms",
  "content": "Your message"
}
```

Expected successful response:

```json
{
  "messageId": "uuid-here",
  "status": "accepted",
  "timestamp": "2026-05-13T09:00:00Z"
}
```

HTTP `202 Accepted` and `200 OK` are treated as accepted. Other statuses trigger retry.

## Operations

Run tests:

```bash
go test ./...
go test -race ./...
```

Regenerate Swagger after changing annotations:

```bash
go run github.com/swaggo/swag/cmd/swag@v1.16.3 init -g cmd/server/main.go -o docs
```

Tracing is OpenTelemetry-compatible. Set `TRACING_ENABLED=true` and optionally `OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector:4318` to export spans.

## Environment

| Variable | Default | Notes |
| --- | --- | --- |
| `APP_PORT` | `8080` | HTTP port |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_USER` | `postgres` | PostgreSQL user |
| `DB_PASSWORD` | `postgres` | PostgreSQL password |
| `DB_NAME` | `notifications` | PostgreSQL database |
| `REDIS_HOST` | `localhost` | Redis host |
| `PROVIDER_WEBHOOK_URL` | required | webhook.site endpoint |
| `WORKER_CONCURRENCY` | `10` | Workers per channel |
| `WORKER_RATE_LIMIT` | `100` | Messages per second per channel, max 100 |
| `TRACING_ENABLED` | `false` | Enables OpenTelemetry spans |

## Project Layout

```text
cmd/server                 application entrypoint
docs                       generated Swagger/OpenAPI files
internal/api               HTTP handlers and middleware
internal/config            environment configuration
internal/db                database pool and migrations
internal/delivery          webhook.site provider client
internal/metrics           in-memory runtime counters
internal/models            domain and API models
internal/queue             Redis queue manager
internal/realtime          WebSocket status hub
internal/templates         template rendering
internal/tracing           OpenTelemetry setup
internal/worker            queue workers and retry logic
.github/workflows/ci.yml   CI pipeline
docker-compose.yml         local stack
```
