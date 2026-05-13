package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/bicak/notification-system/docs"
	"github.com/bicak/notification-system/internal/api/handlers"
	"github.com/bicak/notification-system/internal/api/middleware"
	"github.com/bicak/notification-system/internal/config"
	"github.com/bicak/notification-system/internal/db"
	"github.com/bicak/notification-system/internal/delivery"
	"github.com/bicak/notification-system/internal/queue"
	"github.com/bicak/notification-system/internal/templates"
	"github.com/bicak/notification-system/internal/tracing"
	"github.com/bicak/notification-system/internal/worker"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/swagger"
)

// @title Notification System API
// @version 1.0
// @description Event-driven multi-channel notification system
// @host localhost:8080
// @BasePath /
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	shutdownTracing, err := tracing.Init(context.Background(), cfg.Tracing)
	if err != nil {
		log.Fatalf("tracing error: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(ctx); err != nil {
			log.Printf("tracing shutdown error: %v", err)
		}
	}()

	pool, err := db.NewPool(&cfg.DB)
	if err != nil {
		log.Fatalf("db connection error: %v", err)
	}
	defer pool.Close()

	if err := db.RunMigrations(pool); err != nil {
		log.Fatalf("migration error: %v", err)
	}
	log.Println("migrations applied")

	rdb, err := queue.NewRedisClient(&cfg.Redis)
	if err != nil {
		log.Fatalf("redis connection error: %v", err)
	}
	defer rdb.Close()

	// Servisler
	queueMgr := queue.NewManager(rdb)
	provider := delivery.NewProvider(cfg.Provider.WebhookURL)
	templateSvc := templates.NewService(pool)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerPool := worker.NewPool(pool, queueMgr, provider, cfg.Worker.Concurrency, cfg.Worker.RateLimit)
	workerPool.Start(ctx)
	workerPool.StartScheduler(ctx)

	// Fiber app
	app := fiber.New(fiber.Config{
		AppName:      "notification-system",
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			return c.Status(code).JSON(fiber.Map{"error": err.Error()})
		},
	})

	// Global middleware
	app.Use(cors.New())
	app.Use(middleware.CorrelationID())
	app.Use(middleware.Tracing())
	app.Use(middleware.Logger())
	app.Use(middleware.Recover())

	// Handler'lar
	notifHandler := handlers.NewNotificationHandler(pool, queueMgr, templateSvc)
	metricsHandler := handlers.NewMetricsHandler(queueMgr, pool, rdb)
	templateHandler := handlers.NewTemplateHandler(templateSvc)

	// Swagger
	app.Get("/swagger/*", swagger.HandlerDefault)

	// Health
	app.Get("/health", metricsHandler.Health)

	// API v1
	v1 := app.Group("/api/v1")

	// Notifications
	notifs := v1.Group("/notifications")
	notifs.Post("/", notifHandler.Create)
	notifs.Post("/batch", notifHandler.CreateBatch)
	notifs.Get("/", notifHandler.List)
	notifs.Get("/batch/:id", notifHandler.GetBatch)
	notifs.Get("/:id", notifHandler.Get)
	notifs.Patch("/:id/cancel", notifHandler.Cancel)

	// Templates
	tmpl := v1.Group("/templates")
	tmpl.Post("/", templateHandler.Create)
	tmpl.Get("/", templateHandler.List)
	tmpl.Get("/:id", templateHandler.Get)

	// Metrics
	v1.Get("/metrics", metricsHandler.Metrics)

	// WebSocket
	app.Use("/ws", handlers.WSUpgrade)
	app.Get("/ws/notifications", websocket.New(handlers.WSHandler))

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("shutting down...")
		cancel()
		if err := app.ShutdownWithTimeout(10 * time.Second); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}()

	log.Printf("server starting on :%s (env: %s)", cfg.App.Port, cfg.App.Env)
	if err := app.Listen(":" + cfg.App.Port); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
