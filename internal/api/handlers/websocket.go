package handlers

import (
	"log/slog"

	"github.com/bicak/notification-system/internal/realtime"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
)

// WSUpgrade WebSocket upgrade middleware
func WSUpgrade(c *fiber.Ctx) error {
	if websocket.IsWebSocketUpgrade(c) {
		return c.Next()
	}
	return fiber.ErrUpgradeRequired
}

// WSHandler godoc
// @Summary WebSocket endpoint for real-time notification status
// @Tags system
// @Router /ws/notifications [get]
func WSHandler(c *websocket.Conn) {
	notifID := c.Query("id", "all")
	unregister := realtime.RegisterStatusClient(notifID, c)
	defer unregister()

	slog.Info("websocket client connected", "watch", notifID)
	defer slog.Info("websocket client disconnected", "watch", notifID)

	for {
		_, _, err := c.ReadMessage()
		if err != nil {
			break
		}
	}
}
