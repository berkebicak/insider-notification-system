package realtime

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/gofiber/contrib/websocket"
)

type Hub struct {
	mu      sync.RWMutex
	clients map[string]map[*websocket.Conn]struct{}
}

var statusHub = &Hub{
	clients: make(map[string]map[*websocket.Conn]struct{}),
}

func RegisterStatusClient(notificationID string, conn *websocket.Conn) func() {
	statusHub.mu.Lock()
	if statusHub.clients[notificationID] == nil {
		statusHub.clients[notificationID] = make(map[*websocket.Conn]struct{})
	}
	statusHub.clients[notificationID][conn] = struct{}{}
	statusHub.mu.Unlock()

	return func() {
		statusHub.mu.Lock()
		delete(statusHub.clients[notificationID], conn)
		if len(statusHub.clients[notificationID]) == 0 {
			delete(statusHub.clients, notificationID)
		}
		statusHub.mu.Unlock()
	}
}

func BroadcastStatus(notificationID, status string) {
	msg, _ := json.Marshal(map[string]string{
		"notification_id": notificationID,
		"status":          status,
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
	})

	statusHub.mu.RLock()
	defer statusHub.mu.RUnlock()

	for watchKey, clients := range statusHub.clients {
		if watchKey != "all" && watchKey != notificationID {
			continue
		}
		for conn := range clients {
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				slog.Warn("websocket status write failed", "watch", watchKey, "notification_id", notificationID, "error", err)
			}
		}
	}
}
