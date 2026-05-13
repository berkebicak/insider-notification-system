CREATE INDEX IF NOT EXISTS idx_notifications_status
	ON notifications(status);

CREATE INDEX IF NOT EXISTS idx_notifications_channel
	ON notifications(channel);

CREATE INDEX IF NOT EXISTS idx_notifications_batch_id
	ON notifications(batch_id);

CREATE INDEX IF NOT EXISTS idx_notifications_created
	ON notifications(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_notifications_scheduled
	ON notifications(scheduled_at)
	WHERE scheduled_at IS NOT NULL;
