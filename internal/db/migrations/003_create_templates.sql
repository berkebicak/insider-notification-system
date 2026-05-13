CREATE TABLE IF NOT EXISTS templates (
	id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	name       TEXT NOT NULL UNIQUE,
	channel    TEXT NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
	content    TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
