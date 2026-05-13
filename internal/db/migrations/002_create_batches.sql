CREATE TABLE IF NOT EXISTS batches (
	id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	total      INT NOT NULL DEFAULT 0 CHECK (total >= 0),
	pending    INT NOT NULL DEFAULT 0 CHECK (pending >= 0),
	delivered  INT NOT NULL DEFAULT 0 CHECK (delivered >= 0),
	failed     INT NOT NULL DEFAULT 0 CHECK (failed >= 0),
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
