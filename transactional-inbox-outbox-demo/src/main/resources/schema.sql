CREATE TABLE IF NOT EXISTS demo_events (
    id UUID PRIMARY KEY,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE,
    retry_count INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    priority INT NOT NULL DEFAULT 0,
    payload VARCHAR(255) NOT NULL
);

ALTER TABLE demo_events
    ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_demo_events_polling
    ON demo_events (status, priority, next_retry_at, created_at);

CREATE TABLE IF NOT EXISTS batch_demo_events (
    id UUID PRIMARY KEY,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE,
    retry_count INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    batch_key VARCHAR(255) NOT NULL,
    payload VARCHAR(255) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_batch_demo_events_polling
    ON batch_demo_events (status, next_retry_at, created_at);

CREATE INDEX IF NOT EXISTS idx_batch_demo_events_batch_key
    ON batch_demo_events (batch_key);
