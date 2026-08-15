CREATE TABLE IF NOT EXISTS jdbc_demo_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE,
    retry_count INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    priority INT NOT NULL DEFAULT 0,
    payload VARCHAR(255) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jdbc_demo_events_polling
    ON jdbc_demo_events (status, priority, next_retry_at, created_at);
