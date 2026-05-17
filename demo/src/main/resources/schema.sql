CREATE TABLE IF NOT EXISTS my_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Aggregate information
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,

    -- Payload and versioning
    payload TEXT NOT NULL,
    schema_version VARCHAR(50) DEFAULT '1.0',

    -- Lifecycle status
    -- Используем VARCHAR для статуса, так как ENUM в R2DBC/Postgres требует дополнительной настройки типа.
    -- Если хотите строгую типизацию, можно создать TYPE event_status AS ENUM ('PENDING', 'PROCESSED', 'FAILED') и использовать его.
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',

    retry_count INT NOT NULL DEFAULT 0,

    -- Timestamps
    -- ZonedDateTime маппится на TIMESTAMPTZ (timestamp with time zone)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ
);

-- Индексы для оптимизации выборок
-- 1. Часто ищем события по статусу для обработки
CREATE INDEX IF NOT EXISTS idx_my_events_status ON my_events (status);

-- 2. Часто ищем события по агрегату (для идемпотентности или истории)
CREATE INDEX IF NOT EXISTS idx_my_events_aggregate ON my_events (aggregate_type, aggregate_id);

-- 3. Для сортировки при выборке "старых".pending событий
CREATE INDEX IF NOT EXISTS idx_my_events_created_at ON my_events (created_at);