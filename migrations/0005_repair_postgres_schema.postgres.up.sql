-- 0005_repair_postgres_schema.postgres.up.sql
-- Repairs databases that recorded migration 0004 before its schema changes
-- were applied. Every statement is idempotent so healthy databases are
-- unchanged.

CREATE TABLE IF NOT EXISTS shoebox_schedules (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    payload BYTEA NOT NULL,
    enqueue_options JSONB NOT NULL DEFAULT '{}',
    interval_ns BIGINT NOT NULL,
    next_run_at TIMESTAMPTZ NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_shoebox_schedules_due
    ON shoebox_schedules(enabled, next_run_at);

ALTER TABLE shoebox_messages
    ADD COLUMN IF NOT EXISTS dedupe_key TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_shoebox_durable_dedupe
    ON shoebox_messages(queue, dedupe_key)
    WHERE dedupe_key <> '' AND status IN ('pending', 'processing');
