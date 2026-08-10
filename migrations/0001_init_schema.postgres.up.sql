-- 0001_init_schema.postgres.up.sql
-- Initial schema for shoebox Postgres backend.
-- See PRD §Data Model and ADR 0005 for column semantics.
--
-- Postgres-native types:
--   payload    → BYTEA
--   metadata   → JSONB (queryable: WHERE metadata->>'trace_id' = ...)
--   timestamps → TIMESTAMPTZ (native, no string parsing)
--   dead_at    → TIMESTAMPTZ (nullable; NULL = not dead-lettered)

CREATE TABLE IF NOT EXISTS shoebox_messages (
    id           TEXT PRIMARY KEY,
    queue        TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_retries  INTEGER NOT NULL DEFAULT 5,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata     JSONB NOT NULL DEFAULT '{}',
    error        TEXT NOT NULL DEFAULT '',
    dead_at      TIMESTAMPTZ,
    status       TEXT NOT NULL DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS idx_shoebox_dequeue
    ON shoebox_messages(queue, status, scheduled_at);

-- Partial index: only dead rows, so it stays small even on high-volume queues.
CREATE INDEX IF NOT EXISTS idx_shoebox_dlq
    ON shoebox_messages(queue, status) WHERE status = 'dead';

CREATE TABLE IF NOT EXISTS shoebox_stats (
    queue     TEXT PRIMARY KEY,
    processed BIGINT NOT NULL DEFAULT 0,
    errors    BIGINT NOT NULL DEFAULT 0,
    retries   BIGINT NOT NULL DEFAULT 0,
    dead      BIGINT NOT NULL DEFAULT 0
);
