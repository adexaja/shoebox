-- schema_postgres.sql
-- Embedded schema for the Postgres backend, applied on first open via
-- NewPostgres. This is a copy of migrations/0001_init_schema.postgres.up.sql
-- kept here for //go:embed. Edit the migration file as the canonical source
-- and mirror changes here (or re-run the migration tool if using one
-- externally).

CREATE TABLE IF NOT EXISTS shoebox_messages (
    id           TEXT PRIMARY KEY,
    queue        TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_retries  INTEGER NOT NULL DEFAULT 5,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    priority     INTEGER NOT NULL DEFAULT 0,
    metadata     JSONB NOT NULL DEFAULT '{}',
    error        TEXT NOT NULL DEFAULT '',
    dead_at      TIMESTAMPTZ,
    status       TEXT NOT NULL DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS idx_shoebox_dequeue
    ON shoebox_messages(queue, status, priority DESC, scheduled_at);

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
