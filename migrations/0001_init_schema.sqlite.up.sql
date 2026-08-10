-- 0001_init_schema.sqlite.up.sql
-- Initial schema for shoebox SQLite backend.
-- See PRD §Data Model and ADR 0005 for column semantics.
--
-- SQLite-specific adaptations (vs Postgres):
--   payload    → BLOB (vs BYTEA)
--   metadata   → TEXT with JSON encoding (vs native JSONB)
--   timestamps → TEXT in RFC 3339 Nano (vs native TIMESTAMPTZ)
--   dead_at    → TEXT (vs TIMESTAMPTZ; empty string = NULL)

CREATE TABLE IF NOT EXISTS shoebox_messages (
    id           TEXT PRIMARY KEY,
    queue        TEXT NOT NULL,
    payload      BLOB NOT NULL,
    attempts     INTEGER DEFAULT 0,
    max_retries  INTEGER DEFAULT 5,
    created_at   TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    metadata     TEXT DEFAULT '{}',
    error        TEXT DEFAULT '',
    dead_at      TEXT DEFAULT '',
    status       TEXT DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS idx_shoebox_dequeue
    ON shoebox_messages(queue, status, scheduled_at);

CREATE INDEX IF NOT EXISTS idx_shoebox_dlq
    ON shoebox_messages(queue, status);

CREATE TABLE IF NOT EXISTS shoebox_stats (
    queue     TEXT PRIMARY KEY,
    processed INTEGER DEFAULT 0,
    errors    INTEGER DEFAULT 0,
    retries   INTEGER DEFAULT 0,
    dead      INTEGER DEFAULT 0
);
