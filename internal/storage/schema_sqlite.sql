-- schema_sqlite.sql
-- Embedded schema for the SQLite backend, applied on first open via NewSQLite.
-- This is a copy of migrations/0001_init_schema.sqlite.up.sql kept here for
-- //go:embed. Edit the migration file as the canonical source and mirror
-- changes here (or re-run the migration tool if using one externally).

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
