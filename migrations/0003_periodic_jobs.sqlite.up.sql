-- 0003_periodic_jobs.sqlite.up.sql
CREATE TABLE IF NOT EXISTS shoebox_schedules (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    payload BLOB NOT NULL,
    enqueue_options TEXT NOT NULL DEFAULT '',
    interval_ns INTEGER NOT NULL,
    next_run_at TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shoebox_schedules_due
    ON shoebox_schedules(enabled, next_run_at);
