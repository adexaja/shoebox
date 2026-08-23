-- 0003_periodic_jobs.postgres.up.sql
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
