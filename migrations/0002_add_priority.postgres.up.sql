-- 0002_add_priority.postgres.up.sql
-- Adds the priority column used by the priority feature (E6-S2).
-- Higher values are dequeued first (priority DESC, created_at ASC).
-- DEFAULT 0 preserves existing rows at the lowest priority.

ALTER TABLE shoebox_messages ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;

-- The dequeue index previously covered (queue, status, scheduled_at).
-- Add priority ahead of scheduled_at so the index can serve the new
-- ORDER BY priority DESC, scheduled_at, created_at without a sort.
DROP INDEX IF EXISTS idx_shoebox_dequeue;
CREATE INDEX idx_shoebox_dequeue
    ON shoebox_messages(queue, status, priority DESC, scheduled_at);
