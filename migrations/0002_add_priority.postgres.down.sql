-- 0002_add_priority.postgres.down.sql
-- Reverses 0002_add_priority.

ALTER TABLE shoebox_messages DROP COLUMN priority;

DROP INDEX IF EXISTS idx_shoebox_dequeue;
CREATE INDEX idx_shoebox_dequeue
    ON shoebox_messages(queue, status, scheduled_at);
