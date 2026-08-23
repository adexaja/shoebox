-- 0004_durable_dedupe.postgres.up.sql
ALTER TABLE shoebox_messages ADD COLUMN IF NOT EXISTS dedupe_key TEXT NOT NULL DEFAULT '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_shoebox_durable_dedupe
    ON shoebox_messages(queue, dedupe_key)
    WHERE dedupe_key <> '' AND status IN ('pending', 'processing');
