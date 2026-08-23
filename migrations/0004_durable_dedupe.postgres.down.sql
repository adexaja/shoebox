DROP INDEX IF EXISTS idx_shoebox_durable_dedupe;
ALTER TABLE shoebox_messages DROP COLUMN IF EXISTS dedupe_key;
