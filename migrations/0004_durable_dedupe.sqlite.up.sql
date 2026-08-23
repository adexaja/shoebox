-- 0004_durable_dedupe.sqlite.up.sql
ALTER TABLE shoebox_messages ADD COLUMN dedupe_key TEXT NOT NULL DEFAULT '';
