-- 0002_add_priority.sqlite.down.sql
-- Reverses 0002_add_priority. SQLite (>= 3.35) supports DROP COLUMN.

ALTER TABLE shoebox_messages DROP COLUMN priority;
