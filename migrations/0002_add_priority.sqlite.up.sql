-- 0002_add_priority.sqlite.up.sql
-- Adds the priority column used by the priority feature (E6-S2).
-- Higher values are dequeued first (priority DESC, created_at ASC).
-- DEFAULT 0 preserves existing rows at the lowest priority so their
-- delivery order is unchanged after migration.

ALTER TABLE shoebox_messages ADD COLUMN priority INTEGER DEFAULT 0;
