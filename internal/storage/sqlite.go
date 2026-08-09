// Package storage — SQLite backend. Placeholder for Week 2.
//
// See docs/tasks.md (E2) and the PRD §Data Model for the target schema:
//
//	CREATE TABLE shoebox_messages (
//	    id          TEXT PRIMARY KEY,
//	    queue       TEXT NOT NULL,
//	    payload     BLOB NOT NULL,
//	    attempts    INTEGER DEFAULT 0,
//	    max_retries INTEGER DEFAULT 5,
//	    created_at  TIMESTAMPTZ DEFAULT now(),
//	    scheduled_at TIMESTAMPTZ DEFAULT now(),
//	    visible_at  TIMESTAMPTZ DEFAULT now(),
//	    metadata    JSONB DEFAULT '{}',
//	    status      TEXT DEFAULT 'pending'
//	);
//
// The implementation will use modernc.org/sqlite (pure Go, no CGo) per
// ADR 0004.
package storage

// TODO(E2): implement SQLiteStorage using modernc.org/sqlite.
