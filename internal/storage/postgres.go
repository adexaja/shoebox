// Package storage — Postgres backend.
//
// Will use pgx and SELECT ... FOR UPDATE SKIP LOCKED for concurrent
// consumers (see PRD §Key Design Decisions #1, ADR 0004). The schema mirrors
// SQLite (PRD §Data Model); the only differences are:
//   - metadata uses native JSONB (not TEXT-encoded JSON)
//   - timestamps use native TIMESTAMPTZ (not RFC 3339 strings)
//   - Dequeue uses FOR UPDATE SKIP LOCKED instead of a single-writer mutex
//
// Not yet implemented. The interface contract and lifecycle (pending →
// processing → deleted, with Reclaim on open) are identical to SQLite.
package storage

// TODO(E2): implement Postgres using pgx with SKIP LOCKED dequeue.
// Design sketch:
//
//	func NewPostgres(ctx context.Context, dsn string) (*Postgres, error)
//
//	func (p *Postgres) Dequeue(ctx, queue, limit) {
//	    BEGIN
//	    SELECT id, ... FROM shoebox_messages
//	     WHERE queue = $1 AND status = 'pending' AND scheduled_at <= now()
//	     ORDER BY created_at
//	     LIMIT $2
//	     FOR UPDATE SKIP LOCKED
//	    UPDATE shoebox_messages SET status = 'processing' WHERE id IN (...)
//	    COMMIT
//	}
