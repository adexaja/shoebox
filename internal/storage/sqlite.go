package storage

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver; registers as "sqlite"
)

// SQLite is a persistent storage backend using an embedded SQLite database.
// Messages survive process restarts. It uses a status lifecycle
// (pending → processing → deleted) so that a crash mid-handler leaves
// "processing" rows that Reclaim transitions back to "pending" on the next
// open — this is what makes at-least-once delivery work across restarts
// (E2-S1).
//
// The driver is modernc.org/sqlite (pure Go, no CGo) per ADR 0004.
type SQLite struct {
	db *sql.DB
}

//go:embed schema_sqlite.sql
var schemaSQL string

// NewSQLite opens (or creates) a SQLite database at path and initialises the
// schema. It also reclaims any "processing" rows left by a previous crash
// (transitioning them back to "pending") so that unacked messages are
// redelivered on restart (E2-S1).
func NewSQLite(ctx context.Context, path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("shoebox/sqlite: open %s: %w", path, err)
	}

	// SQLite is single-writer; a small pool is fine. The defaults
	// (unlimited open, 2 idle) work but let's be explicit.
	db.SetMaxOpenConns(1) // SQLite serialises writes anyway
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0) // persistent file; no recycling needed

	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("shoebox/sqlite: init schema: %w", err)
	}

	s := &SQLite{db: db}

	// Reclaim stale in-flight messages from a previous crash. Every
	// "processing" row across all queues goes back to "pending" so the
	// dispatcher picks it up again.
	if _, err := db.ExecContext(ctx,
		`UPDATE shoebox_messages SET status = 'pending' WHERE status = 'processing'`); err != nil {
		db.Close()
		return nil, fmt.Errorf("shoebox/sqlite: reclaim: %w", err)
	}

	return s, nil
}

// Enqueue persists a new message with status 'pending'.
func (s *SQLite) Enqueue(ctx context.Context, queue string, msg Message) error {
	meta, err := json.Marshal(msg.Metadata)
	if err != nil {
		return fmt.Errorf("shoebox/sqlite: marshal metadata: %w", err)
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	if msg.ScheduledAt.IsZero() {
		msg.ScheduledAt = time.Now()
	}
	// Coalesce nil payload to empty []byte so SQLite's NOT NULL constraint
	// is satisfied. A nil []byte would be stored as SQL NULL.
	payload := msg.Payload
	if payload == nil {
		payload = []byte{}
	}
	status := "pending"
	if strings.HasSuffix(queue, ".dlq") {
		status = "dead"
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO shoebox_messages
		(id, queue, payload, attempts, max_retries, created_at, scheduled_at, priority, metadata, error, dead_at, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, queue, payload, msg.Attempts, msg.MaxRetries,
		msg.CreatedAt.Format(time.RFC3339Nano),
		msg.ScheduledAt.Format(time.RFC3339Nano),
		msg.Priority,
		string(meta), msg.Error, tsOrEmpty(msg.DeadAt), status,
	)
	if err != nil {
		return fmt.Errorf("shoebox/sqlite: enqueue: %w", err)
	}
	return nil
}

// Dequeue atomically transitions up to `limit` due messages from 'pending' to
// 'processing' and returns them. Messages whose ScheduledAt is in the future
// are skipped. If no messages are due it returns ErrEmpty.
func (s *SQLite) Dequeue(ctx context.Context, queue string, limit int) ([]Message, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("shoebox/sqlite: dequeue begin: %w", err)
	}
	defer tx.Rollback() // safe to call after Commit

	now := time.Now().Format(time.RFC3339Nano)

	rows, err := tx.QueryContext(ctx,
		`SELECT id, payload, attempts, max_retries, created_at, scheduled_at, metadata, error, dead_at, priority
		 FROM shoebox_messages
		 WHERE queue = ? AND status = 'pending' AND scheduled_at <= ?
		 ORDER BY priority DESC, created_at ASC
		 LIMIT ?`,
		queue, now, limit)
	if err != nil {
		return nil, fmt.Errorf("shoebox/sqlite: dequeue query: %w", err)
	}

	var msgs []Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("shoebox/sqlite: dequeue scan: %w", err)
		}
		m.Queue = queue
		msgs = append(msgs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("shoebox/sqlite: dequeue rows: %w", err)
	}

	if len(msgs) == 0 {
		return nil, ErrEmpty
	}

	// Transition the dequeued messages to 'processing' atomically.
	for i := range msgs {
		if _, err := tx.ExecContext(ctx,
			`UPDATE shoebox_messages SET status = 'processing' WHERE id = ? AND queue = ?`,
			msgs[i].ID, queue); err != nil {
			return nil, fmt.Errorf("shoebox/sqlite: dequeue mark processing: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("shoebox/sqlite: dequeue commit: %w", err)
	}
	return msgs, nil
}

// Ack removes a successfully processed message.
func (s *SQLite) Ack(ctx context.Context, queue, msgID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("shoebox/sqlite: ack begin: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM shoebox_messages WHERE id = ? AND queue = ?`, msgID, queue); err != nil {
		return fmt.Errorf("shoebox/sqlite: ack delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO shoebox_stats (queue, processed, errors, retries, dead)
		 VALUES (?, 1, 0, 0, 0)
		 ON CONFLICT(queue) DO UPDATE SET processed = processed + 1`,
		queue); err != nil {
		return fmt.Errorf("shoebox/sqlite: ack stats: %w", err)
	}

	return tx.Commit()
}

// Nack records a failed delivery. The broker re-enqueues the message with a
// future ScheduledAt; this method just bumps the retry counter. The message
// is transitioned back to 'pending' by the subsequent Enqueue (INSERT OR
// REPLACE) or left in 'processing' if the broker hasn't re-enqueued yet.
func (s *SQLite) Nack(ctx context.Context, queue, msgID string, nakErr error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("shoebox/sqlite: nack begin: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM shoebox_messages WHERE id = ? AND queue = ?`, msgID, queue); err != nil {
		return fmt.Errorf("shoebox/sqlite: nack delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO shoebox_stats (queue, processed, errors, retries, dead)
		 VALUES (?, 0, 1, 1, 0)
		 ON CONFLICT(queue) DO UPDATE SET errors = errors + 1, retries = retries + 1`,
		queue); err != nil {
		return fmt.Errorf("shoebox/sqlite: nack stats: %w", err)
	}

	return tx.Commit()
}

// Dead marks a message as dead (DLQ). The message row is transitioned to
// status='dead' with the last error and a timestamp, and the dead counter is
// bumped. The broker has already set msg.Error and msg.DeadAt before calling.
func (s *SQLite) Dead(ctx context.Context, queue, msgID string, deadErr error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("shoebox/sqlite: dead begin: %w", err)
	}
	defer tx.Rollback()

	errStr := ""
	if deadErr != nil {
		errStr = deadErr.Error()
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE shoebox_messages SET status = 'dead', error = ?, dead_at = ?
		 WHERE id = ? AND queue = ?`,
		errStr, time.Now().Format(time.RFC3339Nano), msgID, queue); err != nil {
		return fmt.Errorf("shoebox/sqlite: dead update: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO shoebox_stats (queue, processed, errors, retries, dead)
		 VALUES (?, 0, 0, 0, 1)
		 ON CONFLICT(queue) DO UPDATE SET dead = dead + 1`,
		queue); err != nil {
		return fmt.Errorf("shoebox/sqlite: dead stats: %w", err)
	}

	return tx.Commit()
}

// List returns up to `limit` dead messages from a queue (DLQ inspection).
// The queue name should be the shadow queue name (e.g. "orders.dlq").
func (s *SQLite) List(ctx context.Context, queue string, limit int) ([]Message, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, payload, attempts, max_retries, created_at, scheduled_at, metadata, error, dead_at, priority
		 FROM shoebox_messages
		 WHERE queue = ? AND status = 'dead'
		 ORDER BY dead_at DESC
		 LIMIT ?`,
		queue, limit)
	if err != nil {
		return nil, fmt.Errorf("shoebox/sqlite: list query: %w", err)
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, fmt.Errorf("shoebox/sqlite: list scan: %w", err)
		}
		m.Queue = queue
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("shoebox/sqlite: list rows: %w", err)
	}
	if len(msgs) == 0 {
		return nil, ErrEmpty
	}
	return msgs, nil
}

// Reclaim transitions stale in-flight ('processing') messages back to
// 'pending' for the given queue. Called on open for crash recovery (E2-S1).
// For per-queue reclaim; the global reclaim (all queues) is done in NewSQLite.
func (s *SQLite) Reclaim(ctx context.Context, queue string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE shoebox_messages SET status = 'pending' WHERE queue = ? AND status = 'processing'`,
		queue)
	if err != nil {
		return fmt.Errorf("shoebox/sqlite: reclaim: %w", err)
	}
	return nil
}

// Stats returns a snapshot of queue statistics.
func (s *SQLite) Stats(ctx context.Context, queue string) (QueueStats, error) {
	// Read counters from the stats table.
	var s2 QueueStats
	s2.Queue = queue
	row := s.db.QueryRowContext(ctx,
		`SELECT processed, errors, retries, dead FROM shoebox_stats WHERE queue = ?`,
		queue)
	var processed, errs, retries, dead sql.NullInt64
	if err := row.Scan(&processed, &errs, &retries, &dead); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No stats row yet — depth may still be non-zero.
		} else {
			return QueueStats{}, fmt.Errorf("shoebox/sqlite: stats query: %w", err)
		}
	}
	s2.Processed = uint64(processed.Int64)
	s2.Errors = uint64(errs.Int64)
	s2.Retries = uint64(retries.Int64)
	s2.Dead = uint64(dead.Int64)

	// Depth: count of pending messages.
	var depth int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM shoebox_messages WHERE queue = ? AND status = 'pending'`,
		queue).Scan(&depth); err != nil {
		return QueueStats{}, fmt.Errorf("shoebox/sqlite: stats depth: %w", err)
	}
	s2.Depth = depth
	return s2, nil
}

// Close closes the underlying database connection.
func (s *SQLite) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// scanMessage scans a row into a Message. The caller must set m.Queue since
// we don't select it (it's the WHERE clause, redundant in the projection).
func scanMessage(rs interface {
	Scan(dest ...any) error
}) (Message, error) {
	var m Message
	var metaStr string
	var createdAt, scheduledAt, deadAtStr, errStr string

	if err := rs.Scan(
		&m.ID, &m.Payload, &m.Attempts, &m.MaxRetries,
		&createdAt, &scheduledAt, &metaStr, &errStr, &deadAtStr,
		&m.Priority,
	); err != nil {
		return m, err
	}

	var err error
	m.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return m, fmt.Errorf("parse created_at %q: %w", createdAt, err)
	}
	m.ScheduledAt, err = time.Parse(time.RFC3339Nano, scheduledAt)
	if err != nil {
		return m, fmt.Errorf("parse scheduled_at %q: %w", scheduledAt, err)
	}
	if metaStr != "" && metaStr != "{}" {
		if err := json.Unmarshal([]byte(metaStr), &m.Metadata); err != nil {
			return m, fmt.Errorf("parse metadata %q: %w", metaStr, err)
		}
	}
	m.Error = errStr
	if deadAtStr != "" {
		m.DeadAt, err = time.Parse(time.RFC3339Nano, deadAtStr)
		if err != nil {
			return m, fmt.Errorf("parse dead_at %q: %w", deadAtStr, err)
		}
	}
	return m, nil
}

// tsOrEmpty formats a time as RFC 3339, or returns "" for the zero time
// (stored as empty TEXT in SQLite).
func tsOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
