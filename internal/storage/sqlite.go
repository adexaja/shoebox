package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver; registers as "sqlite"

	"github.com/adexaja/shoebox/migrations"
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

// initSQLiteSchema applies pending schema migrations from the embedded
// migrations/ directory (the canonical DDL source), creating the schema on
// fresh databases and upgrading older ones in place. PRAGMA user_version
// tracks the applied version; it is transactional in SQLite, so each
// migration commits atomically with its version bump.
//
// The pool holds a single connection, so a process cannot race itself, and
// SQLite's file locking serialises concurrent openers; a concurrent opener
// sees the recorded version and applies nothing.
//
// Databases created before the migration runner existed carry user_version 0
// with existing tables; their baseline is detected once from the live schema
// and recorded (see detectSQLiteBaseline).
func initSQLiteSchema(ctx context.Context, db *sql.DB) error {
	ups, err := migrations.Up("sqlite")
	if err != nil {
		return err
	}
	latest := ups[len(ups)-1].Version

	version, err := sqliteUserVersion(ctx, db)
	if err != nil {
		return err
	}
	if version == 0 {
		baseline, err := detectSQLiteBaseline(ctx, db, latest)
		if err != nil {
			return err
		}
		if baseline > 0 {
			if err := setSQLiteUserVersion(ctx, db, baseline); err != nil {
				return err
			}
			version = baseline
		}
	}

	for _, m := range ups {
		if m.Version <= version {
			continue
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %s: %w", m.Name, err)
		}
		if _, err := tx.ExecContext(ctx,
			"PRAGMA user_version = "+strconv.Itoa(m.Version)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("set user_version %d: %w", m.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// sqliteUserVersion reads PRAGMA user_version, the applied-migration marker.
func sqliteUserVersion(ctx context.Context, db *sql.DB) (int, error) {
	var v int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&v); err != nil {
		return 0, fmt.Errorf("read user_version: %w", err)
	}
	return v, nil
}

// setSQLiteUserVersion records the applied-migration marker.
func setSQLiteUserVersion(ctx context.Context, db *sql.DB, version int) error {
	if _, err := db.ExecContext(ctx,
		"PRAGMA user_version = "+strconv.Itoa(version)); err != nil {
		return fmt.Errorf("set user_version %d: %w", version, err)
	}
	return nil
}

// detectSQLiteBaseline determines the version of a pre-runner database from
// its live schema: no shoebox tables → 0 (fresh), a shoebox_messages table
// without a priority column → 1, with one → latest. Mirrors
// appliedPostgresVersion for the Postgres backend.
func detectSQLiteBaseline(ctx context.Context, db *sql.DB, latest int) (int, error) {
	var tables int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'shoebox_messages'`).Scan(&tables); err != nil {
		return 0, err
	}
	if tables == 0 {
		return 0, nil
	}

	rows, err := db.QueryContext(ctx, `PRAGMA table_info(shoebox_messages)`)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull, pk int
		var dfltValue any // column default; NULL when absent
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dfltValue, &pk); err != nil {
			return 0, err
		}
		if name == "priority" {
			return latest, nil
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return 1, nil
}

// NewSQLite opens (or creates) a SQLite database at path, applies pending
// schema migrations, and reclaims any "processing" rows left by a previous
// crash (transitioning them back to "pending") so that unacked messages are
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

	if err := initSQLiteSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("shoebox/sqlite: init schema: %w", err)
	}

	s := &SQLite{db: db}

	// Reclaim stale in-flight messages from a previous crash. Every
	// "processing" row across all queues goes back to "pending" so the
	// dispatcher picks it up again.
	if _, err := db.ExecContext(ctx,
		`UPDATE shoebox_messages SET status = 'pending' WHERE status = 'processing'`); err != nil {
		_ = db.Close()
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
		(id, queue, payload, attempts, max_retries, created_at, scheduled_at, priority, dedupe_key, metadata, error, dead_at, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, queue, payload, msg.Attempts, msg.MaxRetries,
		msg.CreatedAt.Format(time.RFC3339Nano),
		msg.ScheduledAt.Format(time.RFC3339Nano),
		msg.Priority, msg.DedupeKey,
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
	defer func() { _ = tx.Rollback() }() // safe to call after Commit

	now := time.Now().Format(time.RFC3339Nano)

	rows, err := tx.QueryContext(ctx,
		`SELECT id, payload, attempts, max_retries, created_at, scheduled_at, dedupe_key, metadata, error, dead_at, priority
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
			_ = rows.Close()
			return nil, fmt.Errorf("shoebox/sqlite: dequeue scan: %w", err)
		}
		m.Queue = queue
		msgs = append(msgs, m)
	}
	_ = rows.Close()
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
	defer func() { _ = tx.Rollback() }()

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
	defer func() { _ = tx.Rollback() }()

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
	defer func() { _ = tx.Rollback() }()

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
		`SELECT id, payload, attempts, max_retries, created_at, scheduled_at, dedupe_key, metadata, error, dead_at, priority
		 FROM shoebox_messages
		 WHERE queue = ? AND status = 'dead'
		 ORDER BY dead_at DESC
		 LIMIT ?`,
		queue, limit)
	if err != nil {
		return nil, fmt.Errorf("shoebox/sqlite: list query: %w", err)
	}
	defer func() { _ = rows.Close() }()

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
		&createdAt, &scheduledAt, &m.DedupeKey, &metaStr, &errStr, &deadAtStr,
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

func sqliteScheduleTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func scanSQLiteSchedule(row interface{ Scan(...any) error }) (Schedule, error) {
	var s Schedule
	var payload []byte
	var options string
	var interval int64
	var next, created, updated string
	var enabled int
	if err := row.Scan(&s.ID, &s.Queue, &payload, &options, &interval, &next, &enabled, &created, &updated); err != nil {
		return Schedule{}, err
	}
	s.Payload = append([]byte(nil), payload...)
	s.EnqueueOptions = []byte(options)
	s.Interval = time.Duration(interval)
	s.NextRunAt, _ = time.Parse(time.RFC3339Nano, next)
	s.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	s.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	s.Enabled = enabled != 0
	return s, nil
}

func (s *SQLite) CreateSchedule(ctx context.Context, v Schedule) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO shoebox_schedules
		(id, queue, payload, enqueue_options, interval_ns, next_run_at, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		v.ID, v.Queue, v.Payload, string(v.EnqueueOptions), int64(v.Interval),
		sqliteScheduleTime(v.NextRunAt), v.Enabled, sqliteScheduleTime(v.CreatedAt), sqliteScheduleTime(v.UpdatedAt))
	return err
}

func (s *SQLite) UpdateSchedule(ctx context.Context, v Schedule) error {
	_, err := s.db.ExecContext(ctx, `UPDATE shoebox_schedules SET queue=?, payload=?,
		enqueue_options=?, interval_ns=?, next_run_at=?, enabled=?, updated_at=? WHERE id=?`,
		v.Queue, v.Payload, string(v.EnqueueOptions), int64(v.Interval), sqliteScheduleTime(v.NextRunAt),
		v.Enabled, sqliteScheduleTime(v.UpdatedAt), v.ID)
	return err
}

func (s *SQLite) DeleteSchedule(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM shoebox_schedules WHERE id=?`, id)
	return err
}

func (s *SQLite) ListSchedules(ctx context.Context, queue string) ([]Schedule, error) {
	query := `SELECT id, queue, payload, enqueue_options, interval_ns, next_run_at, enabled, created_at, updated_at
		FROM shoebox_schedules`
	args := []any{}
	if queue != "" {
		query += ` WHERE queue=?`
		args = append(args, queue)
	}
	query += ` ORDER BY id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Schedule
	for rows.Next() {
		v, err := scanSQLiteSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *SQLite) DueSchedules(ctx context.Context, now time.Time, limit int) ([]Schedule, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, queue, payload, enqueue_options, interval_ns,
		next_run_at, enabled, created_at, updated_at FROM shoebox_schedules
		WHERE enabled=1 AND next_run_at <= ? ORDER BY next_run_at LIMIT ?`,
		sqliteScheduleTime(now), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Schedule
	for rows.Next() {
		v, err := scanSQLiteSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *SQLite) ClaimSchedule(ctx context.Context, id string, now, next time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE shoebox_schedules SET next_run_at=?, updated_at=?
		WHERE id=? AND enabled=1 AND next_run_at <= ?`,
		sqliteScheduleTime(next), sqliteScheduleTime(time.Now()), id, sqliteScheduleTime(now))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
