package storage

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is a persistent storage backend using an external PostgreSQL
// database. It uses pgx/v5 with a connection pool (pgxpool) for concurrent
// access, and SELECT … FOR UPDATE SKIP LOCKED for safe concurrent dequeue
// without double-delivery (E2-S4). Multiple consumer processes can pull from
// the same queue without conflict.
//
// The schema mirrors SQLite (PRD §Data Model) with native JSONB metadata
// and TIMESTAMPTZ. The status lifecycle (pending → processing → deleted)
// provides crash recovery via Reclaim, identical to SQLite.
type Postgres struct {
	pool   *pgxpool.Pool
	schema string
}

//go:embed schema_postgres.sql
var postgresSchema string

// NewPostgres opens a connection pool to the Postgres database at dsn,
// initialises the schema, and reclaims stale 'processing' rows from any
// previous crash (E2-S1). The optional schema defaults to "public". The dsn
// should be a standard Postgres connection string.
func NewPostgres(ctx context.Context, dsn string, schemas ...string) (*Postgres, error) {
	schema := "public"
	if len(schemas) > 0 && schemas[0] != "" {
		schema = schemas[0]
	}
	quotedSchema, err := quotePostgresIdentifier(schema)
	if err != nil {
		return nil, fmt.Errorf("shoebox/postgres: schema: %w", err)
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("shoebox/postgres: parse dsn: %w", err)
	}

	// Sensible pool defaults for shoebox's target scale (hundreds to low
	// thousands of msgs/min). Users with higher throughput can tune via
	// the DSN's pool_max_conns etc.
	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	// A pool can create connections after startup, so configure the schema on
	// every connection rather than changing only the connection used below.
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+quotedSchema); err != nil {
			return err
		}
		_, err := conn.Exec(ctx, "SET search_path TO "+quotedSchema)
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("shoebox/postgres: connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("shoebox/postgres: ping: %w", err)
	}

	if err := initPostgresSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("shoebox/postgres: init schema: %w", err)
	}

	// Reclaim stale in-flight messages from a previous crash (same as SQLite).
	if _, err := pool.Exec(ctx,
		`UPDATE shoebox_messages SET status = 'pending' WHERE status = 'processing'`); err != nil {
		pool.Close()
		return nil, fmt.Errorf("shoebox/postgres: reclaim: %w", err)
	}

	return &Postgres{pool: pool, schema: schema}, nil
}

// quotePostgresIdentifier safely quotes a configured schema name. Schema
// names cannot be passed as query parameters, so quote the identifier here.
func quotePostgresIdentifier(identifier string) (string, error) {
	if identifier == "" || strings.IndexByte(identifier, 0) >= 0 {
		return "", errors.New("must be a non-empty identifier without NUL bytes")
	}
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`, nil
}

// initPostgresSchema serialises schema creation across all shoebox instances
// that share a database. CREATE TABLE IF NOT EXISTS is not sufficient here:
// concurrent CREATE TABLE statements can race while PostgreSQL creates the
// table's implicit composite type, resulting in a duplicate pg_type entry.
//
// The advisory lock is transaction-scoped and the schema is executed on the
// same acquired connection, so the lock remains held for the whole DDL batch.
func initPostgresSchema(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended('shoebox:schema:init', 0))`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, postgresSchema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Enqueue persists a new message with status 'pending'.
func (p *Postgres) Enqueue(ctx context.Context, queue string, msg Message) error {
	meta, err := json.Marshal(msg.Metadata)
	if err != nil {
		return fmt.Errorf("shoebox/postgres: marshal metadata: %w", err)
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	if msg.ScheduledAt.IsZero() {
		msg.ScheduledAt = time.Now()
	}
	payload := msg.Payload
	if payload == nil {
		payload = []byte{}
	}

	var deadAt any
	if !msg.DeadAt.IsZero() {
		deadAt = msg.DeadAt
	}

	status := "pending"
	if strings.HasSuffix(queue, ".dlq") {
		status = "dead"
	}
	_, err = p.pool.Exec(ctx, `INSERT INTO shoebox_messages
		(id, queue, payload, attempts, max_retries, created_at, scheduled_at, priority, metadata, error, dead_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		msg.ID, queue, payload, msg.Attempts, msg.MaxRetries,
		msg.CreatedAt, msg.ScheduledAt, msg.Priority, meta, msg.Error, deadAt,
		status,
	)
	if err != nil {
		return fmt.Errorf("shoebox/postgres: enqueue: %w", err)
	}
	return nil
}

// Dequeue uses SELECT … FOR UPDATE SKIP LOCKED to atomically claim up to
// `limit` due messages. This allows multiple consumer processes to pull from
// the same queue without double-delivery (E2-S4): each consumer locks its
// rows, so no two consumers ever see the same message.
func (p *Postgres) Dequeue(ctx context.Context, queue string, limit int) ([]Message, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("shoebox/postgres: dequeue begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT id, payload, attempts, max_retries, created_at, scheduled_at, metadata, error, dead_at, priority
		 FROM shoebox_messages
		 WHERE queue = $1 AND status = 'pending' AND scheduled_at <= now()
		 ORDER BY priority DESC, created_at DESC
		 LIMIT $2
		 FOR UPDATE SKIP LOCKED`,
		queue, limit)
	if err != nil {
		return nil, fmt.Errorf("shoebox/postgres: dequeue query: %w", err)
	}

	var msgs []Message
	for rows.Next() {
		m, err := scanPostgresMessage(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("shoebox/postgres: dequeue scan: %w", err)
		}
		m.Queue = queue
		msgs = append(msgs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("shoebox/postgres: dequeue rows: %w", err)
	}

	if len(msgs) == 0 {
		return nil, ErrEmpty
	}

	for i := range msgs {
		if _, err := tx.Exec(ctx,
			`UPDATE shoebox_messages SET status = 'processing' WHERE id = $1 AND queue = $2`,
			msgs[i].ID, queue); err != nil {
			return nil, fmt.Errorf("shoebox/postgres: dequeue mark processing: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("shoebox/postgres: dequeue commit: %w", err)
	}
	return msgs, nil
}

// Ack removes a successfully processed message and bumps the processed counter.
func (p *Postgres) Ack(ctx context.Context, queue, msgID string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("shoebox/postgres: ack begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`DELETE FROM shoebox_messages WHERE id = $1 AND queue = $2`, msgID, queue); err != nil {
		return fmt.Errorf("shoebox/postgres: ack delete: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO shoebox_stats (queue, processed, errors, retries, dead)
		 VALUES ($1, 1, 0, 0, 0)
		 ON CONFLICT(queue) DO UPDATE SET processed = shoebox_stats.processed + 1`,
		queue); err != nil {
		return fmt.Errorf("shoebox/postgres: ack stats: %w", err)
	}

	return tx.Commit(ctx)
}

// Nack records a failed delivery. The broker re-enqueues the message with a
// future ScheduledAt; this method removes the current row and bumps counters.
func (p *Postgres) Nack(ctx context.Context, queue, msgID string, nakErr error) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("shoebox/postgres: nack begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`DELETE FROM shoebox_messages WHERE id = $1 AND queue = $2`, msgID, queue); err != nil {
		return fmt.Errorf("shoebox/postgres: nack delete: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO shoebox_stats (queue, processed, errors, retries, dead)
		 VALUES ($1, 0, 1, 1, 0)
		 ON CONFLICT(queue) DO UPDATE SET errors = shoebox_stats.errors + 1, retries = shoebox_stats.retries + 1`,
		queue); err != nil {
		return fmt.Errorf("shoebox/postgres: nack stats: %w", err)
	}

	return tx.Commit(ctx)
}

// Dead marks a message as dead (DLQ) and bumps the dead counter.
func (p *Postgres) Dead(ctx context.Context, queue, msgID string, deadErr error) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("shoebox/postgres: dead begin: %w", err)
	}
	defer tx.Rollback(ctx)

	errStr := ""
	if deadErr != nil {
		errStr = deadErr.Error()
	}
	if _, err := tx.Exec(ctx,
		`UPDATE shoebox_messages SET status = 'dead', error = $1, dead_at = now()
		 WHERE id = $2 AND queue = $3`,
		errStr, msgID, queue); err != nil {
		return fmt.Errorf("shoebox/postgres: dead update: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO shoebox_stats (queue, processed, errors, retries, dead)
		 VALUES ($1, 0, 0, 0, 1)
		 ON CONFLICT(queue) DO UPDATE SET dead = shoebox_stats.dead + 1`,
		queue); err != nil {
		return fmt.Errorf("shoebox/postgres: dead stats: %w", err)
	}

	return tx.Commit(ctx)
}

// List returns up to `limit` dead messages from a queue (DLQ inspection).
func (p *Postgres) List(ctx context.Context, queue string, limit int) ([]Message, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT id, payload, attempts, max_retries, created_at, scheduled_at, metadata, error, dead_at, priority
		 FROM shoebox_messages
		 WHERE queue = $1 AND status = 'dead'
		 ORDER BY dead_at DESC
		 LIMIT $2`,
		queue, limit)
	if err != nil {
		return nil, fmt.Errorf("shoebox/postgres: list query: %w", err)
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		m, err := scanPostgresMessage(rows)
		if err != nil {
			return nil, fmt.Errorf("shoebox/postgres: list scan: %w", err)
		}
		m.Queue = queue
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("shoebox/postgres: list rows: %w", err)
	}
	if len(msgs) == 0 {
		return nil, ErrEmpty
	}
	return msgs, nil
}

// Reclaim transitions stale 'processing' messages back to 'pending' for the
// given queue (crash recovery, E2-S1).
func (p *Postgres) Reclaim(ctx context.Context, queue string) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE shoebox_messages SET status = 'pending' WHERE queue = $1 AND status = 'processing'`,
		queue)
	if err != nil {
		return fmt.Errorf("shoebox/postgres: reclaim: %w", err)
	}
	return nil
}

// Stats returns a snapshot of queue statistics.
func (p *Postgres) Stats(ctx context.Context, queue string) (QueueStats, error) {
	var s QueueStats
	s.Queue = queue

	row := p.pool.QueryRow(ctx,
		`SELECT processed, errors, retries, dead FROM shoebox_stats WHERE queue = $1`,
		queue)
	var processed, errs, retries, dead int64
	if err := row.Scan(&processed, &errs, &retries, &dead); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No stats row yet — depth may still be non-zero.
		} else {
			return QueueStats{}, fmt.Errorf("shoebox/postgres: stats query: %w", err)
		}
	}
	s.Processed = uint64(processed)
	s.Errors = uint64(errs)
	s.Retries = uint64(retries)
	s.Dead = uint64(dead)

	var depth int
	if err := p.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM shoebox_messages WHERE queue = $1 AND status = 'pending'`,
		queue).Scan(&depth); err != nil {
		return QueueStats{}, fmt.Errorf("shoebox/postgres: stats depth: %w", err)
	}
	s.Depth = depth
	return s, nil
}

// Close closes the connection pool.
func (p *Postgres) Close() error {
	if p.pool == nil {
		return nil
	}
	p.pool.Close()
	return nil
}

// scanPostgresMessage scans a pgx.Rows row into a Message. Postgres returns
// native TIMESTAMPTZ (parsed directly into time.Time) and JSONB (parsed as
// raw []byte into the metadata map).
func scanPostgresMessage(rows pgx.Rows) (Message, error) {
	var m Message
	var metaBytes []byte
	var deadAt pgxType

	if err := rows.Scan(
		&m.ID, &m.Payload, &m.Attempts, &m.MaxRetries,
		&m.CreatedAt, &m.ScheduledAt, &metaBytes, &m.Error, &deadAt.val,
		&m.Priority,
	); err != nil {
		return m, err
	}

	if len(metaBytes) > 0 && string(metaBytes) != "{}" {
		if err := json.Unmarshal(metaBytes, &m.Metadata); err != nil {
			return m, fmt.Errorf("parse metadata: %w", err)
		}
	}
	m.DeadAt = deadAt.Time()
	return m, nil
}

// pgxType wraps a nullable TIMESTAMPTZ scanner. Postgres returns NULL for
// unset dead_at; we need to distinguish "no value" from "zero time".
type pgxType struct {
	val  any
	set  bool
	time time.Time
}

func (p *pgxType) Scan(src any) error {
	if src == nil {
		p.set = false
		return nil
	}
	t, ok := src.(time.Time)
	if !ok {
		return fmt.Errorf("shoebox/postgres: cannot scan %T into time.Time", src)
	}
	p.time = t
	p.set = true
	return nil
}

func (p *pgxType) Time() time.Time {
	if !p.set {
		return time.Time{}
	}
	return p.time
}
