// Package storage defines the Storage interface that abstracts over the
// queue's persistence layer. The broker only ever talks to Storage; the
// concrete backend (Memory, SQLite, Postgres) is selected at New() time.
package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

// Message is the storage-layer view of a queued message. It mirrors the
// public shoebox.Message so callers don't need to translate between the
// two; the broker is the only package that does so.
type Message struct {
	ID          string
	Queue       string
	Payload     []byte
	Attempts    int
	MaxRetries  int
	CreatedAt   time.Time
	ScheduledAt time.Time
	Metadata    map[string]string

	// Priority is the delivery hint. Higher values are dequeued first
	// (priority DESC) within the same due window; ties break by
	// created_at ASC. Zero (the default) is the lowest priority.
	Priority int

	// Error is set when the message is moved to the dead-letter queue; it
	// holds the last handler error. Empty for live messages.
	Error string

	// DeadAt is set when the message is moved to the dead-letter queue; it
	// records the time of dead-lettering. Zero for live messages.
	DeadAt time.Time
}

// Schedule is a persistent periodic enqueue definition.
type Schedule struct {
	ID             string
	Queue          string
	Payload        []byte
	EnqueueOptions []byte
	Interval       time.Duration
	NextRunAt      time.Time
	Enabled        bool
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ScheduleStore persists periodic enqueue definitions.
type ScheduleStore interface {
	CreateSchedule(ctx context.Context, schedule Schedule) error
	UpdateSchedule(ctx context.Context, schedule Schedule) error
	DeleteSchedule(ctx context.Context, id string) error
	ListSchedules(ctx context.Context, queue string) ([]Schedule, error)
	ClaimSchedule(ctx context.Context, id string, now, next time.Time) (bool, error)
	DueSchedules(ctx context.Context, now time.Time, limit int) ([]Schedule, error)
}

var ErrScheduleExists = errors.New("shoebox/storage: schedule already exists")

// QueueStats is the storage-layer view of queue statistics.
type QueueStats struct {
	Queue     string
	Depth     int
	Processed uint64
	Errors    uint64
	Retries   uint64
	Dead      uint64
}

// ErrEmpty is returned by Dequeue when no messages are available.
var ErrEmpty = errors.New("shoebox/storage: queue empty")

// Storage is the interface every backend implements. It is deliberately
// small: the broker is the only caller.
//
// Enqueue persists a new message. Dequeue returns up to `limit` messages
// that are due (ScheduledAt <= now), atomically transitioning them to an
// in-flight state (SQLite/Postgres: status='processing'; Memory: removed
// from the pending slice). Ack confirms successful processing and removes
// the message. Nack records a failed delivery (the broker re-enqueues with
// a future ScheduledAt for backoff). Dead marks the message as dead and
// records the last error. List returns dead messages from a queue (DLQ
// inspection). Reclaim transitions stale in-flight messages back to pending
// (crash recovery; called once during open of a persistent backend).
type Storage interface {
	Enqueue(ctx context.Context, queue string, m Message) error
	Dequeue(ctx context.Context, queue string, limit int) ([]Message, error)
	Ack(ctx context.Context, queue string, msgID string) error
	Nack(ctx context.Context, queue string, msgID string, err error) error
	Dead(ctx context.Context, queue string, msgID string, err error) error
	Stats(ctx context.Context, queue string) (QueueStats, error)
	List(ctx context.Context, queue string, limit int) ([]Message, error)
	Reclaim(ctx context.Context, queue string) error
	Close() error
}

// NewMessageID returns a 16-byte random hex string suitable for use as a
// message ID. We avoid pulling in a UUID library to keep the dependency
// surface small (see ADR 0004). If crypto/rand fails (unrecoverable on a
// healthy system) it falls back to a time-based identifier so we never
// panic in a user's hot path.
func NewMessageID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		now := time.Now().UnixNano()
		return "ts-" + hex.EncodeToString([]byte{
			byte(now >> 56), byte(now >> 48), byte(now >> 40), byte(now >> 32),
			byte(now >> 24), byte(now >> 16), byte(now >> 8), byte(now),
		})
	}
	return hex.EncodeToString(b[:])
}
