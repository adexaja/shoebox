// Package storage defines the Storage interface that abstracts over the
// queue's persistence layer. The broker only ever talks to Storage; the
// concrete backend (Memory, SQLite, Postgres) is selected at New() time.
//
// See docs/adr/0001-storage-interface-is-core-abstraction.md for the
// rationale.
package storage

import (
	"context"
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
}

// QueueStats is the storage-layer view of queue statistics.
type QueueStats struct {
	Queue   string
	Depth   int
	Processed uint64
	Errors  uint64
	Retries uint64
	Dead    uint64
}

// ErrEmpty is returned by Dequeue when no messages are available.
var ErrEmpty = errors.New("shoebox/storage: queue empty")

// Storage is the interface every backend implements. It is deliberately
// small: the broker is the only caller.
//
// Enqueue persists a new message. Dequeue returns up to `limit` messages
// that are due (ScheduledAt <= now), removing them from the pending set.
// Ack confirms successful processing. Nack records a failed delivery
// (the broker re-enqueues with a future ScheduledAt for backoff).
// Dead records that a message has exhausted its retries; the broker has
// already moved the message to the {queue}.dlq shadow queue, this is
// just the stat bump.
type Storage interface {
	Enqueue(ctx context.Context, queue string, m Message) error
	Dequeue(ctx context.Context, queue string, limit int) ([]Message, error)
	Ack(ctx context.Context, queue string, msgID string) error
	Nack(ctx context.Context, queue string, msgID string, err error) error
	Dead(ctx context.Context, queue string, msgID string, err error) error
	Stats(ctx context.Context, queue string) (QueueStats, error)
	Close() error
}
