// Package dlq implements the dead-letter queue: a shadow queue per source
// queue where messages that exhaust their retries are stored with their
// original payload, the last error, the retry count, and a timestamp.
//
// The broker routes dead messages to a `{queue}.dlq` shadow queue via the
// storage layer's Enqueue. This package provides structured record types and
// list/inspect/replay APIs consumed by the HTTP layer in E4 and by
// programmatic callers.
package dlq

import (
	"context"
	"fmt"
	"time"

	"github.com/adexaja/shoebox/internal/storage"
)

// Record is the structured view of a dead-lettered message. It wraps
// storage.Message with convenience fields for inspection and display.
type Record struct {
	// Message is the original message as it was when it was dead-lettered,
	// including payload, metadata, and the accumulated attempt count.
	storage.Message

	// OriginalQueue is the queue the message came from before it was moved
	// to the shadow queue (e.g. "orders" for a message in "orders.dlq").
	OriginalQueue string

	// ErrorMessage is the last error the handler returned before the message
	// was dead-lettered. Mirrors Message.Error; exposed as a top-level field
	// for convenience.
	ErrorMessage string

	// DeadAt is when the message was moved to the dead-letter queue.
	// Mirrors Message.DeadAt.
	DeadAt time.Time
}

// Manager provides list, inspect, and replay operations on dead-letter
// queues. It is a thin wrapper around a storage backend; the DLQ data lives
// in the storage layer as messages with status='dead' (SQLite/Postgres) or as
// messages in the {queue}.dlq pending slice (Memory).
type Manager struct {
	store storage.Storage
}

// NewManager creates a DLQ Manager backed by the given storage.
func NewManager(store storage.Storage) *Manager {
	return &Manager{store: store}
}

// dlqQueue returns the shadow queue name for a source queue.
func dlqQueue(queue string) string {
	return queue + ".dlq"
}

// List returns up to `limit` dead-lettered messages from the named queue's
// shadow queue. The messages are returned newest-first (by dead_at).
func (m *Manager) List(ctx context.Context, queue string, limit int) ([]Record, error) {
	if limit <= 0 {
		limit = 50
	}
	msgs, err := m.store.List(ctx, dlqQueue(queue), limit)
	if err != nil {
		return nil, fmt.Errorf("dlq: list %s: %w", queue, err)
	}
	records := make([]Record, len(msgs))
	for i, msg := range msgs {
		records[i] = toRecord(msg, queue)
	}
	return records, nil
}

// Inspect returns a single dead-lettered message by ID from the named queue's
// shadow queue. Returns storage.ErrEmpty if not found.
func (m *Manager) Inspect(ctx context.Context, queue, id string) (Record, error) {
	// List with a generous limit and find the ID. This is O(n) but DLQs are
	// small by nature (only poison messages land here). A production system
	// with very large DLQs would add a dedicated GetByID to the storage
	// interface.
	msgs, err := m.store.List(ctx, dlqQueue(queue), 10000)
	if err != nil {
		return Record{}, fmt.Errorf("dlq: inspect %s/%s: %w", queue, id, err)
	}
	for _, msg := range msgs {
		if msg.ID == id {
			return toRecord(msg, queue), nil
		}
	}
	return Record{}, fmt.Errorf("dlq: %s/%s: %w", queue, id, storage.ErrEmpty)
}

// Replay moves a dead-lettered message back to the source queue so the
// dispatcher can retry it. The message is re-enqueued with a fresh
// ScheduledAt (immediately due), the attempt count preserved (so the handler's
// MaxRetries check can decide whether to dead-letter it again), and the
// error/dead metadata cleared.
//
// The original DLQ entry is removed on successful re-enqueue. If the
// re-enqueue fails the DLQ entry is retained.
func (m *Manager) Replay(ctx context.Context, queue, id string) error {
	msgs, err := m.store.List(ctx, dlqQueue(queue), 10000)
	if err != nil {
		return fmt.Errorf("dlq: replay %s/%s: %w", queue, id, err)
	}

	var found *storage.Message
	for i := range msgs {
		if msgs[i].ID == id {
			found = &msgs[i]
			break
		}
	}
	if found == nil {
		return fmt.Errorf("dlq: replay %s/%s: %w", queue, id, storage.ErrEmpty)
	}

	// Clear DLQ metadata and re-enqueue to the source queue.
	msg := *found
	msg.Queue = queue
	msg.ScheduledAt = time.Now()
	msg.Error = ""
	msg.DeadAt = time.Time{}

	if err := m.store.Enqueue(ctx, queue, msg); err != nil {
		return fmt.Errorf("dlq: replay enqueue %s/%s: %w", queue, id, err)
	}

	// Remove from DLQ. We Ack the DLQ queue (Ack removes a message by ID).
	if err := m.store.Ack(ctx, dlqQueue(queue), id); err != nil {
		return fmt.Errorf("dlq: replay cleanup %s/%s: %w", queue, id, err)
	}
	return nil
}

// toRecord converts a storage.Message into a DLQ Record.
func toRecord(msg storage.Message, originalQueue string) Record {
	return Record{
		Message:       msg,
		OriginalQueue: originalQueue,
		ErrorMessage:  msg.Error,
		DeadAt:        msg.DeadAt,
	}
}
